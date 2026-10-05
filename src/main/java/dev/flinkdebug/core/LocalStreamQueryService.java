package dev.flinkdebug.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/** Standard-typed, default-namespace keyed state reads through Flink's DataStream BATCH API. */
public final class LocalStreamQueryService {
    public enum Kind { VALUE, LIST, MAP }
    public enum ScalarType { STRING, INT, LONG, DOUBLE, BOOLEAN }
    public record QuerySpec(String operatorId, String stateName, Kind kind, ScalarType keyType,
                            ScalarType valueType, ScalarType mapKeyType, int limit) {
        public QuerySpec {
            if (operatorId == null || !operatorId.matches("[0-9a-fA-F]{32}")) throw new IllegalArgumentException("请选择有效的 operator ID hash。");
            if (stateName == null || stateName.isBlank()) throw new IllegalArgumentException("请填写状态名。");
            if (kind == null || keyType == null || valueType == null || mapKeyType == null) throw new IllegalArgumentException("请选择状态类型。");
            if (limit < 1 || limit > 10000) throw new IllegalArgumentException("样本上限应为 1–10000。");
        }
    }
    public record QueryRow(String key, String value) {}
    public record QueryReport(List<QueryRow> rows, boolean truncated, List<String> warnings) {}

    public QueryReport query(SnapshotSession session, QuerySpec spec) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(session.classLoader());
        try {
            validateSchema(session, spec);
            try { return run(session, spec); }
            catch (Exception failure) {
                if (Thread.currentThread().isInterrupted()) throw failure;
                var details = new StringBuilder();
                Throwable cause = failure;
                for (int depth = 0; cause != null && depth < 12; depth++, cause = cause.getCause()) {
                    if (cause.getMessage() != null) details.append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage()).append("\n");
                }
                String message = details.length() <= 6000 ? details.toString() : details.substring(0, 6000) + "…";
                throw new java.io.IOException("状态查询失败: " + spec.stateName() + " (" + spec.kind()
                        + ", key=" + spec.keyType() + ", value=" + spec.valueType() + ", mapKey=" + spec.mapKeyType()
                        + ")。请检查类型、序列化器和文件完整性。\n" + message, failure);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private QueryReport run(SnapshotSession session, QuerySpec spec) throws Exception {
        try (var copy = LocalSnapshotCopy.create(session, spec.operatorId())) {
            Configuration config = new Configuration();
            config.setString("jobmanager.bind-host", "127.0.0.1");
            config.setString("jobmanager.rpc.address", "127.0.0.1");
            config.setString("taskmanager.host", "127.0.0.1");
            config.setString("taskmanager.bind-host", "127.0.0.1");
            config.setString("rest.bind-address", "127.0.0.1");
            config.setString("rest.address", "127.0.0.1");
            config.setString("rest.bind-port", "0");
            config.setString("taskmanager.memory.managed.size", "128m");
            config.setString("taskmanager.memory.network.min", "16m");
            config.setString("taskmanager.memory.network.max", "16m");
            var env = StreamExecutionEnvironment.createLocalEnvironment(config);
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(1);
            env.getConfig().disableClosureCleaner();
            var reader = SavepointReader.read(env, copy.directory().toUri().toString(), new EmbeddedRocksDBStateBackend());
            var stream = reader.readKeyedState(OperatorIdentifier.forUidHash(spec.operatorId()), new PrimitiveReader(spec),
                    type(spec.keyType()), Types.TUPLE(Types.STRING, Types.STRING));
            var rows = new ArrayList<QueryRow>();
            boolean truncated = false;
            try (var iterator = stream.executeAndCollect("Flink State Lens · local state query")) {
                while (iterator.hasNext()) {
                    LocalSnapshotCopy.checkInterrupted();
                    var row = iterator.next();
                    if (rows.size() == spec.limit()) { truncated = true; break; }
                    rows.add(new QueryRow(row.f0, row.f1));
                }
            }
            return new QueryReport(List.copyOf(rows), truncated, List.of(
                    "本地 DataStream BATCH 查询；样本覆盖整个算子的子任务。上限限制输出条数，恢复时仍需读取相关状态文件。",
                    "每个 key 的 List/Map 最多显示 1000 项，超出时标记截断；文本最多显示 65536 字符。",
                    "仅支持标准类型与默认 namespace。类型须与原状态描述符匹配；TTL/自定义序列化器/窗口状态需专用读取器。",
                    "Native savepoint / aligned checkpoint 使用 Flink 内部恢复机制，属于实验性支持。"));
        }
    }

    private void validateSchema(SnapshotSession session, QuerySpec spec) throws Exception {
        var operator = session.metadata().getOperatorStates().stream()
                .filter(op -> op.getOperatorID().toHexString().equalsIgnoreCase(spec.operatorId())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("找不到算子 " + spec.operatorId()));
        var names = new LinkedHashSet<String>();
        boolean found = false;
        for (var subtask : operator.getStates()) {
            if (!subtask.getInputChannelState().isEmpty() || !subtask.getResultSubpartitionState().isEmpty())
                throw new IllegalArgumentException("State Processor API 不支持 unaligned checkpoint 的业务查询。");
            for (var handle : subtask.getManagedKeyedState()) {
                var metadata = handle instanceof IncrementalRemoteKeyedStateHandle incremental ? incremental.getMetaDataStateHandle()
                        : handle instanceof KeyGroupsStateHandle full ? full.getDelegateStateHandle() : null;
                if (metadata == null) throw new IllegalArgumentException("该状态 handle 暂不支持业务查询: " + handle.getClass().getSimpleName());
                try (var input = session.files().open(metadata)) {
                    var proxy = new KeyedBackendSerializationProxy<>(session.classLoader());
                    proxy.read(new DataInputViewStreamWrapper(input));
                    for (var schema : proxy.getStateMetaInfoSnapshots()) {
                        names.add(schema.getName());
                        if (schema.getName().equals(spec.stateName())) {
                            String kind = schema.getOption(org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE);
                            if (!spec.kind().name().equals(kind)) throw new IllegalArgumentException("状态 " + spec.stateName() + " 实际类型为 " + kind + "，请选择对应类型。");
                            found = true;
                        }
                    }
                }
            }
        }
        if (!found) throw new IllegalArgumentException("不存在状态 " + spec.stateName() + "。可用状态: " + String.join(", ", names));
    }

    @SuppressWarnings("unchecked") private static TypeInformation<Object> type(ScalarType type) {
        TypeInformation<?> result = switch (type) {
            case STRING -> Types.STRING;
            case INT -> Types.INT;
            case LONG -> Types.LONG;
            case DOUBLE -> Types.DOUBLE;
            case BOOLEAN -> Types.BOOLEAN;
        };
        return (TypeInformation<Object>) result;
    }

    private static final class PrimitiveReader extends KeyedStateReaderFunction<Object, Tuple2<String, String>> {
        private final String stateName;
        private final Kind kind;
        private final ScalarType valueType;
        private final ScalarType mapKeyType;
        private transient ValueState<Object> value;
        private transient ListState<Object> list;
        private transient MapState<Object, Object> map;
        private transient ObjectMapper json;

        private PrimitiveReader(QuerySpec spec) {
            stateName = spec.stateName(); kind = spec.kind(); valueType = spec.valueType(); mapKeyType = spec.mapKeyType();
        }

        @Override public void open(Configuration ignored) {
            json = new ObjectMapper();
            switch (kind) {
                case VALUE -> value = getRuntimeContext().getState(new ValueStateDescriptor<>(stateName, type(valueType)));
                case LIST -> list = getRuntimeContext().getListState(new ListStateDescriptor<>(stateName, type(valueType)));
                case MAP -> map = getRuntimeContext().getMapState(new MapStateDescriptor<>(stateName, type(mapKeyType), type(valueType)));
            }
        }

        @Override public void readKey(Object key, Context context, Collector<Tuple2<String, String>> out) throws Exception {
            Object result;
            switch (kind) {
                case VALUE -> result = value.value();
                case LIST -> {
                    var values = new ArrayList<Object>();
                    var entries = list.get();
                    if (entries != null) for (Object item : entries) {
                        if (values.size() == 1000) { values.add("… (List items truncated)"); break; }
                        values.add(item);
                    }
                    result = values;
                }
                case MAP -> {
                    var values = new ArrayList<Map<String, Object>>();
                    var entries = map.entries();
                    if (entries != null) for (var item : entries) {
                        if (values.size() == 1000) { values.add(Map.of("truncated", true)); break; }
                        var pair = new LinkedHashMap<String, Object>();
                        pair.put("key", item.getKey()); pair.put("value", item.getValue()); values.add(pair);
                    }
                    result = values;
                }
                default -> throw new IllegalStateException("未知状态类型");
            }
            if (result != null) out.collect(Tuple2.of(bounded(json.writeValueAsString(key)), bounded(json.writeValueAsString(result))));
        }

        private static String bounded(String value) { return value.length() <= 65536 ? value : value.substring(0, 65536) + "… (text truncated)"; }
    }
}
