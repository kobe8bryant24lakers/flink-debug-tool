package dev.flinkdebug.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Generates an explicitly requested example using Flink itself, never synthetic UI data. */
public final class ExampleSnapshot {
    public static final String UID = "example-orders";
    private ExampleSnapshot() {}

    public static void create(Path directory) throws Exception {
        if (Files.exists(directory)) throw new IllegalArgumentException("示例输出目录必须尚不存在: " + directory);
        Configuration config = new Configuration();
        config.setString("jobmanager.bind-host", "127.0.0.1");
        config.setString("jobmanager.rpc.address", "127.0.0.1");
        config.setString("taskmanager.bind-host", "127.0.0.1");
        config.setString("taskmanager.host", "127.0.0.1");
        config.setString("rest.address", "127.0.0.1");
        config.setString("rest.bind-address", "127.0.0.1");
        config.setString("rest.bind-port", "0");
        config.setString("taskmanager.memory.managed.size", "128m");
        config.setString("taskmanager.memory.network.min", "16m");
        config.setString("taskmanager.memory.network.max", "16m");
        var env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().disableClosureCleaner();
        env.getConfig().enableObjectReuse();
        var source = env.fromCollection(List.of(Tuple2.of("alice", 7L), Tuple2.of("bob", 3L), Tuple2.of("alice", 5L)), Types.TUPLE(Types.STRING, Types.LONG));
        var bootstrap = OperatorTransformation.bootstrapWith(source).setMaxParallelism(128)
                .keyBy(new OrderKey(), Types.STRING).transform(new Bootstrap());
        SavepointWriter.newSavepoint(env, new EmbeddedRocksDBStateBackend(), 128)
                .withOperator(UID, bootstrap).write(directory.toAbsolutePath().toUri().toString());
        env.execute("Flink State Lens · generate example savepoint");
    }

    public static final class OrderKey implements KeySelector<Tuple2<String, Long>, String> {
        @Override public String getKey(Tuple2<String, Long> value) { return value.f0; }
    }

    public static final class Bootstrap extends KeyedStateBootstrapFunction<String, Tuple2<String, Long>> {
        private transient ValueState<Long> count;
        private transient ListState<Long> values;
        private transient MapState<String, Long> attributes;
        @Override public void open(Configuration ignored) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("order-count", Types.LONG));
            values = getRuntimeContext().getListState(new ListStateDescriptor<>("order-values", Types.LONG));
            attributes = getRuntimeContext().getMapState(new MapStateDescriptor<>("order-map", Types.STRING, Types.LONG));
        }
        @Override public void processElement(Tuple2<String, Long> value, Context ignored) throws Exception {
            Long previous = count.value();
            count.update((previous == null ? 0L : previous) + value.f1);
            values.add(value.f1);
            attributes.put("last", value.f1);
        }
    }
}
