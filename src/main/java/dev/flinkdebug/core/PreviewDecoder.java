package dev.flinkdebug.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;

/** Decodes known Flink layouts using the serializers and field order saved in the snapshot. */
final class PreviewDecoder {
    static final int MAX_RECORD_BYTES = 1024 * 1024;
    private static final int MAX_HEX_BYTES = 2048;
    private static final int MAX_TEXT_CHARS = 4096;
    private static final int MAX_BIG_NUMBER_BYTES = 4096;
    private static final String BASE = "org.apache.flink.api.common.typeutils.base.";
    private static final Set<String> SCALAR_NAMES = Set.of("String", "Boolean", "Byte", "Short", "Int",
            "Long", "Float", "Double", "Char", "Date", "SqlDate", "SqlTime", "SqlTimestamp",
            "LocalDate", "LocalTime", "LocalDateTime", "Instant", "Void", "BigInt", "BigDec");
    private static final String VOID_NAMESPACE = "org.apache.flink.runtime.state.VoidNamespaceSerializer";
    private static final String POJO = "org.apache.flink.api.java.typeutils.runtime.PojoSerializer";
    private static final String TTL = "org.apache.flink.runtime.state.ttl.TtlStateFactory$TtlSerializer";
    private static final String TTL_AWARE = "org.apache.flink.runtime.state.ttl.TtlAwareSerializer";
    private static final String TIMER = "org.apache.flink.streaming.api.operators.TimerSerializer";
    private static final String TIME_WINDOW = "org.apache.flink.streaming.api.windowing.windows.TimeWindow$Serializer";
    private static final String GLOBAL_WINDOW = "org.apache.flink.streaming.api.windowing.windows.GlobalWindow$Serializer";
    private static final ObjectMapper JSON = new ObjectMapper();

    record RawBytes(byte[] bytes, int length) {
        boolean complete() { return bytes.length == length; }
    }

    record Schema(StateMetaInfoSnapshot metadata, TypeSerializer<?> keySerializer,
                  TypeSerializer<?> namespaceSerializer, TypeSerializer<?> valueSerializer,
                  PreviewReport.StateSchema report) {}

    private record TtlData(Object value, long timestamp) {}
    private static final class UnsupportedLayout extends IOException {
        UnsupportedLayout(String message) { super(message); }
    }

    static List<Schema> schemas(KeyedBackendSerializationProxy<?> proxy) {
        List<Schema> result = new ArrayList<>();
        TypeSerializerSnapshot<?> keySnapshot = proxy.getKeySerializerSnapshot();
        TypeSerializer<?> key = restoreKnown(keySnapshot);
        for (StateMetaInfoSnapshot state : proxy.getStateMetaInfoSnapshots()) {
            TypeSerializerSnapshot<?> namespaceSnapshot = state.getTypeSerializerSnapshot(NAMESPACE_SERIALIZER);
            TypeSerializerSnapshot<?> valueSnapshot = state.getTypeSerializerSnapshot(VALUE_SERIALIZER);
            TypeSerializer<?> namespace = restoreKnown(namespaceSnapshot);
            TypeSerializer<?> value = restoreKnown(valueSnapshot);
            TypeSerializer<?> reportedKey = key;
            if (state.getBackendStateType() == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE
                    && value != null && value.getClass().getName().equals(TIMER)) {
                try {
                    // Queue metadata stores both serializers inside the timer element serializer.
                    reportedKey = (TypeSerializer<?>) serializerMember(value, "getKeySerializer");
                    namespace = (TypeSerializer<?>) serializerMember(value, "getNamespaceSerializer");
                } catch (ReflectiveOperationException unavailable) {
                    value = null;
                }
            }
            String type = state.getOption(KEYED_STATE_TYPE);
            if (type == null) type = state.getBackendStateType().name();
            result.add(new Schema(state, reportedKey, namespace, value,
                    new PreviewReport.StateSchema(state.getName(), type,
                            serializerLabel(keySnapshot, reportedKey), serializerLabel(namespaceSnapshot, namespace),
                            serializerLabel(valueSnapshot, value))));
        }
        return result;
    }

    private static String serializerLabel(TypeSerializerSnapshot<?> snapshot, TypeSerializer<?> serializer) {
        if (serializer != null) return serializer.getClass().getName();
        return snapshot == null ? "unknown" : snapshot.getClass().getName() + " (snapshot; raw preview)";
    }

    private static TypeSerializer<?> restoreKnown(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot == null) return null;
        try {
            // Restoring an outer Flink serializer also restores every nested serializer.
            // Validate the complete tree first so unknown serializers are never invoked.
            if (!knownSnapshotTree(snapshot, 0, new java.util.IdentityHashMap<>(), new int[1])) return null;
            return snapshot.restoreSerializer();
        } catch (ReflectiveOperationException | RuntimeException | LinkageError unavailable) {
            return null;
        }
    }

    private static boolean knownSnapshotTree(TypeSerializerSnapshot<?> snapshot, int depth,
                                              java.util.IdentityHashMap<TypeSerializerSnapshot<?>, Boolean> active,
                                              int[] visits) throws ReflectiveOperationException {
        if (snapshot == null || depth > 16 || ++visits[0] > 4096 || active.containsKey(snapshot)) return false;
        active.put(snapshot, Boolean.TRUE);
        try {
            String name = snapshot.getClass().getName();
            if (name.equals(VOID_NAMESPACE + "$VoidNamespaceSerializerSnapshot")) return true;
            if (name.equals(TIME_WINDOW + "$TimeWindowSerializerSnapshot")
                    || name.equals(GLOBAL_WINDOW + "$GlobalWindowSerializerSnapshot")) return true;
            for (String scalar : SCALAR_NAMES) {
                if (name.equals(BASE + scalar + "Serializer$" + scalar + "SerializerSnapshot")) return true;
            }
            if (name.equals(BASE + "MapSerializerSnapshot") || name.equals(TTL + "Snapshot")
                    || name.equals(TIMER + "Snapshot")) {
                if (!(snapshot instanceof org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot<?, ?> composite)) return false;
                TypeSerializerSnapshot<?>[] nested = composite.getNestedSerializerSnapshots();
                if (nested == null || nested.length != 2) return false;
                for (TypeSerializerSnapshot<?> child : nested) {
                    if (!knownSnapshotTree(child, depth + 1, active, visits)) return false;
                }
                return true;
            }
            if (name.equals(TTL_AWARE + "Snapshot")) {
                // The public API's spelling is "Orinal" in Flink 2.2; reflection keeps 1.20 compatible.
                Object nested = snapshot.getClass().getMethod("getOrinalTypeSerializerSnapshot").invoke(snapshot);
                return nested instanceof TypeSerializerSnapshot<?> child
                        && knownSnapshotTree(child, depth + 1, active, visits);
            }
            if (name.equals(POJO + "Snapshot")) {
                Field dataField = snapshot.getClass().getDeclaredField("snapshotData");
                dataField.setAccessible(true);
                Object data = dataField.get(snapshot);
                if (data == null || !data.getClass().getName().equals(POJO + "SnapshotData")) return false;
                // Both supported versions use these three registries. Missing fields or an API
                // change leave this serializer raw rather than restoring unexamined serializers.
                for (String accessor : List.of("getFieldSerializerSnapshots", "getRegisteredSubclassSerializerSnapshots",
                        "getNonRegisteredSubclassSerializerSnapshots")) {
                    Method method = data.getClass().getDeclaredMethod(accessor);
                    method.setAccessible(true);
                    Object registry = method.invoke(data);
                    if (!(registry instanceof org.apache.flink.util.LinkedOptionalMap<?, ?> optional)
                            || optional.hasAbsentKeysOrValues() || optional.size() > 256) return false;
                    Map<?, ?> snapshots = optional.unwrapOptionals();
                    if (snapshots.size() > 256) return false;
                    for (Object nested : snapshots.values()) {
                        if (!(nested instanceof TypeSerializerSnapshot<?> child)
                                || !knownSnapshotTree(child, depth + 1, active, visits)) return false;
                    }
                }
                return true;
            }
            return false;
        } finally {
            active.remove(snapshot);
        }
    }

    static PreviewReport.StateEntry entry(String stateName, Schema schema, RawBytes key, RawBytes value,
                                         int expectedKeyGroup, int keyGroupPrefixBytes) {
        String keyText = null;
        String namespaceText = null;
        String mapKeyText = null;
        String valueText = null;
        Long ttlTimestamp = null;
        Long timerTimestamp = null;
        String timerType = null;
        int keyGroup = expectedKeyGroup;
        String status;
        try {
            if (keyGroupPrefixBytes < 1 || keyGroupPrefixBytes > 2) throw new IOException("invalid key-group prefix length");
            if (key.bytes.length < keyGroupPrefixBytes) throw new IOException("key-group prefix missing");
            DataInputDeserializer keyView = new DataInputDeserializer(key.bytes);
            int encodedKeyGroup = CompositeKeySerializationUtils.readKeyGroup(keyGroupPrefixBytes, keyView);
            if (expectedKeyGroup >= 0 && encodedKeyGroup != expectedKeyGroup) {
                throw new IOException("key-group prefix disagrees with snapshot offset");
            }
            keyGroup = encodedKeyGroup;
            if (schema == null) {
                status = "RAW: state schema unavailable";
            } else if (!key.complete() || !value.complete()) {
                status = "RAW: record exceeds " + MAX_RECORD_BYTES + " byte decoding limit";
            } else if (schema.metadata.getBackendStateType() == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) {
                if (schema.valueSerializer == null || !schema.valueSerializer.getClass().getName().equals(TIMER)) {
                    status = "RAW: priority queue serializer requires an explicit reader";
                } else {
                    // RocksDB queues and canonical queue iterators write key-group + TimerSerializer
                    // bytes as the key, and no value. TimerSerializer flips the timestamp sign bit
                    // for lexicographic ordering; its key/namespace have no composite-key markers.
                    if (value.length != 0) throw new IOException("non-empty timer value");
                    long timestamp = keyView.readLong() ^ Long.MIN_VALUE;
                    Object decodedKey = readKnown(schema.keySerializer, keyView, key.bytes, 0);
                    Object decodedNamespace = readKnown(schema.namespaceSerializer, keyView, key.bytes, 0);
                    requireConsumed(keyView, "timer");
                    keyText = render(decodedKey);
                    namespaceText = render(decodedNamespace);
                    timerTimestamp = timestamp;
                    timerType = timerType(stateName);
                    status = "DECODED: Flink timer snapshot serializers";
                }
            } else if (schema.metadata.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                status = "RAW: " + schema.metadata.getBackendStateType() + " layout";
            } else if (!Set.of("VALUE", "REDUCING", "AGGREGATING", "MAP").contains(schema.report.type())) {
                status = "RAW: " + schema.report.type() + " layout requires an explicit reader";
            } else {
                boolean isMap = "MAP".equals(schema.report.type());
                TypeSerializer<?> stateSerializer = unwrap(schema.valueSerializer);
                MapSerializer<?, ?> mapSerializer = stateSerializer instanceof MapSerializer<?, ?> map ? map : null;
                boolean keyDecoded = false;
                boolean valueDecoded = false;
                if (scalarSupported(schema.keySerializer) && scalarSupported(schema.namespaceSerializer)) {
                    boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            schema.keySerializer, schema.namespaceSerializer);
                    validateScalar(schema.keySerializer, keyView, key.bytes);
                    Object decodedKey = CompositeKeySerializationUtils.readKey(schema.keySerializer, keyView, ambiguous);
                    validateScalar(schema.namespaceSerializer, keyView, key.bytes);
                    Object decodedNamespace = CompositeKeySerializationUtils.readNamespace(schema.namespaceSerializer, keyView, ambiguous);
                    String decodedMapKey = null;
                    if (isMap && mapSerializer != null) {
                        decodedMapKey = render(readKnown(mapSerializer.getKeySerializer(), keyView, key.bytes, 0));
                    }
                    if (!isMap || mapSerializer != null) requireConsumed(keyView, "composite-key");
                    keyText = render(decodedKey);
                    namespaceText = render(decodedNamespace);
                    mapKeyText = decodedMapKey;
                    keyDecoded = !isMap || mapSerializer != null;
                }
                TypeSerializer<?> valueSerializer = isMap
                        ? mapSerializer == null ? null : mapSerializer.getValueSerializer()
                        : schema.valueSerializer;
                if (valueSerializer != null) {
                    DataInputDeserializer valueView = new DataInputDeserializer(value.bytes);
                    Object decodedValue = isMap && readNullFlag(valueView) ? null
                            : readKnown(valueSerializer, valueView, value.bytes, 0);
                    requireConsumed(valueView, "value");
                    if (decodedValue instanceof TtlData ttl) {
                        ttlTimestamp = ttl.timestamp();
                        decodedValue = ttl.value();
                    }
                    valueText = render(decodedValue);
                    valueDecoded = true;
                }
                status = keyDecoded && valueDecoded ? "DECODED: Flink snapshot serializers"
                        : keyText != null || valueDecoded ? "PARTIAL: unsupported serializer fields remain raw"
                        : "RAW: serializer requires an explicit reader";
            }
        } catch (UnsupportedLayout unsupported) {
            status = (keyText != null || valueText != null ? "PARTIAL: " : "RAW: ") + bounded(unsupported.getMessage());
        } catch (Exception | LinkageError decodingFailure) {
            status = "RAW / DECODE_FAILED: " + decodingFailure.getClass().getSimpleName() + ": "
                    + bounded(decodingFailure.getMessage() == null ? "invalid serialized record" : decodingFailure.getMessage());
        }
        return new PreviewReport.StateEntry(stateName, keyGroup, keyText, namespaceText, valueText,
                hex(key), hex(value), status, mapKeyText, ttlTimestamp, timerTimestamp, timerType);
    }

    private static String timerType(String name) {
        if (name.startsWith("_timer_state/event_")) return "EVENT_TIME";
        if (name.startsWith("_timer_state/processing_")) return "PROCESSING_TIME";
        return null;
    }

    private static void requireConsumed(DataInputDeserializer input, String field) throws IOException {
        if (input.available() != 0) throw new IOException("unconsumed " + field + " bytes");
    }

    private static boolean readNullFlag(DataInputDeserializer input) throws IOException {
        int flag = input.readUnsignedByte();
        if (flag > 1) throw new IOException("invalid null marker");
        return flag == 1;
    }

    private static TypeSerializer<?> unwrap(TypeSerializer<?> serializer) throws ReflectiveOperationException {
        // Flink 2.x introduces a transparent wrapper; 1.20 has no such class.
        if (serializer != null && serializer.getClass().getName().equals(TTL_AWARE)) {
            return (TypeSerializer<?>) serializer.getClass().getMethod("getOriginalTypeSerializer").invoke(serializer);
        }
        return serializer;
    }

    private static boolean scalarSupported(TypeSerializer<?> serializer) {
        if (serializer == null) return false;
        String name = serializer.getClass().getName();
        return name.equals(VOID_NAMESPACE) || SCALAR_NAMES.stream().anyMatch(s -> name.equals(BASE + s + "Serializer"));
    }

    private static Object readKnown(TypeSerializer<?> original, DataInputDeserializer input,
                                    byte[] backing, int depth) throws Exception {
        if (depth > 16) throw new UnsupportedLayout("nested serializer depth exceeds preview limit");
        TypeSerializer<?> serializer = unwrap(original);
        if (serializer == null) throw new UnsupportedLayout("serializer unavailable");
        if (scalarSupported(serializer)) {
            validateScalar(serializer, input, backing);
            return serializer.deserialize(input);
        }
        String name = serializer.getClass().getName();
        if (name.equals(TIME_WINDOW)) {
            Map<String, Object> window = new LinkedHashMap<>();
            window.put("start", input.readLong());
            window.put("end", input.readLong());
            return window;
        }
        if (name.equals(GLOBAL_WINDOW)) {
            if (input.readUnsignedByte() != 0) throw new IOException("invalid global-window marker");
            return "GlobalWindow";
        }
        if (name.equals(TTL)) {
            TypeSerializer<?> timestamp = (TypeSerializer<?>) serializerMember(serializer, "getTimestampSerializer");
            if (!timestamp.getClass().getName().equals(BASE + "LongSerializer")) {
                throw new UnsupportedLayout("unsupported TTL timestamp serializer");
            }
            long lastAccess = input.readLong();
            TypeSerializer<?> user = (TypeSerializer<?>) serializerMember(serializer, "getValueSerializer");
            return new TtlData(readKnown(user, input, backing, depth + 1), lastAccess);
        }
        if (name.equals(POJO)) {
            int flags = input.readUnsignedByte();
            if (flags == 1) return null;
            if (flags != 2) throw new UnsupportedLayout("POJO subclass/unknown flags require an explicit reader");
            // Use the saved serializer's field order, never source declaration or reflection order.
            Field[] fields = (Field[]) serializerMember(serializer, "getFields");
            TypeSerializer<?>[] serializers = (TypeSerializer<?>[]) serializerMember(serializer, "getFieldSerializers");
            if (fields.length != serializers.length || fields.length > 256) {
                throw new UnsupportedLayout("POJO field count exceeds preview limit");
            }
            Map<String, Object> object = new LinkedHashMap<>();
            for (int i = 0; i < fields.length; i++) {
                if (fields[i] == null) throw new UnsupportedLayout("POJO field no longer available in job JAR");
                if (object.containsKey(fields[i].getName())) throw new UnsupportedLayout("duplicate POJO field name");
                Object field = readNullFlag(input) ? null : readKnown(serializers[i], input, backing, depth + 1);
                object.put(fields[i].getName(), jsonValue(field));
            }
            return object;
        }
        throw new UnsupportedLayout("unsupported serializer " + name + "; bytes remain raw");
    }

    private static Object serializerMember(TypeSerializer<?> serializer, String name) throws ReflectiveOperationException {
        Method method = serializer.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        return method.invoke(serializer);
    }

    /** Check variable-length scalar allocations against the actual bounded record. */
    private static void validateScalar(TypeSerializer<?> serializer, DataInputDeserializer input,
                                       byte[] backing) throws IOException {
        String name = serializer.getClass().getName();
        DataInputDeserializer duplicate = new DataInputDeserializer(backing, input.getPosition(), input.available());
        if (name.equals(BASE + "BigIntSerializer") || name.equals(BASE + "BigDecSerializer")) {
            int length = duplicate.readInt();
            if (length < 0 || length > MAX_BIG_NUMBER_BYTES + 4 || length > 3 && length - 4 > duplicate.available()) {
                throw new IOException("big-number length exceeds bounded record");
            }
        }
        if (!name.equals(BASE + "StringSerializer")) return;
        long encodedLength = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int next = duplicate.readUnsignedByte();
            encodedLength |= (long) (next & 0x7f) << shift;
            if ((next & 0x80) == 0) {
                if (encodedLength > MAX_RECORD_BYTES + 1L || encodedLength - 1L > duplicate.available()) {
                    throw new IOException("string length exceeds available bounded record");
                }
                return;
            }
        }
        throw new IOException("invalid string length varint");
    }

    private static Object jsonValue(Object object) {
        if (object == null || object instanceof String || object instanceof Number || object instanceof Boolean || object instanceof Map) return object;
        if (object instanceof TtlData ttl) {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("value", jsonValue(ttl.value()));
            fields.put("ttlTimestamp", ttl.timestamp());
            return fields;
        }
        return String.valueOf(object);
    }

    private static String render(Object object) throws IOException {
        // Preserve complete decoded fields so filters also match beyond the first 4,096 characters.
        // Input records remain subject to MAX_RECORD_BYTES; only diagnostics and raw hex are bounded.
        return object instanceof Map ? JSON.writeValueAsString(object) : object == null ? "null" : String.valueOf(object);
    }

    private static String bounded(String value) {
        return value.length() <= MAX_TEXT_CHARS ? value : value.substring(0, MAX_TEXT_CHARS) + "… (text truncated)";
    }

    private static String hex(RawBytes value) {
        int count = Math.min(value.bytes.length, MAX_HEX_BYTES);
        String hex = HexFormat.of().formatHex(value.bytes, 0, count);
        return count == value.length ? hex : hex + "… (" + value.length + " bytes; prefix shown)";
    }
}
