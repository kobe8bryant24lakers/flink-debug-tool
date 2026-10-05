package dev.flinkdebug.core;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;

/** Decoding is deliberately limited to serializers with a known scalar representation. */
final class PreviewDecoder {
    static final int MAX_RECORD_BYTES = 1024 * 1024;
    private static final int MAX_HEX_BYTES = 2048;
    private static final int MAX_TEXT_CHARS = 4096;
    private static final String BASE = "org.apache.flink.api.common.typeutils.base.";
    private static final Set<String> SCALAR_NAMES = Set.of("String", "Boolean", "Byte", "Short", "Int",
            "Long", "Float", "Double", "Char", "Date", "SqlDate", "SqlTime", "SqlTimestamp",
            "LocalDate", "LocalTime", "LocalDateTime", "Instant", "Void");
    private static final String VOID_NAMESPACE = "org.apache.flink.runtime.state.VoidNamespaceSerializer";

    record RawBytes(byte[] bytes, int length) {
        boolean complete() { return bytes.length == length; }
    }

    record Schema(StateMetaInfoSnapshot metadata, TypeSerializer<?> keySerializer,
                  TypeSerializer<?> namespaceSerializer, TypeSerializer<?> valueSerializer,
                  PreviewReport.StateSchema report) {}

    static List<Schema> schemas(KeyedBackendSerializationProxy<?> proxy) {
        List<Schema> result = new ArrayList<>();
        TypeSerializerSnapshot<?> keySnapshot = proxy.getKeySerializerSnapshot();
        TypeSerializer<?> key = restoreStandard(keySnapshot);
        for (StateMetaInfoSnapshot state : proxy.getStateMetaInfoSnapshots()) {
            TypeSerializerSnapshot<?> namespaceSnapshot = state.getTypeSerializerSnapshot(NAMESPACE_SERIALIZER);
            TypeSerializerSnapshot<?> valueSnapshot = state.getTypeSerializerSnapshot(VALUE_SERIALIZER);
            TypeSerializer<?> namespace = restoreStandard(namespaceSnapshot);
            TypeSerializer<?> value = restoreStandard(valueSnapshot);
            String type = state.getOption(KEYED_STATE_TYPE);
            if (type == null) type = state.getBackendStateType().name();
            result.add(new Schema(state, key, namespace, value,
                    new PreviewReport.StateSchema(state.getName(), type,
                            serializerLabel(keySnapshot, key), serializerLabel(namespaceSnapshot, namespace),
                            serializerLabel(valueSnapshot, value))));
        }
        return result;
    }

    private static String serializerLabel(TypeSerializerSnapshot<?> snapshot, TypeSerializer<?> serializer) {
        if (serializer != null) return serializer.getClass().getName();
        return snapshot == null ? "unknown" : snapshot.getClass().getName() + " (snapshot; raw preview)";
    }

    private static TypeSerializer<?> restoreStandard(TypeSerializerSnapshot<?> snapshot) {
        if (snapshot == null) return null;
        String snapshotName = snapshot.getClass().getName();
        String serializerName = null;
        for (String scalar : SCALAR_NAMES) {
            String candidate = BASE + scalar + "Serializer";
            if (snapshotName.equals(candidate + "$" + scalar + "SerializerSnapshot")) {
                serializerName = candidate;
                break;
            }
        }
        if (snapshotName.equals(VOID_NAMESPACE + "$VoidNamespaceSerializerSnapshot")) {
            serializerName = VOID_NAMESPACE;
        }
        if (serializerName == null) return null;
        try {
            TypeSerializer<?> serializer = snapshot.restoreSerializer();
            return serializer != null && serializer.getClass().getName().equals(serializerName) ? serializer : null;
        } catch (RuntimeException | LinkageError unavailable) {
            return null;
        }
    }

    static PreviewReport.StateEntry entry(String stateName, Schema schema, RawBytes key, RawBytes value,
                                         int expectedKeyGroup, int keyGroupPrefixBytes) {
        String keyText = null;
        String namespaceText = null;
        String valueText = null;
        int keyGroup = expectedKeyGroup;
        String status;
        try {
            if (key.bytes.length < keyGroupPrefixBytes) throw new IOException("key-group prefix missing");
            DataInputDeserializer keyView = new DataInputDeserializer(key.bytes);
            int encodedKeyGroup = CompositeKeySerializationUtils.readKeyGroup(keyGroupPrefixBytes, keyView);
            if (expectedKeyGroup >= 0 && encodedKeyGroup != expectedKeyGroup) {
                throw new IOException("key-group prefix disagrees with snapshot offset");
            }
            keyGroup = encodedKeyGroup;
            if (schema == null) {
                status = "RAW: state schema unavailable";
            } else if (schema.metadata.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                status = "RAW: " + schema.metadata.getBackendStateType() + " layout";
            } else if (!Set.of("VALUE", "REDUCING", "AGGREGATING").contains(schema.report.type())) {
                status = "RAW: " + schema.report.type() + " layout requires an explicit reader";
            } else if (!key.complete() || !value.complete()) {
                status = "RAW: record exceeds " + MAX_RECORD_BYTES + " byte decoding limit";
            } else {
                boolean keyDecoded = false;
                boolean valueDecoded = false;
                if (schema.keySerializer != null && schema.namespaceSerializer != null) {
                    boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            schema.keySerializer, schema.namespaceSerializer);
                    validateScalar(schema.keySerializer, keyView, key.bytes);
                    Object decodedKey = CompositeKeySerializationUtils.readKey(schema.keySerializer, keyView, ambiguous);
                    validateScalar(schema.namespaceSerializer, keyView, key.bytes);
                    Object decodedNamespace = CompositeKeySerializationUtils.readNamespace(
                            schema.namespaceSerializer, keyView, ambiguous);
                    if (keyView.available() != 0) throw new IOException("unconsumed composite-key bytes");
                    keyText = render(decodedKey);
                    namespaceText = render(decodedNamespace);
                    keyDecoded = true;
                }
                if (schema.valueSerializer != null) {
                    DataInputDeserializer valueView = new DataInputDeserializer(value.bytes);
                    validateScalar(schema.valueSerializer, valueView, value.bytes);
                    Object decodedValue = schema.valueSerializer.deserialize(valueView);
                    if (valueView.available() != 0) throw new IOException("unconsumed value bytes");
                    valueText = render(decodedValue);
                    valueDecoded = true;
                }
                status = keyDecoded && valueDecoded ? "DECODED: Flink standard serializers"
                        : keyDecoded || valueDecoded ? "PARTIAL: unsupported serializer fields remain raw"
                        : "RAW: serializer requires an explicit reader";
            }
        } catch (Exception decodingFailure) {
            // Keep successful independent fields visible, but never label the whole record decoded.
            status = "RAW / DECODE_FAILED: " + decodingFailure.getClass().getSimpleName() + ": "
                    + bounded(decodingFailure.getMessage() == null ? "invalid serialized record" : decodingFailure.getMessage());
        }
        return new PreviewReport.StateEntry(stateName, keyGroup, keyText, namespaceText, valueText,
                hex(key), hex(value), status);
    }

    /** Prevent a corrupt String length from allocating more than the bounded record. */
    private static void validateScalar(TypeSerializer<?> serializer, DataInputDeserializer input,
                                       byte[] backing) throws IOException {
        if (!serializer.getClass().getName().equals(BASE + "StringSerializer")) return;
        DataInputDeserializer duplicate = new DataInputDeserializer(backing, input.getPosition(), input.available());
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

    private static String render(Object object) {
        return bounded(object == null ? "null" : String.valueOf(object));
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
