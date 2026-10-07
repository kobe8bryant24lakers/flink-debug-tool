package dev.flinkdebug.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.BigDecSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;
import static org.junit.jupiter.api.Assertions.*;

/** All records and serializer metadata are generated locally from artificial values. */
public class PreviewDecoderTest {
    private static final String STATE_NAME = "fixture-map";
    private static final int MAX_PARALLELISM = 256;

    @Test void decodesStringBusinessKeyAndLongMapKeyUsingRoundtrippedMapMetadata() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] bytes = mapValue(121L, LongSerializer.INSTANCE);

        PreviewReport.StateEntry entry = fixture.decode(bytes);

        assertDecoded(entry);
        assertEquals(STATE_NAME, entry.stateName());
        assertEquals(fixture.keyGroup(), entry.keyGroup());
        assertEquals(2, fixture.prefix());
        assertEquals("fixture-key", entry.key());
        assertEquals("37", entry.mapKey());
        assertEquals("121", entry.value());
        assertNull(entry.ttlTimestamp());
        assertEquals(HexFormat.of().formatHex(fixture.key()), entry.keyHex());
        assertEquals(HexFormat.of().formatHex(bytes), entry.valueHex());
    }

    @Test void decodesPojoMapValueWithBigDecimalAndIntFields() throws Exception {
        TypeSerializer<SamplePojo> serializer = TypeInformation.of(SamplePojo.class)
                .createSerializer(new ExecutionConfig().getSerializerConfig());
        assertInstanceOf(PojoSerializer.class, serializer, "fixture must use Flink's POJO serializer");
        Fixture fixture = fixture("fixture-object", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                501L, LongSerializer.INSTANCE, serializer);
        SamplePojo sample = new SamplePojo();
        sample.decimal = new BigDecimal("18.625");
        sample.count = 4;

        PreviewReport.StateEntry entry = fixture.decode(mapValue(sample, serializer));

        assertDecoded(entry);
        assertEquals("fixture-object", entry.key());
        assertEquals("501", entry.mapKey());
        JsonNode decoded = new ObjectMapper().readTree(entry.value());
        assertEquals(new BigDecimal("18.625"), decoded.required("decimal").decimalValue());
        assertEquals(4, decoded.required("count").intValue());
        assertNull(entry.ttlTimestamp());
    }

    @Test void ttlLongMapEntryExposesStoredTimestampWithoutFilteringOldRecords() throws Exception {
        TypeSerializer<TtlValue<Long>> ttl = new TtlStateFactory.TtlSerializer<>(
                LongSerializer.INSTANCE, LongSerializer.INSTANCE);
        Fixture fixture = fixture("fixture-ttl", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                "slot", StringSerializer.INSTANCE, ttl);
        // This timestamp is deliberately ancient: a snapshot preview must show stored bytes,
        // rather than use the viewer's processing time to silently filter state.
        PreviewReport.StateEntry entry = fixture.decode(mapValue(new TtlValue<>(88L, 1L), ttl));

        assertDecoded(entry);
        assertEquals("fixture-ttl", entry.key());
        assertEquals("slot", entry.mapKey());
        assertEquals("88", entry.value());
        assertEquals(Long.valueOf(1L), entry.ttlTimestamp());
    }

    @Test void nullMapValueUsesNullMarkerWithoutAttemptingToReadALong() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] bytes = mapValue(null, LongSerializer.INSTANCE);
        assertEquals(1, bytes.length);

        PreviewReport.StateEntry entry = fixture.decode(bytes);

        assertDecoded(entry);
        assertEquals("37", entry.mapKey());
        assertEquals("null", entry.value());
        assertNull(entry.ttlTimestamp());
    }

    @Test void variableSizedBusinessKeyAndNamespaceKeepTheirAmbiguousBoundaries() throws Exception {
        String businessKey = "fixture-key/" + "x".repeat(300);
        String namespace = "fixture-namespace/" + "y".repeat(300);
        String mapKey = "entry-\u03bb";
        assertTrue(CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                StringSerializer.INSTANCE, StringSerializer.INSTANCE));
        Fixture fixture = fixture(businessKey, StringSerializer.INSTANCE,
                namespace, StringSerializer.INSTANCE, mapKey, StringSerializer.INSTANCE,
                IntSerializer.INSTANCE);

        PreviewReport.StateEntry entry = fixture.decode(mapValue(12345, IntSerializer.INSTANCE));

        assertDecoded(entry);
        assertEquals(businessKey, entry.key());
        assertEquals(namespace, entry.namespace());
        assertEquals(mapKey, entry.mapKey());
        assertEquals("12345", entry.value());
    }

    @Test void truncatedMapKeyClearlyFailsDecoding() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] key = Arrays.copyOf(fixture.key(), fixture.key().length - 1);

        PreviewReport.StateEntry entry = fixture.decode(key, mapValue(121L, LongSerializer.INSTANCE));

        assertDecodeFailed(entry);
        assertEquals(HexFormat.of().formatHex(key), entry.keyHex());
    }

    @Test void truncatedMapValueClearlyFailsDecoding() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] complete = mapValue(121L, LongSerializer.INSTANCE);
        byte[] truncated = Arrays.copyOf(complete, complete.length - 1);

        PreviewReport.StateEntry entry = fixture.decode(truncated);

        assertDecodeFailed(entry);
        assertEquals(HexFormat.of().formatHex(truncated), entry.valueHex());
    }

    @Test void trailingCompositeKeyBytesClearlyFailDecoding() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] key = appendByte(fixture.key());

        PreviewReport.StateEntry entry = fixture.decode(key, mapValue(121L, LongSerializer.INSTANCE));

        assertDecodeFailed(entry);
        assertEquals(HexFormat.of().formatHex(key), entry.keyHex());
    }

    @Test void trailingMapValueBytesClearlyFailDecoding() throws Exception {
        Fixture fixture = scalarFixture();
        byte[] bytes = appendByte(mapValue(121L, LongSerializer.INSTANCE));

        PreviewReport.StateEntry entry = fixture.decode(bytes);

        assertDecodeFailed(entry);
        assertEquals(HexFormat.of().formatHex(bytes), entry.valueHex());
    }

    @Test void nullMapMarkerWithTrailingBytesClearlyFailsDecoding() throws Exception {
        Fixture fixture = scalarFixture();

        PreviewReport.StateEntry entry = fixture.decode(appendByte(mapValue(null, LongSerializer.INSTANCE)));

        assertDecodeFailed(entry);
    }

    @Test void nullablePojoFieldIsRenderedAsJsonNull() throws Exception {
        TypeSerializer<SamplePojo> serializer = TypeInformation.of(SamplePojo.class)
                .createSerializer(new ExecutionConfig().getSerializerConfig());
        Fixture fixture = fixture("fixture-null-field", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                501L, LongSerializer.INSTANCE, serializer);
        SamplePojo sample = new SamplePojo();
        sample.count = 3;
        sample.decimal = null;
        PreviewReport.StateEntry entry = fixture.decode(mapValue(sample, serializer));
        assertDecoded(entry);
        JsonNode decoded = new ObjectMapper().readTree(entry.value());
        assertTrue(decoded.required("decimal").isNull());
        assertEquals(3, decoded.required("count").intValue());
    }

    @Test void invalidMapNullMarkerIsRejected() throws Exception {
        byte[] bytes = mapValue(121L, LongSerializer.INSTANCE);
        bytes[0] = 2;
        assertDecodeFailed(scalarFixture().decode(bytes));
    }

    @Test void corruptBigDecimalLengthCannotAllocateItsClaimedPayload() throws Exception {
        Fixture fixture = fixture("fixture-big-number", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                501L, LongSerializer.INSTANCE, BigDecSerializer.INSTANCE);
        DataOutputSerializer bytes = new DataOutputSerializer(8);
        bytes.writeBoolean(false);
        bytes.writeInt(Integer.MAX_VALUE);
        PreviewReport.StateEntry entry = fixture.decode(bytes.getCopyOfBuffer());
        assertDecodeFailed(entry);
        assertNull(entry.value());
    }

    @Test void corruptStringMapKeyLengthCannotAllocateItsClaimedPayload() throws Exception {
        Fixture fixture = fixture("fixture-string-key", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                "entry", StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        byte[] key = Arrays.copyOf(fixture.key(), fixture.key().length);
        Arrays.fill(key, key.length - 6, key.length, (byte) 0xff);
        assertDecodeFailed(fixture.decode(key, mapValue(121L, LongSerializer.INSTANCE)));
    }

    @Test void decodedStringFieldsKeepContentBeyondFormerTextPreviewLimit() throws Exception {
        String businessKey = "key-" + "k".repeat(5000) + "-key-tail";
        String namespace = "namespace-" + "n".repeat(5000) + "-namespace-tail";
        String mapKey = "map-" + "m".repeat(5000) + "-map-tail";
        String value = "value-" + "v".repeat(5000) + "-value-tail";
        Fixture fixture = fixture(businessKey, StringSerializer.INSTANCE, namespace,
                StringSerializer.INSTANCE, mapKey, StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        PreviewReport.StateEntry entry = fixture.decode(mapValue(value, StringSerializer.INSTANCE));
        assertDecoded(entry);
        assertEquals(businessKey, entry.key());
        assertEquals(namespace, entry.namespace());
        assertEquals(mapKey, entry.mapKey());
        assertEquals(value, entry.value());
    }

    @Test void eventTimeTimerUsesSavedNestedSerializersForNativeAndCanonicalRecordLayouts() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_fixture", "timer-key",
                StringSerializer.INSTANCE, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 123456789L);
        for (int expectedGroup : List.of(fixture.group(), -1)) {
            PreviewReport.StateEntry entry = fixture.decode(fixture.key(), new byte[0], expectedGroup);
            assertDecoded(entry);
            assertEquals("timer-key", entry.key());
            assertEquals(fixture.group(), entry.keyGroup());
            assertEquals(Long.valueOf(123456789L), entry.timerTimestamp());
            assertEquals("EVENT_TIME", entry.timerType());
            assertEquals(VoidNamespaceSerializer.class.getName(), fixture.schema().report().namespaceSerializer());
            assertNull(entry.value(), "timers have no business value payload");
            assertNull(entry.ttlTimestamp(), "timer timestamp must not be confused with TTL");
        }
    }

    @Test void processingTimerPreservesSignedTimestampAndVariableKeyNamespaceBoundaries() throws Exception {
        String key = "timer-" + "k".repeat(300);
        String namespace = "namespace-" + "n".repeat(300);
        for (long timestamp : List.of(Long.MIN_VALUE, -17L, 0L, Long.MAX_VALUE)) {
            TimerFixture fixture = timerFixture("_timer_state/processing_fixture", key,
                    StringSerializer.INSTANCE, namespace, StringSerializer.INSTANCE, timestamp);
            PreviewReport.StateEntry entry = fixture.decode();
            assertDecoded(entry);
            assertEquals(key, entry.key());
            assertEquals(namespace, entry.namespace());
            assertEquals(Long.valueOf(timestamp), entry.timerTimestamp());
            assertEquals("PROCESSING_TIME", entry.timerType());
        }
    }

    @Test void windowTimerExposesSavedWindowBounds() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_window", "window-key",
                StringSerializer.INSTANCE, new TimeWindow(1200L, 1800L), new TimeWindow.Serializer(), 1799L);
        PreviewReport.StateEntry entry = fixture.decode();
        assertDecoded(entry);
        JsonNode namespace = new ObjectMapper().readTree(entry.namespace());
        assertEquals(1200L, namespace.required("start").longValue());
        assertEquals(1800L, namespace.required("end").longValue());
        assertEquals(Long.valueOf(1799L), entry.timerTimestamp());
    }

    @Test void globalWindowTimerUsesItsSerializedMarker() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/processing_global", 42,
                IntSerializer.INSTANCE, GlobalWindow.get(), new GlobalWindow.Serializer(), 5100L);
        PreviewReport.StateEntry entry = fixture.decode();
        assertDecoded(entry);
        assertEquals("42", entry.key());
        assertEquals("GlobalWindow", entry.namespace());
        byte[] corrupt = Arrays.copyOf(fixture.key(), fixture.key().length);
        corrupt[corrupt.length - 1] = 1;
        assertDecodeFailed(fixture.decode(corrupt, new byte[0], fixture.group()));
    }

    @Test void customQueueNameDoesNotGuessTimerKindFromIncidentalWords() throws Exception {
        TimerFixture fixture = timerFixture("custom_event_processing_queue", "key",
                StringSerializer.INSTANCE, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 99L);
        PreviewReport.StateEntry entry = fixture.decode();
        assertDecoded(entry);
        assertNull(entry.timerType());
        assertEquals(Long.valueOf(99L), entry.timerTimestamp());
    }

    @Test void timerRejectsTruncatedTimestampOrNamespaceAndTrailingKeyBytes() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_fixture", "key",
                StringSerializer.INSTANCE, 17L, LongSerializer.INSTANCE, 99L);
        for (byte[] corrupt : List.of(Arrays.copyOf(fixture.key(), fixture.prefix() + Long.BYTES - 1),
                Arrays.copyOf(fixture.key(), fixture.key().length - 1), appendByte(fixture.key()))) {
            PreviewReport.StateEntry entry = fixture.decode(corrupt, new byte[0], fixture.group());
            assertDecodeFailed(entry);
            assertNull(entry.timerTimestamp(), "malformed timer must not appear in time filters");
            assertNull(entry.key());
        }
    }

    @Test void timerRejectsNonEmptyValueAndWrongKeyGroup() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_fixture", "key",
                StringSerializer.INSTANCE, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 99L);
        assertDecodeFailed(fixture.decode(fixture.key(), new byte[]{0}, fixture.group()));
        assertDecodeFailed(fixture.decode(fixture.key(), new byte[0], fixture.group() + 1));
    }

    @Test void timerRejectsCorruptStringLengthBeforeCallingDeserializer() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_fixture", "key",
                StringSerializer.INSTANCE, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 99L);
        byte[] corrupt = Arrays.copyOf(fixture.key(), fixture.prefix() + Long.BYTES + 5);
        Arrays.fill(corrupt, fixture.prefix() + Long.BYTES, corrupt.length, (byte) 0xff);
        assertDecodeFailed(fixture.decode(corrupt, new byte[0], fixture.group()));
    }

    @Test void oversizedTimerRecordStaysRawWithoutDecodingPartialBytes() throws Exception {
        TimerFixture fixture = timerFixture("_timer_state/event_fixture", "key",
                StringSerializer.INSTANCE, VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 99L);
        PreviewReport.StateEntry entry = PreviewDecoder.entry(fixture.name(), fixture.schema(),
                new PreviewDecoder.RawBytes(fixture.key(), PreviewDecoder.MAX_RECORD_BYTES + 1),
                new PreviewDecoder.RawBytes(new byte[0], 0), fixture.group(), fixture.prefix());
        assertTrue(entry.decodeStatus().startsWith("RAW: record exceeds"), entry::decodeStatus);
        assertNull(entry.timerTimestamp());
    }

    @Test void unknownNestedTimerSnapshotIsNeverRestoredOrDecoded() throws Exception {
        List<TimerFixture> fixtures = List.of(
                timerFixture("_timer_state/event_fixture", "key", new UnsupportedStringSerializer(),
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, 99L),
                timerFixture("_timer_state/event_fixture", "key", StringSerializer.INSTANCE,
                        "custom-namespace", new UnsupportedStringSerializer(), 99L));
        for (TimerFixture fixture : fixtures) {
            assertNull(fixture.schema().valueSerializer(), "outer timer must not restore unknown nested serializers");
            PreviewReport.StateEntry entry = fixture.decode();
            assertTrue(entry.decodeStatus().startsWith("RAW:"), entry::decodeStatus);
            assertNull(entry.timerTimestamp());
            assertNull(entry.key());
            assertNull(entry.namespace());
        }
    }

    private static <K, N> TimerFixture timerFixture(String name, K key, TypeSerializer<K> keySerializer,
                                                    N namespace, TypeSerializer<N> namespaceSerializer,
                                                    long timestamp) throws Exception {
        TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
        StateMetaInfoSnapshot metadata = new RegisteredPriorityQueueStateBackendMetaInfo<>(name, timerSerializer).snapshot();
        DataOutputSerializer metadataBytes = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(keySerializer, List.of(metadata), false).write(metadataBytes);
        KeyedBackendSerializationProxy<K> restored = new KeyedBackendSerializationProxy<>(PreviewDecoderTest.class.getClassLoader());
        restored.read(new DataInputDeserializer(metadataBytes.getCopyOfBuffer()));
        PreviewDecoder.Schema schema = PreviewDecoder.schemas(restored).get(0);
        int group = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
        int prefix = CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(MAX_PARALLELISM);
        DataOutputSerializer bytes = new DataOutputSerializer(64);
        CompositeKeySerializationUtils.writeKeyGroup(group, prefix, bytes);
        timerSerializer.serialize(new TimerHeapInternalTimer<>(timestamp, key, namespace), bytes);
        return new TimerFixture(name, schema, bytes.getCopyOfBuffer(), group, prefix);
    }

    private static Fixture scalarFixture() throws Exception {
        return fixture("fixture-key", StringSerializer.INSTANCE,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                37L, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
    }

    private static <K, N, MK, V> Fixture fixture(K businessKey, TypeSerializer<K> keySerializer,
                                               N namespace, TypeSerializer<N> namespaceSerializer,
                                               MK mapKey, TypeSerializer<MK> mapKeySerializer,
                                               TypeSerializer<V> valueSerializer) throws Exception {
        MapSerializer<MK, V> mapSerializer = new MapSerializer<>(mapKeySerializer, valueSerializer);
        StateMetaInfoSnapshot metadata = new StateMetaInfoSnapshot(STATE_NAME,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                Map.of(KEYED_STATE_TYPE.toString(), "MAP"),
                Map.of(NAMESPACE_SERIALIZER.toString(), namespaceSerializer.snapshotConfiguration(),
                        VALUE_SERIALIZER.toString(), mapSerializer.snapshotConfiguration()));
        DataOutputSerializer metadataBytes = new DataOutputSerializer(256);
        new KeyedBackendSerializationProxy<>(keySerializer, List.of(metadata), false).write(metadataBytes);
        KeyedBackendSerializationProxy<K> restored = new KeyedBackendSerializationProxy<>(PreviewDecoderTest.class.getClassLoader());
        restored.read(new DataInputDeserializer(metadataBytes.getCopyOfBuffer()));
        assertInstanceOf(MapSerializerSnapshot.class,
                restored.getStateMetaInfoSnapshots().get(0).getTypeSerializerSnapshot(VALUE_SERIALIZER));
        PreviewDecoder.Schema schema = PreviewDecoder.schemas(restored).get(0);
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(businessKey, MAX_PARALLELISM);
        int prefix = CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(MAX_PARALLELISM);
        SerializedCompositeKeyBuilder<K> keyBuilder = new SerializedCompositeKeyBuilder<>(keySerializer, prefix, 64);
        keyBuilder.setKeyAndKeyGroup(businessKey, keyGroup);
        byte[] key = keyBuilder.buildCompositeKeyNamesSpaceUserKey(namespace, namespaceSerializer, mapKey, mapKeySerializer);
        return new Fixture(schema, key, keyGroup, prefix);
    }

    /** A single native MAP entry has its own null marker; it is not a serialized whole Map. */
    private static <T> byte[] mapValue(T value, TypeSerializer<T> serializer) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeBoolean(value == null);
        if (value != null) serializer.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static byte[] appendByte(byte[] original) {
        byte[] result = Arrays.copyOf(original, original.length + 1);
        result[original.length] = 0x5a;
        return result;
    }

    private static void assertDecoded(PreviewReport.StateEntry entry) {
        assertTrue(entry.decodeStatus().startsWith("DECODED:"), entry::decodeStatus);
    }

    private static void assertDecodeFailed(PreviewReport.StateEntry entry) {
        assertTrue(entry.decodeStatus().contains("DECODE_FAILED"), entry::decodeStatus);
        assertFalse(entry.decodeStatus().startsWith("DECODED:"), entry::decodeStatus);
    }

    private record Fixture(PreviewDecoder.Schema schema, byte[] key, int keyGroup, int prefix) {
        PreviewReport.StateEntry decode(byte[] value) {
            return decode(key, value);
        }

        PreviewReport.StateEntry decode(byte[] suppliedKey, byte[] value) {
            return PreviewDecoder.entry(STATE_NAME, schema,
                    new PreviewDecoder.RawBytes(suppliedKey, suppliedKey.length),
                    new PreviewDecoder.RawBytes(value, value.length), keyGroup, prefix);
        }
    }

    private record TimerFixture(String name, PreviewDecoder.Schema schema, byte[] key, int group, int prefix) {
        PreviewReport.StateEntry decode() { return decode(key, new byte[0], group); }
        PreviewReport.StateEntry decode(byte[] suppliedKey, byte[] value, int expectedGroup) {
            return PreviewDecoder.entry(name, schema,
                    new PreviewDecoder.RawBytes(suppliedKey, suppliedKey.length),
                    new PreviewDecoder.RawBytes(value, value.length), expectedGroup, prefix);
        }
    }

    public static final class UnsupportedStringSerializer extends TypeSerializerSingleton<String> {
        public boolean isImmutableType() { return true; }
        public String createInstance() { return ""; }
        public String copy(String value) { return value; }
        public String copy(String value, String reuse) { return value; }
        public int getLength() { return -1; }
        public void serialize(String value, DataOutputView output) throws IOException { StringSerializer.INSTANCE.serialize(value, output); }
        public String deserialize(DataInputView input) throws IOException { return StringSerializer.INSTANCE.deserialize(input); }
        public String deserialize(String reuse, DataInputView input) throws IOException { return deserialize(input); }
        public void copy(DataInputView input, DataOutputView output) throws IOException { StringSerializer.INSTANCE.copy(input, output); }
        public TypeSerializerSnapshot<String> snapshotConfiguration() { return new UnsupportedStringSnapshot(); }
    }

    public static final class UnsupportedStringSnapshot extends SimpleTypeSerializerSnapshot<String> {
        public UnsupportedStringSnapshot() {
            super(() -> { throw new AssertionError("viewer must not restore unknown serializers"); });
        }
    }

    public static final class SamplePojo {
        public BigDecimal decimal;
        public int count;
        public SamplePojo() {}
    }
}
