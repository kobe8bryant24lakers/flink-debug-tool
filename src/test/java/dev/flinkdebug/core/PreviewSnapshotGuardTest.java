package dev.flinkdebug.core;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.util.LinkedOptionalMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises schema restoration without reading records or starting a Flink backend. */
class PreviewSnapshotGuardTest {
    @Test void rejectsUnknownMapValueBeforeExecutingItsRestore() {
        CountingSnapshot unknown = new CountingSnapshot();
        assertRejected(mapSnapshot(longSnapshot(), unknown));
        assertEquals(0, unknown.restoreCalls);
    }

    @Test void rejectsUnknownMapKeyBeforeExecutingItsRestore() {
        CountingSnapshot unknown = new CountingSnapshot();
        assertRejected(mapSnapshot(unknown, longSnapshot()));
        assertEquals(0, unknown.restoreCalls);
    }

    @Test void rejectsUnknownPojoFieldBeforeExecutingItsRestore() throws Exception {
        CountingSnapshot unknown = new CountingSnapshot();
        TypeSerializerSnapshot<?> pojo = pojoSnapshot();
        LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fields = pojoRegistry(pojo, "fieldSerializerSnapshots");
        Field field = GuardPojo.class.getField("value");
        fields.put(field.getName(), field, unknown);
        assertRejected(pojo);
        assertEquals(0, unknown.restoreCalls);
    }

    @Test void rejectsUnknownRegisteredPojoSubclassBeforeExecutingItsRestore() throws Exception {
        assertUnknownPojoSubclassRejected("registeredSubclassSerializerSnapshots");
    }

    @Test void rejectsUnknownUnregisteredPojoSubclassBeforeExecutingItsRestore() throws Exception {
        assertUnknownPojoSubclassRejected("nonRegisteredSubclassSerializerSnapshots");
    }

    @Test void rejectsUnknownTtlPayloadBeforeExecutingItsRestore() {
        CountingSnapshot unknown = new CountingSnapshot();
        TtlStateFactory.TtlSerializerSnapshot<Long> ttl = new TtlStateFactory.TtlSerializerSnapshot<>();
        CompositeTypeSerializerUtil.setNestedSerializersSnapshots(ttl,
                new TypeSerializerSnapshot<?>[]{longSnapshot(), unknown});
        assertRejected(ttl);
        assertEquals(0, unknown.restoreCalls);
    }

    @Test void permitsDepthSixteenAndRejectsDepthSeventeenWithoutStackOverflow() {
        assertNotNull(assertDoesNotThrow(() -> schema(mapChain(16))).valueSerializer());
        assertRejected(mapChain(17));
    }

    @Test void rejectsVeryDeepKnownSnapshotBeforeRecursiveRestorationOverflows() {
        // Construct iteratively so the test fixture itself does not recurse.
        assertRejected(mapChain(10_000));
    }

    @Test void rejectsSelfReferencingKnownSnapshotWithoutStackOverflow() {
        MapSerializerSnapshot<Object, Object> cycle = new MapSerializerSnapshot<>();
        CompositeTypeSerializerUtil.setNestedSerializersSnapshots(cycle,
                new TypeSerializerSnapshot<?>[]{longSnapshot(), cycle});
        assertRejected(cycle);
    }

    @Test void rejectsExcessiveTotalSnapshotsEvenWhenDepthIsWithinLimit() {
        // A depth-14 binary tree has 32,767 known snapshots; no cycle or unknown leaf.
        assertRejected(knownMapTree(14));
    }

    @Test void rejectsExcessivePojoFieldsBeforeRestoration() throws Exception {
        assertOversizedPojoRegistryRejected("fieldSerializerSnapshots", GuardPojo.class.getField("value"));
    }

    @Test void rejectsExcessiveRegisteredPojoSubclassesBeforeRestoration() throws Exception {
        assertOversizedPojoRegistryRejected("registeredSubclassSerializerSnapshots", GuardPojoSubclass.class);
    }

    @Test void rejectsExcessiveUnregisteredPojoSubclassesBeforeRestoration() throws Exception {
        assertOversizedPojoRegistryRejected("nonRegisteredSubclassSerializerSnapshots", GuardPojoSubclass.class);
    }

    @Test void restoresStandardNestedMapSnapshot() {
        TypeSerializerSnapshot<?> nested = mapSnapshot(longSnapshot(),
                mapSnapshot(longSnapshot(), longSnapshot()));
        MapSerializer<?, ?> restored = assertInstanceOf(MapSerializer.class, schema(nested).valueSerializer());
        assertInstanceOf(LongSerializer.class, restored.getKeySerializer());
        MapSerializer<?, ?> restoredValue = assertInstanceOf(MapSerializer.class, restored.getValueSerializer());
        assertInstanceOf(LongSerializer.class, restoredValue.getKeySerializer());
        assertInstanceOf(LongSerializer.class, restoredValue.getValueSerializer());
    }

    @Test void restoresStandardPojoSnapshot() throws Exception {
        assertInstanceOf(PojoSerializer.class, schema(pojoSnapshot()).valueSerializer());
    }

    @Test void restoresStandardTtlSnapshotNestedInMap() {
        TypeSerializerSnapshot<?> ttl = new TtlStateFactory.TtlSerializer<>(
                LongSerializer.INSTANCE, LongSerializer.INSTANCE).snapshotConfiguration();
        MapSerializer<?, ?> restored = assertInstanceOf(MapSerializer.class,
                schema(mapSnapshot(longSnapshot(), ttl)).valueSerializer());
        assertInstanceOf(TtlStateFactory.TtlSerializer.class, restored.getValueSerializer());
    }

    private static void assertUnknownPojoSubclassRejected(String registryName) throws Exception {
        CountingSnapshot unknown = new CountingSnapshot();
        TypeSerializerSnapshot<?> pojo = pojoSnapshot();
        LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> subclasses = pojoRegistry(pojo, registryName);
        subclasses.put(GuardPojoSubclass.class.getName(), GuardPojoSubclass.class, unknown);
        assertRejected(pojo);
        assertEquals(0, unknown.restoreCalls);
    }

    private static void assertOversizedPojoRegistryRejected(String registryName, Object key) throws Exception {
        TypeSerializerSnapshot<?> pojo = pojoSnapshot();
        LinkedOptionalMap<Object, TypeSerializerSnapshot<?>> entries = pojoRegistry(pojo, registryName);
        // Different saved names exercise the snapshot registry's own entry count without
        // requiring hundreds of generated user classes or fields in this test fixture.
        for (int i = 0; i < 257; i++) entries.put("saved-entry-" + i, key, longSnapshot());
        assertTrue(entries.size() > 256);
        assertRejected(pojo);
    }

    private static void assertRejected(TypeSerializerSnapshot<?> snapshot) {
        PreviewDecoder.Schema result = assertDoesNotThrow(() -> schema(snapshot));
        assertNull(result.valueSerializer(), "Unsupported snapshot trees must remain raw");
        assertTrue(result.report().valueSerializer().contains("snapshot; raw preview"));
        assertInstanceOf(IntSerializer.class, result.keySerializer());
        assertInstanceOf(VoidNamespaceSerializer.class, result.namespaceSerializer());
    }

    private static PreviewDecoder.Schema schema(TypeSerializerSnapshot<?> value) {
        StateMetaInfoSnapshot state = new StateMetaInfoSnapshot("guard-test",
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                Map.of(KEYED_STATE_TYPE.toString(), "MAP"),
                Map.of(NAMESPACE_SERIALIZER.toString(), VoidNamespaceSerializer.INSTANCE.snapshotConfiguration(),
                        VALUE_SERIALIZER.toString(), value));
        KeyedBackendSerializationProxy<Integer> proxy = new KeyedBackendSerializationProxy<>(
                IntSerializer.INSTANCE, List.of(state), false);
        return PreviewDecoder.schemas(proxy).get(0);
    }

    private static TypeSerializerSnapshot<?> longSnapshot() {
        return LongSerializer.INSTANCE.snapshotConfiguration();
    }

    private static MapSerializerSnapshot<Object, Object> mapSnapshot(
            TypeSerializerSnapshot<?> key, TypeSerializerSnapshot<?> value) {
        MapSerializerSnapshot<Object, Object> map = new MapSerializerSnapshot<>();
        CompositeTypeSerializerUtil.setNestedSerializersSnapshots(map,
                new TypeSerializerSnapshot<?>[]{key, value});
        return map;
    }

    private static TypeSerializerSnapshot<?> mapChain(int layers) {
        TypeSerializerSnapshot<?> snapshot = longSnapshot();
        for (int i = 0; i < layers; i++) snapshot = mapSnapshot(longSnapshot(), snapshot);
        return snapshot;
    }

    private static TypeSerializerSnapshot<?> knownMapTree(int depth) {
        return depth == 0 ? longSnapshot() : mapSnapshot(knownMapTree(depth - 1), knownMapTree(depth - 1));
    }

    private static TypeSerializerSnapshot<?> pojoSnapshot() throws Exception {
        return new PojoSerializer<>(GuardPojo.class,
                new TypeSerializer<?>[]{LongSerializer.INSTANCE},
                new Field[]{GuardPojo.class.getField("value")},
                new SerializerConfigImpl()).snapshotConfiguration();
    }

    /** Flink 1.20/2.2 keep POJO nested snapshots behind this internal snapshot-data field. */
    @SuppressWarnings("unchecked")
    private static <K> LinkedOptionalMap<K, TypeSerializerSnapshot<?>> pojoRegistry(
            TypeSerializerSnapshot<?> snapshot, String name) throws Exception {
        Field snapshotData = snapshot.getClass().getDeclaredField("snapshotData");
        snapshotData.setAccessible(true);
        Object data = snapshotData.get(snapshot);
        Field registry = data.getClass().getDeclaredField(name);
        registry.setAccessible(true);
        return (LinkedOptionalMap<K, TypeSerializerSnapshot<?>>) registry.get(data);
    }

    public static class GuardPojo {
        public Long value;
        public GuardPojo() {}
    }

    public static final class GuardPojoSubclass extends GuardPojo {
        public Long extra;
        public GuardPojoSubclass() {}
    }

    public static final class CountingSnapshot implements TypeSerializerSnapshot<Long> {
        private int restoreCalls;
        @Override public int getCurrentVersion() { return 1; }
        @Override public void writeSnapshot(DataOutputView out) {}
        @Override public void readSnapshot(int version, DataInputView in, ClassLoader loader) {}
        @Override public TypeSerializer<Long> restoreSerializer() {
            restoreCalls++;
            return LongSerializer.INSTANCE;
        }
        @Override public TypeSerializerSchemaCompatibility<Long> resolveSchemaCompatibility(
                TypeSerializerSnapshot<Long> previous) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
