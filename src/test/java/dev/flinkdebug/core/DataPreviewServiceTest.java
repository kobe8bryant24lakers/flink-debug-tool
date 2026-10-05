package dev.flinkdebug.core;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.setMetaDataFollowsFlagInKey;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;
import static org.junit.jupiter.api.Assertions.*;

class DataPreviewServiceTest {
    @TempDir Path temporary;
    private final OperatorID operatorId = new OperatorID(11, 22);
    private static final int MAX_PARALLELISM = 128;

    @Test void readsActualCanonicalBytesAndLeavesAllDownloadedFilesUnchanged() throws Exception {
        Path download = canonical(false, "VALUE", List.of(new Value(7, string("order-7")), new Value(8, string("order-8"))));
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 10);
            assertEquals(2, preview.entries().size());
            assertEquals("VALUE", preview.schemas().get(0).type());
            assertEquals(StringSerializer.class.getName(), preview.schemas().get(0).valueSerializer());
            assertTrue(preview.entries().stream().allMatch(entry -> entry.decodeStatus().startsWith("DECODED:")));
            assertEquals(List.of("order-7", "order-8"), preview.entries().stream().map(PreviewReport.StateEntry::value).sorted().toList());
            assertEquals(List.of("7", "8"), preview.entries().stream().map(PreviewReport.StateEntry::key).sorted().toList());
            assertEquals(HexFormat.of().formatHex(string("order-7")), preview.entries().stream()
                    .filter(entry -> entry.value().equals("order-7")).findFirst().orElseThrow().valueHex());
            assertFalse(preview.truncated());
        }
        assertEquals(before, fingerprints(download));
    }

    @Test void readsSnappyCompressedCanonicalGroupsAndStopsAtTheRequestedLimit() throws Exception {
        Path download = canonical(true, "VALUE", List.of(new Value(7, string("one")), new Value(8, string("two")), new Value(9, string("three"))));
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 2);
            assertEquals(2, preview.entries().size());
            assertTrue(preview.truncated());
            assertTrue(preview.entries().stream().allMatch(entry -> entry.decodeStatus().startsWith("DECODED:")));
        }
    }

    @Test void preservesListStateAsRawBytesInsteadOfClaimingBusinessObjects() throws Exception {
        byte[] listBytes = new ListDelimitedSerializer().serializeList(List.of("a", "b"), StringSerializer.INSTANCE);
        Path download = canonical(false, "LIST", List.of(new Value(7, listBytes)));
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 10);
            PreviewReport.StateEntry entry = preview.entries().get(0);
            assertNull(entry.value());
            assertEquals(HexFormat.of().formatHex(listBytes), entry.valueHex());
            assertTrue(entry.decodeStatus().startsWith("RAW: LIST"));
        }
    }

    @Test void rejectsCorruptScalarLengthWithoutAllocatingItsClaimedStringSize() throws Exception {
        Path download = canonical(false, "VALUE", List.of(new Value(7, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f})));
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 10);
            assertNull(preview.entries().get(0).value());
            assertTrue(preview.entries().get(0).decodeStatus().contains("DECODE_FAILED"));
        }
    }

    @Test void oversizedCanonicalRecordsStayBoundedAndKeepTheirRealByteLength() throws Exception {
        byte[] value = new byte[PreviewDecoder.MAX_RECORD_BYTES + 17];
        Path download = canonical(false, "VALUE", List.of(new Value(7, value)));
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 10);
            assertEquals(1, preview.entries().size());
            assertTrue(preview.entries().get(0).decodeStatus().contains("decoding limit"));
            assertTrue(preview.entries().get(0).valueHex().contains(value.length + " bytes; prefix shown"));
            assertTrue(preview.entries().get(0).valueHex().length() < 4200);
        }
    }

    @Test void opensRealIncrementalRocksDbFromPrivateCopiesAndPreservesSourceHashesAndTimestamps() throws Exception {
        Path download = incremental();
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 20);
            assertEquals(3, preview.entries().size(), () -> String.join("\n", preview.warnings()));
            List<PreviewReport.StateEntry> values = preview.entries().stream().filter(entry -> entry.stateName().equals("orders")).toList();
            assertEquals(2, values.size());
            assertTrue(values.stream().allMatch(entry -> entry.decodeStatus().startsWith("DECODED:")));
            assertEquals(List.of("native-7", "native-8"), values.stream().map(PreviewReport.StateEntry::value).sorted().toList());
            PreviewReport.StateEntry list = preview.entries().stream().filter(entry -> entry.stateName().equals("history")).findFirst().orElseThrow();
            assertNull(list.value());
            assertTrue(list.decodeStatus().startsWith("RAW: LIST"));
            assertFalse(preview.truncated());
            assertTrue(preview.warnings().stream().noneMatch(warning -> warning.contains("Could not sample")));
        }
        assertEquals(before, fingerprints(download));
    }

    @Test void nativeSamplingUsesTheSameGlobalLimitAcrossColumnFamilies() throws Exception {
        Path download = incremental();
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 1);
            assertEquals(1, preview.entries().size());
            assertTrue(preview.truncated());
        }
    }

    @Test void nativeHandleIntersectionExcludesOtherKeyGroupsPresentInTheSameSstFiles() throws Exception {
        assertNotEquals(keyGroup(7), keyGroup(8));
        Path download = incremental(new KeyGroupRange(keyGroup(7), keyGroup(7)));
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            PreviewReport preview = new DataPreviewService().preview(session, operatorId.toHexString(), 0, 20);
            assertEquals(2, preview.entries().size(), () -> String.join("\n", preview.warnings()));
            assertTrue(preview.entries().stream().allMatch(entry -> entry.keyGroup() == keyGroup(7)));
            assertEquals(List.of("native-7"), preview.entries().stream().filter(entry -> entry.value() != null)
                    .map(PreviewReport.StateEntry::value).toList());
            assertFalse(preview.truncated());
        }
    }

    @Test void streamApiReadsNativeValueStateFromLocalDownloadedFilesWithoutChangingSources() throws Exception {
        Path download = incremental();
        assertNativeStreamQuery(download, List.of());
    }

    @Test void streamApiReadsNativeValueStateAfterExplicitS3UriToLocalMappingWithoutChangingSources() throws Exception {
        Path download = incremental(new KeyGroupRange(0, MAX_PARALLELISM - 1), true);
        assertNativeStreamQuery(download, List.of(new PathMapping("s3://offline-fixture/snapshot/", download)));
    }

    private void assertNativeStreamQuery(Path download, List<PathMapping> mappings) throws Exception {
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = new SnapshotInspector().open(download, mappings, List.of())) {
            LocalStreamQueryService.QueryReport query = new LocalStreamQueryService().query(session,
                    new LocalStreamQueryService.QuerySpec(operatorId.toHexString(), "orders", LocalStreamQueryService.Kind.VALUE,
                            LocalStreamQueryService.ScalarType.INT, LocalStreamQueryService.ScalarType.STRING,
                            LocalStreamQueryService.ScalarType.STRING, 20));
            assertEquals(2, query.rows().size());
            assertEquals(List.of("7", "8"), query.rows().stream().map(LocalStreamQueryService.QueryRow::key).sorted().toList());
            assertEquals(List.of("\"native-7\"", "\"native-8\""), query.rows().stream()
                    .map(LocalStreamQueryService.QueryRow::value).sorted().toList());
            assertFalse(query.truncated());
        }
        assertEquals(before, fingerprints(download));
    }

    @Test void interruptionCancelsBeforeOpeningAnyStateFile() throws Exception {
        Path download = canonical(false, "VALUE", List.of(new Value(7, string("one"))));
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = new SnapshotInspector().open(download, List.of(), List.of())) {
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class, () -> new DataPreviewService().preview(session, operatorId.toHexString(), 0, 10));
            } finally {
                Thread.interrupted();
            }
        }
        assertEquals(before, fingerprints(download));
    }

    private Path canonical(boolean compressed, String stateType, List<Value> entries) throws Exception {
        Path download = Files.createDirectory(temporary.resolve("download"));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        StateMetaInfoSnapshot state = schema("orders", stateType);
        new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, List.of(state), compressed)
                .write(new DataOutputViewStreamWrapper(stream));
        long[] offsets = new long[MAX_PARALLELISM];
        Map<Integer, List<Value>> groups = new TreeMap<>();
        for (Value entry : entries) groups.computeIfAbsent(keyGroup(entry.key), ignored -> new ArrayList<>()).add(entry);
        for (var group : groups.entrySet()) {
            offsets[group.getKey()] = stream.size();
            OutputStream output = compressed ? SnappyStreamCompressionDecorator.INSTANCE.decorateWithCompression(stream) : stream;
            try (DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(output)) {
                view.writeShort(0);
                for (int index = 0; index < group.getValue().size(); index++) {
                    Value entry = group.getValue().get(index);
                    byte[] key = key(entry.key);
                    if (index == group.getValue().size() - 1) setMetaDataFollowsFlagInKey(key);
                    BytePrimitiveArraySerializer.INSTANCE.serialize(key, view);
                    BytePrimitiveArraySerializer.INSTANCE.serialize(entry.value, view);
                }
                view.writeShort(END_OF_KEY_GROUP_MARK);
            }
        }
        Path stateFile = download.resolve("canonical-state.bin");
        Files.write(stateFile, stream.toByteArray());
        KeyGroupsSavepointStateHandle handle = new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(new KeyGroupRange(0, MAX_PARALLELISM - 1), offsets), fileHandle(stateFile));
        writeMetadata(download, handle);
        return download;
    }

    private Path incremental() throws Exception {
        return incremental(new KeyGroupRange(0, MAX_PARALLELISM - 1));
    }

    private Path incremental(KeyGroupRange keyGroupRange) throws Exception {
        return incremental(keyGroupRange, false);
    }

    private Path incremental(KeyGroupRange keyGroupRange, boolean remoteReferences) throws Exception {
        RocksDB.loadLibrary();
        Path download = Files.createDirectory(temporary.resolve("download"));
        Path checkpointPath = download.resolve("rocks");
        Path databasePath = temporary.resolve("source-db");
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions();
             ColumnFamilyOptions ordersOptions = new ColumnFamilyOptions();
             ColumnFamilyOptions historyOptions = new ColumnFamilyOptions();
             WriteOptions writes = new WriteOptions().setDisableWAL(true);
             FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
            RocksDB db = RocksDB.open(dbOptions, databasePath.toString(), List.of(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions),
                    new ColumnFamilyDescriptor("orders".getBytes(StandardCharsets.UTF_8), ordersOptions),
                    new ColumnFamilyDescriptor("history".getBytes(StandardCharsets.UTF_8), historyOptions)), handles);
            try {
                db.put(handles.get(1), writes, key(7), string("native-7"));
                db.put(handles.get(1), writes, key(8), string("native-8"));
                db.put(handles.get(2), writes, key(7), new ListDelimitedSerializer().serializeList(List.of("a", "b"), StringSerializer.INSTANCE));
                db.flush(flush, handles);
                try (Checkpoint checkpoint = Checkpoint.create(db)) {
                    checkpoint.createCheckpoint(checkpointPath.toString());
                }
            } finally {
                handles.forEach(ColumnFamilyHandle::close);
                db.close();
            }
        }
        List<HandleAndLocalPath> shared = new ArrayList<>();
        List<HandleAndLocalPath> privateFiles = new ArrayList<>();
        try (var paths = Files.list(checkpointPath)) {
            for (Path file : paths.sorted().toList()) {
                if (!Files.isRegularFile(file)) continue;
                HandleAndLocalPath reference = HandleAndLocalPath.of(reference(file, download, remoteReferences), file.getFileName().toString());
                (file.getFileName().toString().endsWith(".sst") ? shared : privateFiles).add(reference);
            }
        }
        Path meta = download.resolve("backend-meta.bin");
        try (OutputStream output = Files.newOutputStream(meta)) {
            new KeyedBackendSerializationProxy<>(IntSerializer.INSTANCE, List.of(schema("orders", "VALUE"), schema("history", "LIST")), false)
                    .write(new DataOutputViewStreamWrapper(output));
        }
        KeyedStateHandle handle = new IncrementalRemoteKeyedStateHandle(UUID.randomUUID(),
                keyGroupRange, 42L, shared, privateFiles, reference(meta, download, remoteReferences));
        writeMetadata(download, handle);
        return download;
    }

    private StateMetaInfoSnapshot schema(String name, String type) {
        TypeSerializerSnapshot<?> value = type.equals("LIST")
                ? new ListSerializer<>(StringSerializer.INSTANCE).snapshotConfiguration()
                : StringSerializer.INSTANCE.snapshotConfiguration();
        return new StateMetaInfoSnapshot(name, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                Map.of(KEYED_STATE_TYPE.toString(), type),
                Map.of(NAMESPACE_SERIALIZER.toString(), VoidNamespaceSerializer.INSTANCE.snapshotConfiguration(),
                        VALUE_SERIALIZER.toString(), value));
    }

    private void writeMetadata(Path download, KeyedStateHandle handle) throws Exception {
        OperatorState operator = new OperatorState(operatorId, 1, MAX_PARALLELISM);
        operator.putState(0, OperatorSubtaskState.builder().setManagedKeyedState(handle).build());
        try (OutputStream output = Files.newOutputStream(download.resolve("_metadata"))) {
            Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(42L, List.of(operator), List.of()), output);
        }
    }

    private static FileStateHandle fileHandle(Path file) throws Exception {
        return new FileStateHandle(new org.apache.flink.core.fs.Path(file.toUri()), Files.size(file));
    }

    private static FileStateHandle reference(Path file, Path download, boolean remote) throws Exception {
        if (!remote) return fileHandle(file);
        return new FileStateHandle(new org.apache.flink.core.fs.Path("s3://offline-fixture/snapshot/"
                + download.relativize(file).toString().replace('\\', '/')), Files.size(file));
    }

    private static int keyGroup(int key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM); }

    private static byte[] key(int key) throws Exception {
        SerializedCompositeKeyBuilder<Integer> builder = new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, 1, 32);
        builder.setKeyAndKeyGroup(key, keyGroup(key));
        return builder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
    }

    private static byte[] string(String value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static Map<String, String> fingerprints(Path directory) throws Exception {
        Map<String, String> result = new LinkedHashMap<>();
        try (var paths = Files.walk(directory)) {
            for (Path file : paths.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList()) {
                result.put(directory.relativize(file).toString(), HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)))
                        + ":" + Files.size(file) + ":" + Files.getLastModifiedTime(file));
            }
        }
        return result;
    }

    private record Value(int key, byte[] value) {}
}
