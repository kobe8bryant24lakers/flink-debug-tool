package dev.flinkdebug.core;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
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
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
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
import java.util.Arrays;
import java.util.HashSet;
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

/** Real Flink metadata and serialized records, with actual disposable RocksDB checkpoints. */
class DataPreviewPagingTest {
    @TempDir Path temporary;
    private static final int MAX_PARALLELISM = 128;
    private static final OperatorID OPERATOR = new OperatorID(101, 202);
    private static final TimerSerializer<String, VoidNamespace> TIMERS =
            new TimerSerializer<>(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
    private final DataPreviewService reader = new DataPreviewService();

    @Test void allCanonicalPagesEnumerateMoreThanOneThousandStringsWithoutLossOrDuplicates() throws Exception {
        List<Row> records = scalarRows("orders", 1505);
        Path download = Files.createDirectory(temporary.resolve("canonical"));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")), records)));
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = open(download)) {
            PreviewReport first = preview(session, page(1000, 0));
            PreviewReport last = preview(session, page(1000, first.page().nextOffset()));
            assertEquals(1000, first.entries().size());
            assertTrue(first.page().hasMore());
            assertEquals(1001, first.page().scanned());
            assertTrue(first.page().complete());
            assertEquals(505, last.entries().size());
            assertEquals(1505, last.page().nextOffset());
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete(), () -> String.join("\n", last.warnings()));
            assertEquals(1505, last.page().scanned()); // Offset pages rescan the immutable snapshot.
            HashSet<String> keys = new HashSet<>();
            first.entries().forEach(row -> assertTrue(keys.add(row.key())));
            last.entries().forEach(row -> assertTrue(keys.add(row.key())));
            assertEquals(1505, keys.size());
            assertTrue(keys.contains("customer-1504"));
            assertTrue(first.entries().stream().allMatch(row -> row.decodeStatus().startsWith("DECODED:")));
        }
        assertEquals(before, fingerprints(download));
    }

    @Test void aFinalPageWithExactlyThePageSizeIsExhaustedWithoutAnExtraEmptyRequest() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("exact"));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")), scalarRows("orders", 1000))));
        try (SnapshotSession session = open(download)) {
            PreviewReport result = preview(session, page(1000, 0));
            assertEquals(1000, result.entries().size());
            assertFalse(result.page().hasMore());
            assertFalse(result.truncated());
            assertTrue(result.page().complete());
            assertEquals(1000, result.page().scanned());
            PreviewReport beyond = preview(session, page(1000, 1000));
            assertTrue(beyond.entries().isEmpty());
            assertFalse(beyond.page().hasMore());
            assertTrue(beyond.page().complete());
            assertEquals(1000, beyond.page().nextOffset());
        }
    }

    @Test void canonicalStateSelectionSkipsDecodingOtherStatesAndKeepsTheWholeSchemaCatalog() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("selected"));
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < 1105; index++) {
            rows.add(scalar("bulk", "bulk-" + index, new byte[] {(byte) 0xff}));
        }
        rows.add(scalar("target", "wanted", string("found-after-large-state")));
        writeMetadata(download, List.of(canonical(download, "state.bin",
                List.of(valueSchema("bulk"), valueSchema("target")), rows)));
        try (SnapshotSession session = open(download)) {
            PreviewReport selected = preview(session, request(10, 0, "target", null, null, null, null, null));
            assertEquals(List.of("found-after-large-state"), selected.entries().stream().map(PreviewReport.StateEntry::value).toList());
            assertEquals(2, selected.schemas().size());
            assertEquals(1, selected.page().scanned());
            assertTrue(selected.page().complete(), () -> String.join("\n", selected.warnings()));
            assertFalse(selected.page().hasMore());
        }
    }

    @Test void nativePagesEnumerateEveryStringBeyondTheOldGlobalSampleLimitAndKeepFilesUnchanged() throws Exception {
        Path download = incremental(List.of(valueSchema("bulk"), valueSchema("later")), concat(
                scalarRows("bulk", 1205), List.of(scalar("later", "late-key", string("late-value")))));
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = open(download)) {
            PreviewReport first = preview(session, page(1000, 0));
            PreviewReport last = preview(session, page(1000, first.page().nextOffset()));
            assertTrue(first.page().hasMore());
            assertEquals(2, first.schemas().size());
            assertEquals(206, last.entries().size());
            assertTrue(last.entries().stream().anyMatch(row -> "late-value".equals(row.value())));
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete(), () -> String.join("\n", last.warnings()));
            HashSet<String> keys = new HashSet<>();
            first.entries().forEach(row -> assertTrue(keys.add(row.key())));
            last.entries().forEach(row -> assertTrue(keys.add(row.key())));
            assertEquals(1206, keys.size());
        }
        assertEquals(before, fingerprints(download));
    }

    @Test void nativeMapKeyAndValueConditionsScanPastLargeColumnFamiliesAndPageOnlyMatchingRows() throws Exception {
        List<Row> rows = concat(scalarRows("bulk", 1205), List.of(
                map("lookup", "account-a", "region-us", "approved-one"),
                map("lookup", "account-a", "region-eu", "approved-two"),
                map("lookup", "account-b", "region-us", "approved-three"),
                map("lookup", "account-a", "region-us-2", "rejected")));
        Path download = incremental(List.of(valueSchema("bulk"), mapSchema("lookup")), rows);
        try (SnapshotSession session = open(download)) {
            PreviewReport first = preview(session, request(1, 0, "lookup", "account-a", "approved", "region-", null, null));
            PreviewReport last = preview(session, request(1, first.page().nextOffset(), "lookup", "account-a", "approved", "region-", null, null));
            assertEquals(1, first.entries().size());
            assertTrue(first.page().hasMore());
            assertEquals(1, last.entries().size());
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete(), () -> String.join("\n", last.warnings()));
            assertEquals(new HashSet<>(List.of("approved-one", "approved-two")),
                    new HashSet<>(List.of(first.entries().get(0).value(), last.entries().get(0).value())));
            assertEquals(2, last.page().nextOffset());
            assertEquals(4, last.page().scanned());
            assertEquals(2, first.schemas().size());
            PreviewReport noMatch = preview(session, request(20, 0, null, null, null, "absent", null, null));
            assertTrue(noMatch.entries().isEmpty());
            assertFalse(noMatch.page().hasMore());
            assertTrue(noMatch.page().complete()); // Scalar rows legitimately cannot match a map-key condition.
            assertEquals(4, noMatch.page().scanned());
        }
    }

    @Test void timerTimestampBoundsAreInclusiveAndExcludeKnownNonTimerStatesWithoutClaimingGaps() throws Exception {
        String timerState = "_timer_state/service/event";
        Path download = incremental(List.of(valueSchema("orders"), mapSchema("lookup"), timerSchema(timerState)), List.of(
                scalar("orders", "a", string("ordinary-value")), map("lookup", "a", "m", "map-value"),
                timer(timerState, "a", -1), timer(timerState, "a", 0), timer(timerState, "a", 1), timer(timerState, "a", 2)));
        try (SnapshotSession session = open(download)) {
            PreviewReport first = preview(session, request(1, 0, null, null, null, null, 0L, 1L));
            PreviewReport last = preview(session, request(1, first.page().nextOffset(), null, null, null, null, 0L, 1L));
            assertTrue(first.page().hasMore(), () -> String.join("\n", first.warnings()));
            assertEquals(List.of(0L), first.entries().stream().map(PreviewReport.StateEntry::timerTimestamp).toList());
            assertEquals(List.of(1L), last.entries().stream().map(PreviewReport.StateEntry::timerTimestamp).toList());
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete(), () -> String.join("\n", last.warnings()));
            assertEquals(4, last.page().scanned());
            assertTrue(last.entries().stream().allMatch(row -> timerState.equals(row.stateName())));
            PreviewReport businessValues = preview(session, request(10, 0, null, null, "ordinary", null, null, null));
            assertEquals(List.of("ordinary-value"), businessValues.entries().stream().map(PreviewReport.StateEntry::value).toList());
            assertTrue(businessValues.page().complete()); // Timers have no business value and legitimately do not match.
        }
    }

    @Test void largeDecodedStringsContinueOnAnotherPageBeforeTheRetainedMemoryBudgetIsExceeded() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("page-memory"));
        String value = "v".repeat(800_000);
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < 24; index++) rows.add(scalar("orders", "large-" + index, string(value)));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")), rows)));
        try (SnapshotSession session = open(download)) {
            PreviewReport first = preview(session, page(1000, 0));
            assertTrue(first.page().hasMore());
            assertTrue(first.entries().size() > 0 && first.entries().size() < 24);
            assertTrue(first.warnings().stream().anyMatch(warning -> warning.contains("Page memory budget reached")));
            assertTrue(first.page().complete());
            PreviewReport last = preview(session, page(1000, first.page().nextOffset()));
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete());
            assertEquals(24, first.entries().size() + last.entries().size());
            assertEquals(24, last.page().nextOffset());
            HashSet<String> keys = new HashSet<>();
            first.entries().forEach(row -> { assertTrue(keys.add(row.key())); assertEquals(value, row.value()); });
            last.entries().forEach(row -> { assertTrue(keys.add(row.key())); assertEquals(value, row.value()); });
        }
    }

    @Test void conditionsUseTheFullDecodedStringRatherThanAClippedTablePreview() throws Exception {
        String longValue = "x".repeat(5000) + "needle-after-preview";
        Path download = Files.createDirectory(temporary.resolve("long-string"));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")),
                List.of(scalar("orders", "a", string(longValue))))));
        try (SnapshotSession session = open(download)) {
            PreviewReport result = preview(session, request(10, 0, "orders", null, "needle-after-preview", null, null, null));
            assertEquals(1, result.entries().size());
            assertEquals(longValue, result.entries().get(0).value());
            assertTrue(result.page().complete());
        }
    }

    @Test void undecodableOrOversizedFieldsAreExcludedFromFiltersAndReportIncompleteCoverage() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("raw"));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")), List.of(
                scalar("orders", "bad", new byte[] {(byte) 0xff}),
                scalar("orders", "huge", new byte[PreviewDecoder.MAX_RECORD_BYTES + 1])))));
        try (SnapshotSession session = open(download)) {
            PreviewReport raw = preview(session, page(10, 0));
            assertEquals(2, raw.entries().size());
            assertFalse(raw.page().complete());
            assertFalse(raw.page().hasMore());
            PreviewReport filtered = preview(session, request(10, 0, "orders", null, "wanted", null, null, null));
            assertTrue(filtered.entries().isEmpty());
            assertFalse(filtered.page().complete());
            assertTrue(filtered.warnings().stream().anyMatch(warning -> warning.contains("value filter could not be evaluated")));
        }
    }

    @Test void laterHandleSchemasRemainVisibleAndTheirRowsAreAccessibleOnFollowingPages() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("handles"));
        KeyedStateHandle firstHandle = canonical(download, "first.bin", List.of(valueSchema("first")),
                List.of(scalar("first", "a", string("one")), scalar("first", "b", string("two"))));
        KeyedStateHandle laterHandle = canonical(download, "later.bin", List.of(valueSchema("later")),
                List.of(scalar("later", "c", string("three"))));
        writeMetadata(download, List.of(firstHandle));
        try (SnapshotSession session = open(download)) {
            attachInMemoryHandles(session, List.of(firstHandle, laterHandle));
            PreviewReport first = preview(session, page(1, 0));
            assertEquals(2, first.schemas().size());
            assertTrue(first.page().hasMore());
            PreviewReport last = preview(session, page(2, 1));
            assertEquals(2, last.entries().size());
            assertTrue(last.entries().stream().anyMatch(row -> row.stateName().equals("later")));
            assertFalse(last.page().hasMore());
            assertTrue(last.page().complete());
        }
    }

    @Test void aMissingLaterHandlePreventsAFalseCompleteReportEvenIfTheFirstHandleFillsThePage() throws Exception {
        Path download = Files.createDirectory(temporary.resolve("missing"));
        KeyedStateHandle firstHandle = canonical(download, "first.bin", List.of(valueSchema("first")), scalarRows("first", 3));
        KeyedStateHandle laterHandle = canonical(download, "missing.bin", List.of(valueSchema("later")),
                List.of(scalar("later", "c", string("three"))));
        writeMetadata(download, List.of(firstHandle));
        Files.delete(download.resolve("missing.bin"));
        try (SnapshotSession session = open(download)) {
            attachInMemoryHandles(session, List.of(firstHandle, laterHandle));
            PreviewReport first = preview(session, page(1, 0));
            assertEquals(1, first.entries().size());
            assertTrue(first.page().hasMore());
            assertFalse(first.page().complete());
            assertTrue(first.warnings().stream().anyMatch(warning -> warning.contains("Could not read schema")));
        }
    }

    @Test void rejectsInvalidPageRequestsAndCancelsBeforeAnySourceFileRead() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> page(0, 0));
        assertThrows(IllegalArgumentException.class, () -> page(1001, 0));
        assertThrows(IllegalArgumentException.class, () -> page(10, -1));
        assertThrows(IllegalArgumentException.class, () -> request(10, 0, null, null, null, null, 2L, 1L));
        Path download = Files.createDirectory(temporary.resolve("cancel"));
        writeMetadata(download, List.of(canonical(download, "state.bin", List.of(valueSchema("orders")), scalarRows("orders", 2))));
        Map<String, String> before = fingerprints(download);
        try (SnapshotSession session = open(download)) {
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class, () -> preview(session, page(10, 0)));
            } finally { Thread.interrupted(); }
        }
        assertEquals(before, fingerprints(download));
    }

    private PreviewReport preview(SnapshotSession session, PreviewRequest request) throws Exception {
        return reader.preview(session, OPERATOR.toHexString(), 0, request);
    }

    private SnapshotSession open(Path download) throws Exception { return new SnapshotInspector().open(download, List.of(), List.of()); }
    private static PreviewRequest page(int size, long offset) { return request(size, offset, null, null, null, null, null, null); }
    private static PreviewRequest request(int size, long offset, String state, String key, String value, String mapKey, Long from, Long to) {
        return new PreviewRequest(size, offset, state, key, value, mapKey, from, to);
    }

    private static List<Row> scalarRows(String state, int size) throws Exception {
        List<Row> rows = new ArrayList<>();
        for (int index = 0; index < size; index++) rows.add(scalar(state, "customer-" + index, string("value-" + index)));
        return rows;
    }

    private static List<Row> concat(List<Row> first, List<Row> second) {
        List<Row> rows = new ArrayList<>(first); rows.addAll(second); return rows;
    }

    private static Row scalar(String state, String key, byte[] value) throws Exception { return new Row(state, group(key), key(key), value); }
    private static Row map(String state, String key, String mapKey, String value) throws Exception {
        DataOutputSerializer encodedKey = new DataOutputSerializer(64);
        encodedKey.write(key(key));
        StringSerializer.INSTANCE.serialize(mapKey, encodedKey);
        DataOutputSerializer encodedValue = new DataOutputSerializer(64);
        encodedValue.writeBoolean(false);
        StringSerializer.INSTANCE.serialize(value, encodedValue);
        return new Row(state, group(key), encodedKey.getCopyOfBuffer(), encodedValue.getCopyOfBuffer());
    }

    private static Row timer(String state, String key, long timestamp) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeByte(group(key));
        TIMERS.serialize(new TimerHeapInternalTimer<>(timestamp, key, VoidNamespace.INSTANCE), output);
        return new Row(state, group(key), output.getCopyOfBuffer(), new byte[0]);
    }

    private static StateMetaInfoSnapshot valueSchema(String name) { return keyedSchema(name, "VALUE", StringSerializer.INSTANCE.snapshotConfiguration()); }
    private static StateMetaInfoSnapshot mapSchema(String name) {
        return keyedSchema(name, "MAP", new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE).snapshotConfiguration());
    }
    private static StateMetaInfoSnapshot keyedSchema(String name, String type, TypeSerializerSnapshot<?> value) {
        return new StateMetaInfoSnapshot(name, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                Map.of(KEYED_STATE_TYPE.toString(), type), Map.of(
                NAMESPACE_SERIALIZER.toString(), VoidNamespaceSerializer.INSTANCE.snapshotConfiguration(), VALUE_SERIALIZER.toString(), value));
    }
    private static StateMetaInfoSnapshot timerSchema(String name) {
        return new StateMetaInfoSnapshot(name, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE,
                Map.of(), Map.of(VALUE_SERIALIZER.toString(), TIMERS.snapshotConfiguration()));
    }

    private static KeyedStateHandle canonical(Path download, String fileName, List<StateMetaInfoSnapshot> schemas, List<Row> rows) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        new KeyedBackendSerializationProxy<>(StringSerializer.INSTANCE, schemas, false).write(new DataOutputViewStreamWrapper(stream));
        long[] offsets = new long[MAX_PARALLELISM];
        Map<Integer, Map<String, List<Row>>> groups = new TreeMap<>();
        for (Row row : rows) groups.computeIfAbsent(row.group(), ignored -> new LinkedHashMap<>())
                .computeIfAbsent(row.state(), ignored -> new ArrayList<>()).add(row);
        DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(stream);
        for (var group : groups.entrySet()) {
            offsets[group.getKey()] = stream.size();
            for (int stateId = 0; stateId < schemas.size(); stateId++) {
                List<Row> stateRows = group.getValue().get(schemas.get(stateId).getName());
                if (stateRows == null) continue;
                output.writeShort(stateId);
                for (int index = 0; index < stateRows.size(); index++) {
                    Row row = stateRows.get(index);
                    byte[] encodedKey = Arrays.copyOf(row.key(), row.key().length);
                    if (index == stateRows.size() - 1) setMetaDataFollowsFlagInKey(encodedKey);
                    BytePrimitiveArraySerializer.INSTANCE.serialize(encodedKey, output);
                    BytePrimitiveArraySerializer.INSTANCE.serialize(row.value(), output);
                }
            }
            output.writeShort(END_OF_KEY_GROUP_MARK);
        }
        Path file = download.resolve(fileName);
        Files.write(file, stream.toByteArray());
        return new KeyGroupsSavepointStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, MAX_PARALLELISM - 1), offsets), fileHandle(file));
    }

    private Path incremental(List<StateMetaInfoSnapshot> schemas, List<Row> rows) throws Exception {
        RocksDB.loadLibrary();
        Path download = Files.createTempDirectory(temporary, "native-");
        Path database = Files.createTempDirectory(temporary, "source-db-");
        Path checkpointDirectory = download.resolve("rocks");
        List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             WriteOptions writes = new WriteOptions().setDisableWAL(true);
             FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
            ColumnFamilyOptions defaults = new ColumnFamilyOptions(); familyOptions.add(defaults);
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaults));
            for (StateMetaInfoSnapshot schema : schemas) {
                ColumnFamilyOptions options = new ColumnFamilyOptions(); familyOptions.add(options);
                descriptors.add(new ColumnFamilyDescriptor(schema.getName().getBytes(StandardCharsets.UTF_8), options));
            }
            RocksDB db = RocksDB.open(dbOptions, database.toString(), descriptors, handles);
            try {
                Map<String, ColumnFamilyHandle> byName = new LinkedHashMap<>();
                for (int index = 0; index < schemas.size(); index++) byName.put(schemas.get(index).getName(), handles.get(index + 1));
                for (Row row : rows) db.put(byName.get(row.state()), writes, row.key(), row.value());
                db.flush(flush, handles);
                try (Checkpoint checkpoint = Checkpoint.create(db)) { checkpoint.createCheckpoint(checkpointDirectory.toString()); }
            } finally {
                handles.forEach(ColumnFamilyHandle::close); handles.clear(); db.close();
            }
        } finally { handles.forEach(ColumnFamilyHandle::close); familyOptions.forEach(ColumnFamilyOptions::close); }
        List<HandleAndLocalPath> shared = new ArrayList<>();
        List<HandleAndLocalPath> privateFiles = new ArrayList<>();
        try (var files = Files.list(checkpointDirectory)) {
            for (Path file : files.sorted().toList()) {
                if (!Files.isRegularFile(file)) continue;
                HandleAndLocalPath reference = HandleAndLocalPath.of(fileHandle(file), file.getFileName().toString());
                (file.getFileName().toString().endsWith(".sst") ? shared : privateFiles).add(reference);
            }
        }
        Path backendMetadata = download.resolve("backend-meta.bin");
        try (OutputStream stream = Files.newOutputStream(backendMetadata)) {
            new KeyedBackendSerializationProxy<>(StringSerializer.INSTANCE, schemas, false).write(new DataOutputViewStreamWrapper(stream));
        }
        writeMetadata(download, List.of(new IncrementalRemoteKeyedStateHandle(UUID.randomUUID(), new KeyGroupRange(0, MAX_PARALLELISM - 1),
                42L, shared, privateFiles, fileHandle(backendMetadata))));
        return download;
    }

    private static void writeMetadata(Path download, List<KeyedStateHandle> handles) throws Exception {
        OperatorState operator = new OperatorState(OPERATOR, 1, MAX_PARALLELISM);
        operator.putState(0, OperatorSubtaskState.builder().setManagedKeyedState(new StateObjectCollection<>(handles)).build());
        try (OutputStream output = Files.newOutputStream(download.resolve("_metadata"))) {
            Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(42, List.of(operator), List.of()), output);
        }
    }
    private static void attachInMemoryHandles(SnapshotSession session, List<KeyedStateHandle> handles) {
        // Flink 1.20's on-disk metadata requires a singleton keyed handle. Exercise the
        // reader's collection support using an in-memory assignment and real state files.
        OperatorState operator = session.metadata().getOperatorStates().iterator().next();
        operator.putState(0, OperatorSubtaskState.builder().setManagedKeyedState(new StateObjectCollection<>(handles)).build());
    }
    private static FileStateHandle fileHandle(Path file) throws Exception { return new FileStateHandle(new org.apache.flink.core.fs.Path(file.toUri()), Files.size(file)); }
    private static int group(String key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM); }
    private static byte[] key(String key) throws Exception {
        SerializedCompositeKeyBuilder<String> builder = new SerializedCompositeKeyBuilder<>(StringSerializer.INSTANCE, 1, 64);
        builder.setKeyAndKeyGroup(key, group(key));
        return builder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
    }
    private static byte[] string(String value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        StringSerializer.INSTANCE.serialize(value, output); return output.getCopyOfBuffer();
    }
    private static Map<String, String> fingerprints(Path directory) throws Exception {
        Map<String, String> result = new LinkedHashMap<>();
        try (var files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).sorted().toList()) result.put(directory.relativize(file).toString(),
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)))
                            + ":" + Files.size(file) + ":" + Files.getLastModifiedTime(file));
        }
        return result;
    }
    private record Row(String state, int group, byte[] key, byte[] value) {}
}
