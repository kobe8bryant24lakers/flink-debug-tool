package dev.flinkdebug.core;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readKeyGroup;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.serializeKeyGroup;
import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.FullSnapshotUtil.hasMetaDataFollowsFlag;

/** Reads filtered pages directly from downloaded files; never restores a running Flink job. */
public final class DataPreviewService {
    public static final int MAX_ENTRIES = 1000;
    private static final long MAX_PAGE_BYTES = 32L * 1024 * 1024;

    public PreviewReport preview(SnapshotSession session, String operatorId, int subtask, int limit) throws Exception {
        return preview(session, operatorId, subtask, new PreviewRequest(limit, 0, null, null, null, null, null, null));
    }

    public PreviewReport preview(SnapshotSession session, String operatorId, int subtask, PreviewRequest request) throws Exception {
        if (session == null) throw new IllegalArgumentException("Open a snapshot first");
        if (request == null) throw new IllegalArgumentException("Specify a page request");
        checkCancelled();
        OperatorState operator = session.metadata().getOperatorStates().stream()
                .filter(state -> state.getOperatorID().toHexString().equalsIgnoreCase(operatorId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown operator ID: " + operatorId));
        if (subtask < 0 || subtask >= operator.getParallelism()) {
            throw new IllegalArgumentException("Subtask must be between 0 and " + (operator.getParallelism() - 1));
        }
        Sample sample = new Sample(request);
        OperatorSubtaskState state = operator.getState(subtask);
        if (state == null) {
            sample.warn("This subtask has no saved state.");
            return sample.report();
        }
        if (!state.getRawKeyedState().isEmpty()) sample.incomplete("Raw keyed state is not decoded; this reader covers managed keyed state only.");
        if (!state.getManagedOperatorState().isEmpty() || !state.getRawOperatorState().isEmpty()) {
            sample.incomplete("Operator/list/broadcast state payloads are not read by this keyed-state reader.");
        }
        if (!state.getInputChannelState().isEmpty() || !state.getResultSubpartitionState().isEmpty()) {
            sample.incomplete("Unaligned checkpoint channel buffers are not business state and are not decoded.");
        }
        int prefixBytes = computeRequiredBytesInKeyGroupPrefix(operator.getMaxParallelism());
        // Catalog every handle before collecting rows. A large first state must not hide
        // the schema of later states or later handles when a page has already filled.
        List<PreparedHandle> handles = new ArrayList<>();
        for (KeyedStateHandle handle : state.getManagedKeyedState()) {
            checkCancelled();
            try {
                if (handle instanceof KeyGroupsSavepointStateHandle || handle instanceof IncrementalRemoteKeyedStateHandle) {
                    handles.add(prepare(session, handle, sample));
                } else {
                    sample.incomplete("Unsupported keyed-state handle " + (handle == null ? "null" : handle.getClass().getSimpleName())
                            + "; changelog and non-canonical full snapshots require a dedicated reader.");
                }
            } catch (InterruptedException cancellation) {
                throw cancellation;
            } catch (Exception | LinkageError failure) {
                checkCancelled();
                sample.incomplete("Could not read schema for " + (handle == null ? "null handle" : handle.getClass().getSimpleName())
                        + ": " + failure.getClass().getSimpleName() + ": " + safeMessage(failure));
            }
        }
        for (PreparedHandle handle : handles) {
            checkCancelled();
            if (sample.hasMore) break;
            try {
                if (handle.handle() instanceof KeyGroupsSavepointStateHandle canonical) {
                    readCanonical(session, canonical, handle, prefixBytes, sample);
                } else if (handle.handle() instanceof IncrementalRemoteKeyedStateHandle incremental) {
                    readIncremental(session, incremental, handle, prefixBytes, sample);
                }
            } catch (InterruptedException cancellation) {
                throw cancellation;
            } catch (Exception | LinkageError failure) {
                checkCancelled();
                sample.incomplete("Could not read " + handle.handle().getClass().getSimpleName()
                        + ": " + failure.getClass().getSimpleName() + ": " + safeMessage(failure));
            }
        }
        checkCancelled();
        if (state.getManagedKeyedState().isEmpty()) sample.warn("This subtask has no managed keyed state.");
        if (sample.hasMore) sample.warn("More matching records are available; load the next page to continue.");
        if (sample.entries.stream().anyMatch(entry -> entry.ttlTimestamp() != null)) {
            sample.warn("TTL timestamps are stored last-access times, not expiration times. Entries are not filtered: retention and visibility policy are not available from the serializer snapshot.");
        }
        return sample.report();
    }

    private record PreparedHandle(KeyedStateHandle handle, KeyedBackendSerializationProxy<?> proxy,
                                  List<PreviewDecoder.Schema> schemas) {}

    private PreparedHandle prepare(SnapshotSession session, KeyedStateHandle handle, Sample sample) throws Exception {
        try (FSDataInputStream input = session.files().open(handle instanceof KeyGroupsSavepointStateHandle canonical
                ? canonical.getDelegateStateHandle() : ((IncrementalRemoteKeyedStateHandle) handle).getMetaDataStateHandle())) {
            KeyedBackendSerializationProxy<?> proxy = readMetadata(input, session.classLoader());
            List<PreviewDecoder.Schema> schemas = PreviewDecoder.schemas(proxy);
            sample.schemas(schemas);
            return new PreparedHandle(handle, proxy, schemas);
        } catch (IOException | RuntimeException | LinkageError unavailableMetadata) {
            if (!(handle instanceof IncrementalRemoteKeyedStateHandle)) throw unavailableMetadata;
            sample.incomplete("RocksDB state schema unavailable: " + safeMessage(unavailableMetadata) + "; values remain raw.");
            return new PreparedHandle(handle, null, List.of());
        }
    }

    private void readCanonical(SnapshotSession session, KeyGroupsSavepointStateHandle handle,
                               PreparedHandle prepared, int prefixBytes, Sample sample) throws Exception {
        // See Flink 1.20 FullSnapshotRestoreOperation: metadata + offset-addressed key groups.
        // Use local files explicitly instead of handle.openInputStream(), which may access a remote FS.
        try (FSDataInputStream input = session.files().open(handle.getDelegateStateHandle())) {
            KeyedBackendSerializationProxy<?> proxy = prepared.proxy();
            List<PreviewDecoder.Schema> schemas = prepared.schemas();
            for (Tuple2<Integer, Long> group : handle.getGroupRangeOffsets()) {
                checkCancelled();
                if (group.f1 == 0L) continue;
                if (group.f1 < 0L) throw new IOException("Negative key-group offset");
                input.seek(group.f1);
                // A compression-stream close must not close the seekable parent used for the next group.
                InputStream groupInput = new FilterInputStream(input) { @Override public void close() {} };
                if (proxy.isUsingKeyGroupCompression()) {
                    groupInput = SnappyStreamCompressionDecorator.INSTANCE.decorateWithCompression(groupInput);
                }
                try (DataInputViewStreamWrapper data = new DataInputViewStreamWrapper(groupInput)) {
                    int stateId = data.readUnsignedShort();
                    while (stateId != END_OF_KEY_GROUP_MARK) {
                        checkCancelled();
                        if (stateId >= schemas.size()) throw new IOException("Unknown canonical state ID " + stateId);
                        PreviewDecoder.Schema schema = schemas.get(stateId);
                        boolean selected = sample.selects(schema.report().name(), schema);
                        PreviewDecoder.RawBytes key = readBytes(data, selected ? PreviewDecoder.MAX_RECORD_BYTES : 1);
                        PreviewDecoder.RawBytes value = readBytes(data, selected ? PreviewDecoder.MAX_RECORD_BYTES : 0);
                        if (key.bytes().length == 0) throw new IOException("Empty canonical key");
                        boolean following = hasMetaDataFollowsFlag(key.bytes());
                        if (following) clearMetaDataFollowsFlag(key.bytes());
                        if (selected) {
                            sample.accept(PreviewDecoder.entry(schema.report().name(), schema, key, value, group.f0, prefixBytes));
                            if (sample.hasMore) return;
                        }
                        if (following) stateId = data.readUnsignedShort();
                    }
                }
            }
        }
    }

    private static PreviewDecoder.RawBytes readBytes(DataInputViewStreamWrapper input, int maximumRetained) throws Exception {
        int length = input.readInt();
        if (length < 0) throw new IOException("Negative byte-array length");
        int retained = Math.min(length, maximumRetained);
        byte[] bytes = new byte[retained];
        input.readFully(bytes);
        int remaining = length - retained;
        while (remaining > 0) {
            checkCancelled();
            int amount = Math.min(remaining, 64 * 1024);
            input.skipBytesToRead(amount);
            remaining -= amount;
        }
        return new PreviewDecoder.RawBytes(bytes, length);
    }

    private void readIncremental(SnapshotSession session, IncrementalRemoteKeyedStateHandle handle,
                                 PreparedHandle prepared, int prefixBytes, Sample sample) throws Exception {
        Map<String, PreviewDecoder.Schema> schemas = new LinkedHashMap<>();
        for (PreviewDecoder.Schema schema : prepared.schemas()) schemas.put(schema.report().name(), schema);
        Path temporary = Files.createTempDirectory("flink-state-preview-");
        try {
            List<HandleAndLocalPath> files = new ArrayList<>(handle.getSharedState());
            files.addAll(handle.getPrivateState());
            if (files.isEmpty()) throw new IOException("Incremental snapshot contains no RocksDB files");
            for (HandleAndLocalPath file : files) {
                checkCancelled();
                copyFile(session, file, temporary);
            }
            checkCancelled();
            RocksDB.loadLibrary();
            List<byte[]> families;
            try (Options options = new Options().setCreateIfMissing(false)) {
                families = RocksDB.listColumnFamilies(options, temporary.toString());
            }
            List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
            try {
                for (byte[] family : families) {
                    ColumnFamilyOptions options = new ColumnFamilyOptions();
                    familyOptions.add(options);
                    descriptors.add(new ColumnFamilyDescriptor(family, options));
                }
                try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false).setParanoidChecks(true);
                     ReadOptions reads = new ReadOptions().setVerifyChecksums(true).setFillCache(false)) {
                    RocksDB db = RocksDB.openReadOnly(dbOptions, temporary.toString(), descriptors, familyHandles, true);
                    try {
                        ByteBuffer buffer = ByteBuffer.allocateDirect(PreviewDecoder.MAX_RECORD_BYTES);
                        byte[] firstGroupPrefix = new byte[prefixBytes];
                        serializeKeyGroup(handle.getKeyGroupRange().getStartKeyGroup(), firstGroupPrefix);
                        for (byte[] family : families) {
                            String name = new String(family, StandardCharsets.UTF_8);
                            if (!schemas.containsKey(name) && !Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                                sample.schema(new PreviewReport.StateSchema(name, "UNKNOWN", "unknown", "unknown", "unknown"));
                            }
                        }
                        for (int index = 0; index < familyHandles.size(); index++) {
                            checkCancelled();
                            String name = new String(families.get(index), StandardCharsets.UTF_8);
                            PreviewDecoder.Schema schema = schemas.get(name);
                            boolean defaultFamily = Arrays.equals(families.get(index), RocksDB.DEFAULT_COLUMN_FAMILY);
                            if (!sample.selects(name, schema)) continue;
                            try (RocksIterator iterator = db.newIterator(familyHandles.get(index), reads)) {
                                if (defaultFamily) iterator.seekToFirst();
                                else iterator.seek(firstGroupPrefix);
                                while (iterator.isValid()) {
                                    checkCancelled();
                                    PreviewDecoder.RawBytes key = iteratorBytes(iterator, buffer, true);
                                    if (!defaultFamily) {
                                        int group = readKeyGroup(prefixBytes, new DataInputDeserializer(key.bytes()));
                                        // A handle's intersection may cover fewer groups than its SST files.
                                        if (group > handle.getKeyGroupRange().getEndKeyGroup()) break;
                                        if (!handle.getKeyGroupRange().contains(group)) throw new IOException("RocksDB key-group outside handle range");
                                    }
                                    if (schema == null) sample.schema(new PreviewReport.StateSchema(name, "UNKNOWN", "unknown", "unknown", "unknown"));
                                    PreviewDecoder.RawBytes value = iteratorBytes(iterator, buffer, false);
                                    sample.accept(PreviewDecoder.entry(name, schema, key, value, -1, prefixBytes));
                                    if (sample.hasMore) return;
                                    iterator.next();
                                }
                                iterator.status();
                            }
                        }
                    } finally {
                        // Flink also closes column-family handles before closing the database.
                        for (ColumnFamilyHandle family : familyHandles) family.close();
                        familyHandles.clear();
                        db.close();
                    }
                }
            } finally {
                for (ColumnFamilyHandle family : familyHandles) family.close();
                for (ColumnFamilyOptions options : familyOptions) options.close();
            }
        } finally {
            deleteTemporary(temporary, sample);
        }
    }

    private static PreviewDecoder.RawBytes iteratorBytes(RocksIterator iterator, ByteBuffer buffer, boolean key) throws IOException {
        buffer.clear();
        int length = key ? iterator.key(buffer) : iterator.value(buffer);
        if (length < 0) throw new IOException("Negative RocksDB record size");
        byte[] bytes = new byte[Math.min(length, buffer.capacity())];
        buffer.get(bytes);
        return new PreviewDecoder.RawBytes(bytes, length);
    }

    private static void copyFile(SnapshotSession session, HandleAndLocalPath source, Path directory) throws Exception {
        String name = source.getLocalPath();
        if (name == null || name.isBlank() || name.contains("/") || name.contains("\\")
                || name.equals(".") || name.equals("..") || Path.of(name).isAbsolute()) {
            throw new IOException("Unsafe RocksDB file name in checkpoint metadata");
        }
        Path target = directory.resolve(name);
        // Every byte is copied, never hard-linked; RocksDB sees only disposable private copies.
        try (FSDataInputStream input = session.files().open(source.getHandle());
             OutputStream output = Files.newOutputStream(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            byte[] buffer = new byte[64 * 1024];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                checkCancelled();
                if (count > 0) output.write(buffer, 0, count);
            }
        }
    }

    private static KeyedBackendSerializationProxy<?> readMetadata(FSDataInputStream input, ClassLoader loader) throws IOException {
        KeyedBackendSerializationProxy<?> proxy = new KeyedBackendSerializationProxy<>(loader);
        proxy.read(new DataInputViewStreamWrapper(input));
        return proxy;
    }

    private static void deleteTemporary(Path directory, Sample sample) {
        try (var files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
        } catch (IOException cleanupFailure) {
            sample.warn("Could not completely remove temporary RocksDB copy " + directory + ": " + safeMessage(cleanupFailure));
        }
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        if (message == null) return "no further details";
        return message.length() > 1000 ? message.substring(0, 1000) + "…" : message;
    }

    private static void checkCancelled() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("State preview cancelled");
    }

    private static final class Sample {
        final PreviewRequest request;
        final Map<PreviewReport.StateSchema, PreviewReport.StateSchema> schemas = new LinkedHashMap<>();
        final List<PreviewReport.StateEntry> entries = new ArrayList<>();
        final List<String> warnings = new ArrayList<>();
        long matched;
        long scanned;
        long retainedBytes;
        boolean hasMore;
        boolean complete = true;

        Sample(PreviewRequest request) { this.request = request; }

        boolean selects(String stateName, PreviewDecoder.Schema schema) {
            if (request.stateName() != null && !request.stateName().equals(stateName)) return false;
            // A condition on a map key or timer timestamp cannot match another known layout.
            // Unknown layouts must be examined so an unevaluable condition is reported.
            if (schema != null) {
                if (request.mapKeyContains() != null && !"MAP".equals(schema.report().type())) return false;
                if ((request.timerFrom() != null || request.timerTo() != null)
                        && schema.metadata().getBackendStateType()
                        != org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) return false;
                if (request.valueContains() != null && schema.metadata().getBackendStateType()
                        == org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) return false;
            }
            return true;
        }

        void accept(PreviewReport.StateEntry entry) {
            scanned++;
            if (entry.decodeStatus() == null || !entry.decodeStatus().startsWith("DECODED:")) {
                incomplete("State " + entry.stateName() + " contains records that are not fully decoded; raw or prefix data does not establish complete decoded results.");
            }
            // Check unknown filter fields before testing any known nonmatching field. Otherwise
            // an AND condition could silently conceal rows whose filters cannot all be evaluated.
            boolean evaluable = filterAvailable(entry.stateName(), "key", request.keyContains(), entry.key())
                    & filterAvailable(entry.stateName(), "value", request.valueContains(), entry.value())
                    & filterAvailable(entry.stateName(), "map key", request.mapKeyContains(), entry.mapKey());
            if ((request.timerFrom() != null || request.timerTo() != null) && entry.timerTimestamp() == null) {
                incomplete("Timer timestamp filter could not be evaluated for state " + entry.stateName() + "; undecoded rows were excluded.");
                evaluable = false;
            }
            if (!evaluable || !contains(entry.key(), request.keyContains())
                    || !contains(entry.value(), request.valueContains())
                    || !contains(entry.mapKey(), request.mapKeyContains())) return;
            if (request.timerFrom() != null && entry.timerTimestamp() < request.timerFrom()) return;
            if (request.timerTo() != null && entry.timerTimestamp() > request.timerTo()) return;
            matched++;
            if (matched <= request.offset()) return;
            long size = retainedSize(entry);
            if (entries.size() >= request.limit()) hasMore = true;
            else if (!entries.isEmpty() && retainedBytes + size > MAX_PAGE_BYTES) {
                // A page is bounded by both rows and retained decoded text. Always allow its
                // first record so a valid large record can never make pagination stall.
                hasMore = true;
                warn("Page memory budget reached; fewer rows were returned. Load the next page to continue.");
            } else {
                entries.add(entry);
                retainedBytes += size;
            }
        }

        private static long retainedSize(PreviewReport.StateEntry entry) {
            long chars = length(entry.stateName()) + length(entry.key()) + length(entry.namespace())
                    + length(entry.value()) + length(entry.mapKey()) + length(entry.keyHex())
                    + length(entry.valueHex()) + length(entry.decodeStatus()) + length(entry.timerType());
            return 256 + chars * 2;
        }

        private static long length(String value) { return value == null ? 0 : value.length(); }

        private boolean filterAvailable(String stateName, String field, String condition, String decoded) {
            if (condition == null || decoded != null) return true;
            incomplete("The " + field + " filter could not be evaluated for state " + stateName + "; undecoded rows were excluded.");
            return false;
        }

        private static boolean contains(String decoded, String condition) {
            return condition == null || decoded != null && decoded.contains(condition);
        }

        void schema(PreviewReport.StateSchema schema) { schemas.putIfAbsent(schema, schema); }
        void schemas(List<PreviewDecoder.Schema> values) { for (PreviewDecoder.Schema value : values) schema(value.report()); }
        void warn(String warning) { if (!warnings.contains(warning)) warnings.add(warning); }
        void incomplete(String warning) { complete = false; warn(warning); }
        PreviewReport report() {
            return new PreviewReport(List.copyOf(schemas.values()), List.copyOf(entries), hasMore, List.copyOf(warnings),
                    new PreviewReport.PageInfo(request.offset(), request.offset() + entries.size(), scanned, hasMore, complete));
        }
    }
}
