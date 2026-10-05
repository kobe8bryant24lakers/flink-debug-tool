package dev.flinkdebug.core;

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** A private, disposable snapshot whose handles can only point at local copies. */
final class LocalSnapshotCopy implements AutoCloseable {
    private final Path directory;
    private final SnapshotSession session;
    private int nextFile;

    private LocalSnapshotCopy(SnapshotSession session) throws IOException {
        this.session = session;
        directory = Files.createTempDirectory("flink-state-query-");
    }

    static LocalSnapshotCopy create(SnapshotSession session, String operatorId) throws Exception {
        var copy = new LocalSnapshotCopy(session);
        try {
            OperatorState original = session.metadata().getOperatorStates().stream()
                    .filter(op -> op.getOperatorID().toHexString().equalsIgnoreCase(operatorId))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("找不到算子 " + operatorId));
            if (original.isFullyFinished()) throw new IOException("算子已经完成，未包含可查询的状态。");
            var relocated = new OperatorState(original.getOperatorID(), original.getParallelism(), original.getMaxParallelism());
            for (var subtask : original.getSubtaskStates().entrySet()) {
                checkInterrupted();
                var state = subtask.getValue();
                if (!state.getInputChannelState().isEmpty() || !state.getResultSubpartitionState().isEmpty()) {
                    throw new IOException("State Processor API 不支持 unaligned checkpoint 内容查询；请使用快照概览。");
                }
                var keyed = new ArrayList<KeyedStateHandle>();
                for (var handle : state.getManagedKeyedState()) keyed.add(copy.keyed(handle));
                var operator = new ArrayList<OperatorStateHandle>();
                for (var handle : state.getManagedOperatorState()) {
                    if (!(handle instanceof OperatorStreamStateHandle stream)) {
                        throw new IOException("暂不支持该 operator state handle: " + handle.getClass().getSimpleName());
                    }
                    operator.add(new OperatorStreamStateHandle(stream.getStateNameToPartitionOffsets(), copy.stream(stream.getDelegateStateHandle())));
                }
                relocated.putState(subtask.getKey(), OperatorSubtaskState.builder()
                        .setManagedKeyedState(new StateObjectCollection<>(keyed))
                        .setManagedOperatorState(new StateObjectCollection<>(operator)).build());
            }
            var metadata = new CheckpointMetadata(session.metadata().getCheckpointId(), List.of(relocated), List.of());
            try (var output = Files.newOutputStream(copy.directory.resolve("_metadata"))) {
                Checkpoints.storeCheckpointMetadata(metadata, output);
            }
            return copy;
        } catch (Exception | Error e) {
            try { copy.close(); } catch (IOException cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
    }

    Path directory() { return directory; }

    private KeyedStateHandle keyed(KeyedStateHandle handle) throws IOException {
        if (handle instanceof KeyGroupsSavepointStateHandle full) {
            return new KeyGroupsSavepointStateHandle(full.getGroupRangeOffsets(), stream(full.getDelegateStateHandle()));
        }
        if (handle instanceof KeyGroupsStateHandle full) {
            return new KeyGroupsStateHandle(full.getGroupRangeOffsets(), stream(full.getDelegateStateHandle()));
        }
        if (handle instanceof IncrementalRemoteKeyedStateHandle incremental) {
            return new IncrementalRemoteKeyedStateHandle(incremental.getBackendIdentifier(), incremental.getKeyGroupRange(),
                    incremental.getCheckpointId(), items(incremental.getSharedState()), items(incremental.getPrivateState()),
                    stream(incremental.getMetaDataStateHandle()), incremental.getCheckpointedSize());
        }
        throw new IOException("暂不支持该 keyed state handle: " + handle.getClass().getSimpleName());
    }

    private List<IncrementalKeyedStateHandle.HandleAndLocalPath> items(List<IncrementalKeyedStateHandle.HandleAndLocalPath> items) throws IOException {
        var result = new ArrayList<IncrementalKeyedStateHandle.HandleAndLocalPath>();
        for (var item : items) {
            String name = item.getLocalPath();
            Path relative = Path.of(name);
            if (relative.isAbsolute() || relative.getNameCount() != 1 || name.equals(".") || name.equals("..") || name.contains("\\")) {
                throw new IOException("无效的 RocksDB 文件名: " + name);
            }
            result.add(IncrementalKeyedStateHandle.HandleAndLocalPath.of(stream(item.getHandle()), name));
        }
        return result;
    }

    private StreamStateHandle stream(StreamStateHandle original) throws IOException {
        checkInterrupted();
        Path target = directory.resolve("state-" + nextFile++);
        try (var input = session.files().open(original); var output = Files.newOutputStream(target)) {
            byte[] buffer = new byte[64 * 1024];
            int count;
            while ((count = input.read(buffer)) != -1) {
                checkInterrupted();
                output.write(buffer, 0, count);
            }
        }
        long size = Files.size(target);
        if (size != original.getStateSize()) {
            throw new IOException("状态文件大小不匹配，声明 " + original.getStateSize() + " bytes，实际 " + size + " bytes。");
        }
        return new FileStateHandle(new org.apache.flink.core.fs.Path(target.toUri()), size);
    }

    static void checkInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("本地解析已取消。");
    }

    @Override public void close() throws IOException {
        IOException failure = null;
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                try { Files.deleteIfExists(path); } catch (IOException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            }
        }
        if (failure != null) throw failure;
    }
}
