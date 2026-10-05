package dev.flinkdebug.core;

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.jar.JarFile;

/** Flink 1.20 snapshot inspection. Only metadata is deserialized; user state stays on disk. */
public final class SnapshotInspector {
    public SnapshotSession open(Path input, List<PathMapping> mappings, List<Path> jars)
            throws Exception {
        Path path = input.toAbsolutePath().normalize();
        Path metadataPath = Files.isDirectory(path) ? path.resolve("_metadata") : path;
        if (!Files.isRegularFile(metadataPath)) {
            throw new IOException("找不到 Flink _metadata 文件: " + metadataPath);
        }
        if (Files.size(metadataPath) < 8) {
            throw new IOException("Flink metadata 文件被截断，至少需要 8 字节头部");
        }
        LocalStateFiles files = new LocalStateFiles(metadataPath.getParent(), mappings);
        URL[] urls = new URL[jars.size()];
        for (int index = 0; index < jars.size(); index++) {
            Path jar = jars.get(index).toAbsolutePath().normalize();
            if (!Files.isRegularFile(jar) || !Files.isReadable(jar)) {
                throw new IOException("无法读取用户 JAR: " + jar);
            }
            try (JarFile ignored = new JarFile(jar.toFile())) {
                urls[index] = jar.toUri().toURL();
            }
        }
        URLClassLoader loader = new URLClassLoader(urls, SnapshotInspector.class.getClassLoader());
        try {
            int formatVersion;
            CheckpointMetadata metadata;
            try (DataInputStream stream = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(metadataPath)))) {
                stream.mark(8);
                int magic = stream.readInt();
                formatVersion = stream.readInt();
                if (magic != Checkpoints.HEADER_MAGIC_NUMBER) {
                    throw new IOException("文件不是受支持的 Flink metadata，magic number 不匹配");
                }
                stream.reset();
                metadata = Checkpoints.loadCheckpointMetadata(
                        stream, loader, metadataPath.getParent().toUri().toString());
                if (stream.read() != -1) {
                    throw new IOException("Flink metadata 包含未识别的尾部数据");
                }
            } catch (EOFException exception) {
                throw new IOException("Flink metadata 内容被截断", exception);
            }
            SnapshotReport report = report(metadataPath, formatVersion, metadata, files);
            return new SnapshotSession(metadata, report, files, loader);
        } catch (Exception | Error exception) {
            loader.close();
            throw exception;
        }
    }

    private SnapshotReport report(Path metadataPath, int formatVersion,
                                  CheckpointMetadata metadata, LocalStateFiles files) {
        List<SnapshotReport.OperatorReport> operators = new ArrayList<>();
        List<SnapshotReport.StateFile> stateFiles = new ArrayList<>();
        List<SnapshotReport.Diagnostic> diagnostics = new ArrayList<>();
        long referencedBytes = 0;
        long checkpointedBytes = 0;
        boolean incremental = false;
        boolean channelState = false;
        List<OperatorState> sorted = metadata.getOperatorStates().stream()
                .sorted(Comparator.comparing(operator -> operator.getOperatorID().toHexString()))
                .toList();
        for (OperatorState operator : sorted) {
            String operatorId = operator.getOperatorID().toHexString();
            List<SnapshotReport.SubtaskReport> subtasks = new ArrayList<>();
            if (operator.getCoordinatorState() != null) {
                addStream(operatorId, -1, "coordinator", "", operator.getCoordinatorState(), files,
                        stateFiles);
            }
            for (var entry : operator.getSubtaskStates().entrySet().stream()
                    .sorted(java.util.Map.Entry.comparingByKey()).toList()) {
                int subtaskIndex = entry.getKey();
                OperatorSubtaskState subtask = entry.getValue();
                List<String> types = new ArrayList<>();
                for (KeyedStateHandle handle : subtask.getManagedKeyedState()) {
                    types.add("managed-keyed: " + handle.getClass().getSimpleName());
                    incremental |= handle instanceof IncrementalRemoteKeyedStateHandle;
                    addKeyed(operatorId, subtaskIndex, "managed-keyed", handle, files, stateFiles);
                }
                for (KeyedStateHandle handle : subtask.getRawKeyedState()) {
                    types.add("raw-keyed: " + handle.getClass().getSimpleName());
                    addKeyed(operatorId, subtaskIndex, "raw-keyed", handle, files, stateFiles);
                }
                for (OperatorStateHandle handle : subtask.getManagedOperatorState()) {
                    types.add("managed-operator: " + handle.getClass().getSimpleName());
                    addStream(operatorId, subtaskIndex, "managed-operator", "",
                            handle.getDelegateStateHandle(), files, stateFiles);
                }
                for (OperatorStateHandle handle : subtask.getRawOperatorState()) {
                    types.add("raw-operator: " + handle.getClass().getSimpleName());
                    addStream(operatorId, subtaskIndex, "raw-operator", "",
                            handle.getDelegateStateHandle(), files, stateFiles);
                }
                for (var handle : subtask.getInputChannelState()) {
                    types.add("input-channel: " + handle.getClass().getSimpleName());
                    addChannel(operatorId, subtaskIndex, "input-channel", handle, files, stateFiles);
                    channelState = true;
                }
                for (var handle : subtask.getResultSubpartitionState()) {
                    types.add("result-subpartition: " + handle.getClass().getSimpleName());
                    addChannel(operatorId, subtaskIndex, "result-subpartition", handle, files,
                            stateFiles);
                    channelState = true;
                }
                subtasks.add(new SnapshotReport.SubtaskReport(subtaskIndex, subtask.getStateSize(),
                        subtask.getCheckpointedSize(), List.copyOf(types),
                        subtask.getManagedKeyedState().size() + subtask.getRawKeyedState().size(),
                        subtask.getManagedOperatorState().size() + subtask.getRawOperatorState().size()));
            }
            operators.add(new SnapshotReport.OperatorReport(operatorId, operator.getParallelism(),
                    operator.getMaxParallelism(), operator.getStateSize(),
                    operator.getCheckpointedSize(), operator.isFullyFinished(), List.copyOf(subtasks)));
            referencedBytes += operator.getStateSize();
            checkpointedBytes += operator.getCheckpointedSize();
        }
        String kind = metadata.getCheckpointProperties() == null ? "UNKNOWN"
                : metadata.getCheckpointProperties().getCheckpointType().toString();
        if (incremental) {
            diagnostics.add(new SnapshotReport.Diagnostic("INFO",
                    "检测到增量 RocksDB 状态；chk-N 目录之外的 shared/private 引用也必须下载完整。"));
        }
        if (channelState) {
            diagnostics.add(new SnapshotReport.Diagnostic("WARNING",
                    "存在 channel state；只能检查其文件引用，首版不解码未对齐 checkpoint 的在途数据。"));
        }
        long unavailable = stateFiles.stream().filter(file ->
                !"PRESENT".equals(file.availability()) && !"EMBEDDED".equals(file.availability()))
                .count();
        if (unavailable > 0) {
            diagnostics.add(new SnapshotReport.Diagnostic("WARNING",
                    unavailable + " 个状态引用未就绪，请检查文件清单和原始 URI 到本地目录的映射。"));
        }
        diagnostics.add(new SnapshotReport.Diagnostic("INFO",
                "状态大小来自 metadata 中的 handle 引用，可能重复引用共享文件；不是业务记录数或去重磁盘占用。"));
        diagnostics.add(new SnapshotReport.Diagnostic("INFO",
                "metadata 格式版本不代表生成快照的 Flink 版本；Flink 1.20 metadata 不包含原始 UID、算子名称和 checkpoint 耗时。"));
        return new SnapshotReport(metadataPath, EnvironmentInformation.getVersion(), formatVersion,
                metadata.getCheckpointId(), kind, referencedBytes, checkpointedBytes,
                List.copyOf(operators), List.copyOf(stateFiles), List.copyOf(diagnostics));
    }

    private void addKeyed(String operator, int subtask, String category, KeyedStateHandle handle,
                          LocalStateFiles files, List<SnapshotReport.StateFile> result) {
        String range = handle.getKeyGroupRange().getStartKeyGroup() + "–"
                + handle.getKeyGroupRange().getEndKeyGroup();
        if (handle instanceof IncrementalRemoteKeyedStateHandle incremental) {
            addStream(operator, subtask, category + "/meta", range,
                    incremental.getMetaDataStateHandle(), files, result);
            for (var entry : incremental.getSharedState()) {
                addStream(operator, subtask, category + "/shared", range,
                        entry.getHandle(), files, result);
            }
            for (var entry : incremental.getPrivateState()) {
                addStream(operator, subtask, category + "/private", range,
                        entry.getHandle(), files, result);
            }
        } else if (handle instanceof KeyGroupsStateHandle grouped) {
            addStream(operator, subtask, category, range, grouped.getDelegateStateHandle(), files,
                    result);
        } else {
            result.add(new SnapshotReport.StateFile(operator, subtask, category,
                    handle.getClass().getSimpleName(), handle.getClass().getName(), null,
                    handle.getStateSize(), null, "UNSUPPORTED", range));
        }
    }

    private void addChannel(String operator, int subtask, String category,
                            AbstractChannelStateHandle<?> handle, LocalStateFiles files,
                            List<SnapshotReport.StateFile> result) {
        addStream(operator, subtask, category, "", handle.getDelegate(), files, result);
    }

    private void addStream(String operator, int subtask, String category, String range,
                           StreamStateHandle handle, LocalStateFiles files,
                           List<SnapshotReport.StateFile> result) {
        if (handle == null) return;
        LocalStateFiles.Resolution resolution = files.resolve(handle);
        result.add(new SnapshotReport.StateFile(operator, subtask, category,
                handle.getClass().getSimpleName(), resolution.logicalPath(),
                resolution.localPath() == null ? null : resolution.localPath().toString(),
                handle.getStateSize(), resolution.actualBytes(), resolution.availability(), range));
    }
}
