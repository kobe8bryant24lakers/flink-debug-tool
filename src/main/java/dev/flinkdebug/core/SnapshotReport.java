package dev.flinkdebug.core;

import java.nio.file.Path;
import java.util.List;

/** Values observed in the snapshot; sizes refer to handles, not decoded record counts. */
public record SnapshotReport(Path metadataPath, String runtimeVersion, int metadataVersion,
                             long checkpointId, String snapshotKind, long referencedStateBytes,
                             long checkpointedBytes, List<OperatorReport> operators,
                             List<StateFile> files, List<Diagnostic> diagnostics) {
    public record OperatorReport(String operatorId, int parallelism, int maxParallelism,
                                 long referencedStateBytes, long checkpointedBytes,
                                 boolean fullyFinished, List<SubtaskReport> subtasks) {}
    public record SubtaskReport(int index, long referencedStateBytes, long checkpointedBytes,
                                List<String> handleTypes, int keyedHandleCount, int operatorHandleCount) {}
    public record StateFile(String operatorId, int subtask, String category, String handleType,
                            String logicalPath, String localPath, long declaredBytes,
                            Long actualBytes, String availability, String keyGroupRange) {}
    public record Diagnostic(String severity, String message) {}
}
