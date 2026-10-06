package dev.flinkdebug.core;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotInspectorTest {
    @TempDir Path directory;

    @Test void readsFlinkWrittenMetadataAndClosingKeepsEveryFileUntouched() throws Exception {
        Path stateFile = directory.resolve("state.sst");
        byte[] stateBytes = {1, 2, 3, 4};
        Files.write(stateFile, stateBytes);
        var shared = new FileStateHandle(new org.apache.flink.core.fs.Path(stateFile.toUri()), 4);
        var state = incremental(shared, new ByteStreamStateHandle("manifest", new byte[]{9, 8}));
        Path metadata = writeSnapshot(state);
        byte[] metadataBytes = Files.readAllBytes(metadata);
        try (SnapshotSession session = new SnapshotInspector().open(directory, List.of(), List.of())) {
            SnapshotReport report = session.report();
            assertEquals(42, report.checkpointId());
            assertEquals(4, report.metadataVersion());
            assertEquals(1, report.operators().size());
            var operator = report.operators().get(0);
            assertEquals(1, operator.parallelism());
            assertEquals(128, operator.maxParallelism());
            assertNull(operator.operatorName());
            assertNull(operator.operatorUid());
            assertEquals(3, report.files().size());
            assertEquals("PRESENT", report.files().stream()
                    .filter(file -> file.category().endsWith("/shared")).findFirst().orElseThrow()
                    .availability());
            try (var stream = session.files().open(shared)) {
                assertArrayEquals(stateBytes, stream.readAllBytes());
            }
        }
        assertArrayEquals(metadataBytes, Files.readAllBytes(metadata));
        assertArrayEquals(stateBytes, Files.readAllBytes(stateFile));
    }

    @Test void readsOptionalLabelsWhenTheOperatorMetadataApiProvidesThem() {
        OperatorState named = new NamedOperator();
        assertEquals("fixture-process", SnapshotInspector.operatorText(named, "getOperatorName"));
        assertEquals("fixture-process-v1", SnapshotInspector.operatorText(named, "getOperatorUid"));
        assertNull(SnapshotInspector.operatorText(named, "missingGetter"));
    }

    @Test void missingOrBlankLabelsRemainUnknown() {
        OperatorState unnamed = new UnnamedOperator();
        assertNull(SnapshotInspector.operatorText(unnamed, "getOperatorName"));
        assertNull(SnapshotInspector.operatorText(unnamed, "getOperatorUid"));
    }

    public static final class NamedOperator extends OperatorState {
        public NamedOperator() { super(new OperatorID(11, 12), 2, 128); }
        public java.util.Optional<String> getOperatorName() { return java.util.Optional.of("fixture-process"); }
        public java.util.Optional<String> getOperatorUid() { return java.util.Optional.of("fixture-process-v1"); }
    }

    public static final class UnnamedOperator extends OperatorState {
        public UnnamedOperator() { super(new OperatorID(13, 14), 2, 128); }
        public java.util.Optional<String> getOperatorName() { return java.util.Optional.empty(); }
        public java.util.Optional<String> getOperatorUid() { return java.util.Optional.of("  "); }
    }

    @Test void missingStateDoesNotPreventMetadataInspection() throws Exception {
        var missing = new FileStateHandle(new org.apache.flink.core.fs.Path(
                directory.resolve("missing.sst").toUri()), 200);
        Path metadata = writeSnapshot(incremental(missing,
                new ByteStreamStateHandle("private", new byte[0])));
        try (var session = new SnapshotInspector().open(metadata, List.of(), List.of())) {
            assertEquals(42, session.metadata().getCheckpointId());
            assertTrue(session.report().files().stream()
                    .anyMatch(file -> "MISSING".equals(file.availability())));
            assertThrows(IOException.class, () -> session.files().open(missing));
        }
    }

    @Test void explicitMappingRelocatesRemoteIncrementalSharedFiles() throws Exception {
        Path sharedDirectory = Files.createDirectory(directory.resolve("downloaded-shared"));
        Files.write(sharedDirectory.resolve("abc.sst"), new byte[]{3, 4, 5});
        var remote = new FileStateHandle(new org.apache.flink.core.fs.Path(
                "s3://checkpoint-bucket/job/shared/abc.sst"), 3);
        writeSnapshot(incremental(remote, new ByteStreamStateHandle("private", new byte[0])));
        var mapping = new PathMapping("s3://checkpoint-bucket/job/shared", sharedDirectory);
        try (var session = new SnapshotInspector().open(directory, List.of(mapping), List.of())) {
            var resolution = session.files().resolve(remote);
            assertEquals("PRESENT", resolution.availability());
            assertEquals(sharedDirectory.resolve("abc.sst"), resolution.localPath());
            try (var stream = session.files().open(remote)) {
                assertArrayEquals(new byte[]{3, 4, 5}, stream.readAllBytes());
            }
        }
    }

    @Test void relocatedSavepointRelativeHandlesUseCurrentMetadataDirectory() throws Exception {
        Path stateFile = directory.resolve("shared.sst");
        Files.write(stateFile, new byte[]{6, 7});
        var relative = new RelativeFileStateHandle(
                new org.apache.flink.core.fs.Path("s3://old-bucket/savepoint/shared.sst"),
                "shared.sst", 2);
        writeSnapshot(incremental(relative, new ByteStreamStateHandle("private", new byte[0])));
        try (var session = new SnapshotInspector().open(directory, List.of(), List.of())) {
            var loaded = (IncrementalRemoteKeyedStateHandle) session.metadata().getOperatorStates()
                    .iterator().next().getState(0).getManagedKeyedState().iterator().next();
            var resolution = session.files().resolve(loaded.getSharedState().get(0).getHandle());
            assertEquals("PRESENT", resolution.availability());
            assertEquals(stateFile, resolution.localPath());
        }
    }

    @Test void rejectsWrongHeaderTruncatedMetadataAndUnknownVersion() throws Exception {
        Path metadata = directory.resolve("_metadata");
        Files.write(metadata, new byte[]{0, 0, 0});
        assertThrows(IOException.class, () -> new SnapshotInspector().open(directory, List.of(), List.of()));
        Files.write(metadata, new byte[8]);
        assertThrows(IOException.class, () -> new SnapshotInspector().open(directory, List.of(), List.of()));
        try (var output = new DataOutputStream(Files.newOutputStream(metadata))) {
            output.writeInt(Checkpoints.HEADER_MAGIC_NUMBER);
            output.writeInt(4);
            output.writeLong(42); // Missing collection counts and properties.
        }
        assertThrows(IOException.class, () -> new SnapshotInspector().open(directory, List.of(), List.of()));
        try (var output = new DataOutputStream(Files.newOutputStream(metadata))) {
            output.writeInt(Checkpoints.HEADER_MAGIC_NUMBER);
            output.writeInt(9999);
        }
        assertThrows(Exception.class, () -> new SnapshotInspector().open(directory, List.of(), List.of()));
    }

    @Test void includesKeyGroupsOperatorChannelAndCoordinatorReferences() throws Exception {
        var memory = new ByteStreamStateHandle("data", new byte[]{1});
        var grouped = new org.apache.flink.runtime.state.KeyGroupsStateHandle(
                new org.apache.flink.runtime.state.KeyGroupRangeOffsets(0, 0, new long[]{0}), memory);
        var operatorHandle = new org.apache.flink.runtime.state.OperatorStreamStateHandle(
                java.util.Map.of("offsets", new org.apache.flink.runtime.state.OperatorStateHandle
                        .StateMetaInfo(new long[]{0}, org.apache.flink.runtime.state.OperatorStateHandle
                        .Mode.SPLIT_DISTRIBUTE)), memory);
        var input = new org.apache.flink.runtime.state.InputChannelStateHandle(
                new org.apache.flink.runtime.checkpoint.channel.InputChannelInfo(0, 0), memory,
                List.of(0L));
        var output = new org.apache.flink.runtime.state.ResultSubpartitionStateHandle(
                new org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo(0, 0), memory,
                List.of(0L));
        var operator = new OperatorState(new OperatorID(3, 4), 1, 128);
        operator.setCoordinatorState(new ByteStreamStateHandle("coordinator", new byte[]{5}));
        operator.putState(0, OperatorSubtaskState.builder()
                .setManagedKeyedState(grouped).setRawKeyedState(grouped)
                .setManagedOperatorState(operatorHandle).setRawOperatorState(operatorHandle)
                .setInputChannelState(new org.apache.flink.runtime.checkpoint.StateObjectCollection<>(List.of(input)))
                .setResultSubpartitionState(new org.apache.flink.runtime.checkpoint.StateObjectCollection<>(List.of(output)))
                .build());
        try (var stream = Files.newOutputStream(directory.resolve("_metadata"))) {
            Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(43, List.of(operator), List.of()), stream);
        }
        try (var session = new SnapshotInspector().open(directory, List.of(), List.of())) {
            var categories = session.report().files().stream()
                    .map(SnapshotReport.StateFile::category).collect(java.util.stream.Collectors.toSet());
            assertEquals(java.util.Set.of("managed-keyed", "raw-keyed", "managed-operator", "raw-operator",
                    "input-channel", "result-subpartition", "coordinator"), categories);
            assertTrue(session.report().files().stream().allMatch(file -> "EMBEDDED".equals(file.availability())));
            assertTrue(session.report().diagnostics().stream()
                    .anyMatch(diagnostic -> diagnostic.message().contains("channel state")));
        }
    }

    private IncrementalRemoteKeyedStateHandle incremental(
            org.apache.flink.runtime.state.StreamStateHandle shared,
            org.apache.flink.runtime.state.StreamStateHandle privateState) {
        return new IncrementalRemoteKeyedStateHandle(UUID.randomUUID(), new KeyGroupRange(0, 127),
                42, List.of(IncrementalKeyedStateHandle.HandleAndLocalPath.of(shared, "00001.sst")),
                List.of(IncrementalKeyedStateHandle.HandleAndLocalPath.of(privateState, "MANIFEST")),
                new ByteStreamStateHandle("backend-meta", new byte[]{1}), 7);
    }

    private Path writeSnapshot(IncrementalRemoteKeyedStateHandle state) throws IOException {
        var operator = new OperatorState(new OperatorID(1, 2), 1, 128);
        operator.putState(0, OperatorSubtaskState.builder().setManagedKeyedState(state).build());
        var metadata = new CheckpointMetadata(42, List.of(operator), List.of(),
                CheckpointProperties.forSavepoint(false, SavepointFormatType.NATIVE));
        Path metadataPath = directory.resolve("_metadata");
        try (var output = Files.newOutputStream(metadataPath)) {
            Checkpoints.storeCheckpointMetadata(metadata, output);
        }
        return metadataPath;
    }
}
