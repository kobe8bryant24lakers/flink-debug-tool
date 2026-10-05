package dev.flinkdebug.core;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LocalStateFilesTest {
    @TempDir Path directory;

    @Test void mappingUsesLongestDirectoryPrefixAndNeverGuessesBasename() throws Exception {
        Path generic = Files.createDirectory(directory.resolve("generic"));
        Path specific = Files.createDirectory(directory.resolve("specific"));
        Files.write(specific.resolve("key.sst"), new byte[]{1});
        Files.write(directory.resolve("unmapped.sst"), new byte[]{1});
        var files = new LocalStateFiles(directory, List.of(
                new PathMapping("s3://bucket/job", generic),
                new PathMapping("s3://bucket/job/shared/", specific)));
        var mapped = file("s3://bucket/job/shared/key.sst", 1);
        assertEquals(specific.resolve("key.sst"), files.resolve(mapped).localPath());
        assertEquals("PRESENT", files.resolve(mapped).availability());
        assertEquals("UNMAPPED", files.resolve(file("s3://bucket/job-other/unmapped.sst", 1))
                .availability());
        assertThrows(IOException.class, () -> files.open(file("s3://bucket/unmapped.sst", 1)));
    }

    @Test void rejectsRelativeTraversalAndMappedSymlinkEscape() throws Exception {
        Path root = Files.createDirectory(directory.resolve("snapshot"));
        Path outside = Files.createDirectory(directory.resolve("outside"));
        Files.write(outside.resolve("secret.sst"), new byte[]{9});
        Files.createSymbolicLink(root.resolve("escape"), outside);
        var files = new LocalStateFiles(root, List.of(new PathMapping("s3://bucket/job", root)));
        var relative = new RelativeFileStateHandle(new org.apache.flink.core.fs.Path(
                root.resolve("state").toUri()), "../outside/secret.sst", 1);
        assertEquals("REJECTED", files.resolve(relative).availability());
        assertEquals("REJECTED", files.resolve(file("s3://bucket/job/escape/secret.sst", 1))
                .availability());
        assertThrows(IOException.class, () -> files.open(relative));
    }

    @Test void encodedTraversalCannotEscapeAMapping() {
        var files = new LocalStateFiles(directory,
                List.of(new PathMapping("s3://bucket/job", directory)));
        var handle = new FileStateHandle(new org.apache.flink.core.fs.Path(
                URI.create("s3://bucket/job/%2e%2e/secret.sst")), 1);
        assertEquals("REJECTED", files.resolve(handle).availability());
    }

    @Test void decodedSpacesAndLiteralPercentCharactersKeepTheirExactFilename() throws Exception {
        Files.write(directory.resolve("state file.sst"), new byte[]{1});
        Files.write(directory.resolve("%2e.sst"), new byte[]{2});
        var files = new LocalStateFiles(directory,
                List.of(new PathMapping("s3://bucket/job%20with%20space/", directory)));
        var withSpace = new FileStateHandle(new org.apache.flink.core.fs.Path(
                URI.create("s3://bucket/job%20with%20space/state%20file.sst")), 1);
        var percent = new FileStateHandle(new org.apache.flink.core.fs.Path(
                URI.create("s3://bucket/job%20with%20space/%252e.sst")), 1);
        assertEquals("PRESENT", files.resolve(withSpace).availability());
        assertEquals(directory.resolve("%2e.sst"), files.resolve(percent).localPath());
    }

    @Test void mergedFileStreamsAreBoundedToTheirLogicalSegment() throws Exception {
        Path physical = directory.resolve("merged");
        Files.write(physical, new byte[]{0, 1, 2, 3, 4});
        var segment = new org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle(
                new org.apache.flink.core.fs.Path(physical.toUri()), 1, 3,
                org.apache.flink.runtime.state.CheckpointedStateScope.SHARED,
                new org.apache.flink.runtime.checkpoint.filemerging.LogicalFile.LogicalFileId("slice"));
        var files = new LocalStateFiles(directory, List.of());
        assertEquals("PRESENT", files.resolve(segment).availability());
        try (var stream = files.open(segment)) {
            assertArrayEquals(new byte[]{1, 2, 3}, stream.readAllBytes());
            stream.seek(2);
            assertEquals(3, stream.read());
            assertEquals(-1, stream.read());
        }
        Files.write(physical, new byte[]{0, 1});
        assertEquals("SIZE_MISMATCH", files.resolve(segment).availability());
        assertThrows(IOException.class, () -> files.open(segment));
    }

    @Test void emptyMergedStateIsASentinelRatherThanAMissingFile() throws Exception {
        var files = new LocalStateFiles(directory, List.of());
        var empty = org.apache.flink.runtime.state.filemerging.EmptySegmentFileStateHandle.INSTANCE;
        assertEquals("EMBEDDED", files.resolve(empty).availability());
        try (var stream = files.open(empty)) { assertEquals(-1, stream.read()); }
    }

    @Test void refusesDamagedFilesAndCanSeekInEmbeddedAndLocalStreams() throws Exception {
        Path state = directory.resolve("state");
        Files.write(state, new byte[]{1, 2, 3});
        var files = new LocalStateFiles(directory, List.of());
        var damaged = new FileStateHandle(new org.apache.flink.core.fs.Path(state.toUri()), 10);
        assertEquals("SIZE_MISMATCH", files.resolve(damaged).availability());
        assertEquals(3L, files.resolve(damaged).actualBytes());
        assertThrows(IOException.class, () -> files.open(damaged));
        for (StreamStateHandle handle : List.of(
                new ByteStreamStateHandle("memory", new byte[]{1, 2, 3}),
                new FileStateHandle(new org.apache.flink.core.fs.Path(state.toUri()), 3))) {
            try (var stream = files.open(handle)) {
                stream.seek(2);
                assertEquals(3, stream.read());
                assertEquals(-1, stream.read());
                assertEquals(0, stream.read(new byte[0], 0, 0));
                assertThrows(IOException.class, () -> stream.seek(4));
                stream.seek(0);
                assertArrayEquals(new byte[]{1, 2, 3}, stream.readAllBytes());
            }
        }
    }

    @Test void unknownHandlesNeverGetAnOpportunityToOpenOrDiscardState() {
        var files = new LocalStateFiles(directory, List.of());
        var unknown = new UnknownHandle();
        assertEquals("UNSUPPORTED", files.resolve(unknown).availability());
        assertThrows(IOException.class, () -> files.open(unknown));
        assertFalse(unknown.opened);
        assertFalse(unknown.discarded);
    }

    private FileStateHandle file(String uri, long bytes) {
        return new FileStateHandle(new org.apache.flink.core.fs.Path(uri), bytes);
    }

    private static final class UnknownHandle implements StreamStateHandle {
        private boolean opened;
        private boolean discarded;
        @Override public FSDataInputStream openInputStream() {
            opened = true;
            throw new AssertionError("Unrecognized handles must not be opened");
        }
        @Override public Optional<byte[]> asBytesIfInMemory() { return Optional.empty(); }
        @Override public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("unknown");
        }
        @Override public void discardState() { discarded = true; }
        @Override public long getStateSize() { return 0; }
    }
}
