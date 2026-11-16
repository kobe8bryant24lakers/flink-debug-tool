package dev.flinkdebug.core;

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A single private RocksDB copy and forward page anchor, owned by an open SnapshotSession. */
final class PreviewRocksCache implements AutoCloseable {
    record Query(String operatorId, int subtask, PreviewRequest firstPage) {
        static Query of(String operatorId, int subtask, PreviewRequest request) {
            return new Query(operatorId, subtask, new PreviewRequest(request.limit(), 0, request.stateName(),
                    request.keyContains(), request.valueContains(), request.mapKeyContains(), request.timerFrom(), request.timerTo()));
        }
    }

    record Position(int family, byte[] key) {}
    record Continuation(Query query, long offset, Position position, List<String> coverageGaps) {}
    record SourceFile(String logicalPath, Path localPath, long size, FileTime modified, Object fileKey) {}

    private IncrementalRemoteKeyedStateHandle handle;
    private List<SourceFile> sourceFiles;
    private Path directory;
    private Continuation continuation;

    boolean owns(IncrementalRemoteKeyedStateHandle candidate) { return directory != null && handle == candidate; }
    boolean current(List<SourceFile> stamps) { return directory != null && sourceFiles.equals(stamps); }
    Path directory() { return directory; }

    Continuation resume(Query query, long offset) {
        if (continuation != null && continuation.offset() == offset && continuation.query().equals(query)) return continuation;
        return null;
    }

    void publish(IncrementalRemoteKeyedStateHandle handle, List<SourceFile> sourceFiles, Path directory,
                 Continuation continuation) {
        this.handle = handle;
        this.sourceFiles = sourceFiles;
        this.directory = directory;
        this.continuation = continuation;
    }

    static List<SourceFile> fingerprint(SnapshotSession session, IncrementalRemoteKeyedStateHandle handle,
                                        List<HandleAndLocalPath> files) throws IOException, InterruptedException {
        List<SourceFile> result = new ArrayList<>(files.size() + 1);
        result.add(stamp(session, handle.getMetaDataStateHandle()));
        for (HandleAndLocalPath file : files) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("State preview cancelled");
            result.add(stamp(session, file.getHandle()));
        }
        return List.copyOf(result);
    }

    private static SourceFile stamp(SnapshotSession session, StreamStateHandle handle) throws IOException {
        var resolved = session.files().resolve(handle);
        if ("EMBEDDED".equals(resolved.availability())) {
            return new SourceFile(resolved.logicalPath(), null, resolved.actualBytes(), null, null);
        }
        if (!"PRESENT".equals(resolved.availability())) {
            throw new IOException("RocksDB source file " + resolved.availability() + ": " + resolved.logicalPath());
        }
        Path local = resolved.localPath();
        BasicFileAttributes attrs = Files.readAttributes(local, BasicFileAttributes.class);
        if (!attrs.isRegularFile()) throw new IOException("RocksDB source is no longer a regular file: " + local);
        return new SourceFile(resolved.logicalPath(), local, attrs.size(), attrs.lastModifiedTime(), attrs.fileKey());
    }

    @Override public void close() throws IOException {
        Path remove = directory;
        handle = null;
        sourceFiles = null;
        directory = null;
        continuation = null;
        if (remove != null) deleteTree(remove);
    }

    static void deleteTree(Path directory) throws IOException {
        try (var files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
        } catch (NoSuchFileException absent) {
            if (Files.exists(directory)) throw absent;
        }
    }
}
