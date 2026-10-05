package dev.flinkdebug.core;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;

import java.io.IOException;
import java.net.URLClassLoader;

/** Holds a read-only metadata view and the optional user-code classloader. */
public final class SnapshotSession implements AutoCloseable {
    private final CheckpointMetadata metadata;
    private final SnapshotReport report;
    private final LocalStateFiles files;
    private final URLClassLoader classLoader;

    SnapshotSession(CheckpointMetadata metadata, SnapshotReport report,
                    LocalStateFiles files, URLClassLoader classLoader) {
        this.metadata = metadata;
        this.report = report;
        this.files = files;
        this.classLoader = classLoader;
    }

    public CheckpointMetadata metadata() { return metadata; }
    public SnapshotReport report() { return report; }
    public LocalStateFiles files() { return files; }
    public ClassLoader classLoader() { return classLoader; }

    @Override public void close() throws IOException {
        // Metadata.dispose()/StateHandle.discardState() delete snapshot files: never call them here.
        classLoader.close();
    }
}
