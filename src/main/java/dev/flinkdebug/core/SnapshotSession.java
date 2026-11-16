package dev.flinkdebug.core;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;

import java.io.IOException;
import java.net.URLClassLoader;

/** Holds a read-only metadata view, the optional user-code classloader, and private preview resources. */
public final class SnapshotSession implements AutoCloseable {
    private final CheckpointMetadata metadata;
    private final SnapshotReport report;
    private final LocalStateFiles files;
    private final URLClassLoader classLoader;
    private final PreviewRocksCache previewRocksCache = new PreviewRocksCache();
    private boolean closed;

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
    PreviewRocksCache previewRocksCache() { return previewRocksCache; }
    void requireOpen() { if (closed) throw new IllegalStateException("The snapshot session is closed"); }

    @Override public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        // Metadata.dispose()/StateHandle.discardState() delete snapshot files: never call them here.
        try (classLoader; previewRocksCache) {
            // Close the private copy before the classloader; preserve any cleanup failure.
        }
    }
}
