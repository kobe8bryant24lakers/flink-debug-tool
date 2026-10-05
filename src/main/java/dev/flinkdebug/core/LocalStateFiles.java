package dev.flinkdebug.core;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.EmptySegmentFileStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/** Resolves downloaded state without consulting Flink filesystem plugins or the network. */
public final class LocalStateFiles {
    public record Resolution(String logicalPath, Path localPath, Long actualBytes,
                             String availability) {}

    private final Path snapshotDirectory;
    private final List<PathMapping> mappings;

    public LocalStateFiles(Path snapshotDirectory, List<PathMapping> mappings) {
        this.snapshotDirectory = snapshotDirectory.toAbsolutePath().normalize();
        this.mappings = mappings.stream().map(mapping -> {
            if (mapping.originalPrefix() == null || mapping.originalPrefix().isBlank()
                    || mapping.localDirectory() == null) {
                throw new IllegalArgumentException("路径映射必须包含原始前缀和本地目录");
            }
            String suppliedPrefix = mapping.originalPrefix().trim();
            org.apache.flink.core.fs.Path original;
            try {
                original = new org.apache.flink.core.fs.Path(URI.create(suppliedPrefix));
            } catch (IllegalArgumentException exception) {
                // Flink prints paths decoded, so a copied prefix can contain spaces.
                original = new org.apache.flink.core.fs.Path(suppliedPrefix);
            }
            if (original.toUri().getQuery() != null || original.toUri().getFragment() != null) {
                throw new IllegalArgumentException("映射前缀不能包含 query 或 fragment");
            }
            rejectTraversal(original.toUri().getPath());
            String prefix = stripTrailingSlash(original.toString());
            return new PathMapping(prefix, mapping.localDirectory().toAbsolutePath().normalize());
        }).sorted(Comparator.comparingInt((PathMapping mapping) -> mapping.originalPrefix().length())
                .reversed()).toList();
    }

    public Resolution resolve(StreamStateHandle handle) {
        if (handle instanceof EmptySegmentFileStateHandle) {
            return new Resolution("embedded:empty-segment", null, 0L, "EMBEDDED");
        }
        if (handle instanceof ByteStreamStateHandle memory) {
            return new Resolution("embedded:" + memory.getHandleName(), null,
                    (long) memory.getData().length, "EMBEDDED");
        }
        String logicalPath;
        Path localPath = null;
        long offset = 0;
        try {
            if (handle instanceof RelativeFileStateHandle relative) {
                logicalPath = relative.getFilePath().toString();
                localPath = containedPath(snapshotDirectory, relative.getRelativePath());
            } else if (handle instanceof FileStateHandle file) {
                logicalPath = file.getFilePath().toString();
                localPath = relocate(file.getFilePath().toUri(), logicalPath);
            } else if (handle instanceof SegmentFileStateHandle segment) {
                logicalPath = segment.getFilePath().toString();
                offset = segment.getStartPos();
                localPath = relocate(segment.getFilePath().toUri(), logicalPath);
            } else {
                return new Resolution(handle.getClass().getName(), null, null, "UNSUPPORTED");
            }
            if (localPath == null) {
                return new Resolution(logicalPath, null, null, "UNMAPPED");
            }
            if (!Files.exists(localPath)) {
                return new Resolution(logicalPath, localPath, null, "MISSING");
            }
            if (!Files.isRegularFile(localPath) || !Files.isReadable(localPath)) {
                return new Resolution(logicalPath, localPath, null, "UNREADABLE");
            }
            long actualBytes = Files.size(localPath);
            if (handle instanceof SegmentFileStateHandle) {
                // The handle represents a slice, not the whole merged physical file.
                actualBytes = Math.min(Math.max(0, actualBytes - offset), handle.getStateSize());
            }
            String availability = offset < 0 || handle.getStateSize() < 0
                    || actualBytes != handle.getStateSize() ? "SIZE_MISMATCH" : "PRESENT";
            return new Resolution(logicalPath, localPath, actualBytes, availability);
        } catch (IllegalArgumentException exception) {
            return new Resolution(logicalName(handle), localPath, null, "REJECTED");
        } catch (IOException | SecurityException exception) {
            return new Resolution(logicalName(handle), localPath, null, "UNREADABLE");
        }
    }

    /** Opens only known local handles; never calls an arbitrary handle's openInputStream(). */
    public FSDataInputStream open(StreamStateHandle handle) throws IOException {
        if (handle instanceof EmptySegmentFileStateHandle) return new MemoryInput(new byte[0]);
        if (handle instanceof ByteStreamStateHandle memory) {
            return new MemoryInput(memory.getData());
        }
        Resolution resolution = resolve(handle);
        if (!"PRESENT".equals(resolution.availability())) {
            throw new IOException("无法离线读取状态文件: " + resolution.availability()
                    + " " + resolution.logicalPath());
        }
        long offset = handle instanceof SegmentFileStateHandle segment ? segment.getStartPos() : 0;
        return new LocalInput(resolution.localPath(), offset, handle.getStateSize());
    }

    private Path relocate(URI uri, String logicalPath) throws IOException {
        rejectTraversal(uri.getPath());
        for (PathMapping mapping : mappings) {
            String prefix = mapping.originalPrefix();
            String directoryPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
            if (logicalPath.equals(prefix) || logicalPath.startsWith(directoryPrefix)) {
                String suffix = logicalPath.substring(prefix.length());
                while (suffix.startsWith("/")) suffix = suffix.substring(1);
                // Both Flink's toString() and the stored prefix are already decoded once.
                return containedPath(mapping.localDirectory(), suffix);
            }
        }
        String scheme = uri.getScheme();
        if (scheme == null) {
            Path path = Path.of(uri.getPath());
            return path.isAbsolute() ? path.normalize() : containedPath(snapshotDirectory, uri.getPath());
        }
        if ("file".equalsIgnoreCase(scheme)
                && (uri.getAuthority() == null || uri.getAuthority().isEmpty())) {
            return Path.of(uri).toAbsolutePath().normalize();
        }
        return null;
    }

    private static Path containedPath(Path directory, String suffix) throws IOException {
        rejectTraversal(suffix);
        Path relative = Path.of(suffix);
        if (relative.isAbsolute()) throw new IllegalArgumentException("映射路径必须是相对路径");
        Path resolved = directory.resolve(relative).normalize();
        if (!resolved.startsWith(directory)) throw new IllegalArgumentException("路径越过映射目录");
        // Check an existing parent too: a missing filename below a symlink must not escape the root.
        Path existing = resolved;
        while (existing != null && !Files.exists(existing)) existing = existing.getParent();
        if (Files.exists(directory) && existing != null
                && !existing.toRealPath().startsWith(directory.toRealPath())) {
            throw new IllegalArgumentException("符号链接越过映射目录");
        }
        return resolved;
    }

    private static void rejectTraversal(String path) {
        if (path == null || path.indexOf('\\') >= 0 || path.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("无效状态文件路径");
        }
        for (String component : path.split("/", -1)) {
            if ("..".equals(component) || ".".equals(component)) {
                throw new IllegalArgumentException("状态文件路径包含目录穿越");
            }
        }
    }

    private static String stripTrailingSlash(String prefix) {
        while (prefix.endsWith("/") && prefix.length() > 1
                && !prefix.matches("[a-zA-Z][a-zA-Z0-9+.-]*:/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix;
    }

    private static String logicalName(StreamStateHandle handle) {
        if (handle instanceof FileStateHandle file) return file.getFilePath().toString();
        if (handle instanceof SegmentFileStateHandle segment) return segment.getFilePath().toString();
        return handle.getClass().getName();
    }

    private static final class LocalInput extends FSDataInputStream {
        private final RandomAccessFile file;
        private final long offset;
        private final long length;
        private long position;

        private LocalInput(Path path, long offset, long length) throws IOException {
            this.file = new RandomAccessFile(path.toFile(), "r");
            this.offset = offset;
            this.length = length;
            file.seek(offset);
        }

        @Override public void seek(long desired) throws IOException {
            if (desired < 0 || desired > length) throw new IOException("状态文件 seek 超出范围");
            file.seek(offset + desired);
            position = desired;
        }
        @Override public long getPos() { return position; }
        @Override public int read() throws IOException {
            if (position >= length) return -1;
            int value = file.read();
            if (value >= 0) position++;
            return value;
        }
        @Override public int read(byte[] bytes, int start, int count) throws IOException {
            java.util.Objects.checkFromIndexSize(start, count, bytes.length);
            if (count == 0) return 0;
            if (position >= length) return -1;
            int read = file.read(bytes, start, (int) Math.min(count, length - position));
            if (read > 0) position += read;
            return read;
        }
        @Override public int available() { return (int) Math.min(Integer.MAX_VALUE, length - position); }
        @Override public void close() throws IOException { file.close(); }
    }

    private static final class MemoryInput extends FSDataInputStream {
        private final byte[] bytes;
        private int position;
        private MemoryInput(byte[] bytes) { this.bytes = bytes; }
        @Override public void seek(long desired) throws IOException {
            if (desired < 0 || desired > bytes.length) throw new IOException("内嵌状态 seek 超出范围");
            position = (int) desired;
        }
        @Override public long getPos() { return position; }
        @Override public int read() { return position < bytes.length ? bytes[position++] & 0xff : -1; }
        @Override public int read(byte[] target, int start, int count) {
            java.util.Objects.checkFromIndexSize(start, count, target.length);
            if (count == 0) return 0;
            if (position >= bytes.length) return -1;
            int read = Math.min(count, bytes.length - position);
            System.arraycopy(bytes, position, target, start, read);
            position += read;
            return read;
        }
        @Override public int available() { return bytes.length - position; }
        @Override public void close() {}
    }
}
