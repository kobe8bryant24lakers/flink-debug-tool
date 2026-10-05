package dev.flinkdebug.core;

import java.nio.file.Path;

/** Explicit relocation from an original URI prefix to a downloaded local directory. */
public record PathMapping(String originalPrefix, Path localDirectory) {}
