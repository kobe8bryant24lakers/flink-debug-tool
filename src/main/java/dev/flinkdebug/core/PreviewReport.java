package dev.flinkdebug.core;

import java.util.List;

/** A bounded sample, never a complete count of the state. */
public record PreviewReport(List<StateSchema> schemas, List<StateEntry> entries,
                            boolean truncated, List<String> warnings) {
    public record StateSchema(String name, String type, String keySerializer,
                               String namespaceSerializer, String valueSerializer) {}
    public record StateEntry(String stateName, int keyGroup, String key, String namespace,
                              String value, String keyHex, String valueHex, String decodeStatus) {}
}
