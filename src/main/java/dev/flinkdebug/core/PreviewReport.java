package dev.flinkdebug.core;

import java.util.List;

/** One page of a snapshot query; exhausted and complete pages establish a complete enumeration. */
public record PreviewReport(List<StateSchema> schemas, List<StateEntry> entries,
                            boolean truncated, List<String> warnings, PageInfo page) {
    public PreviewReport(List<StateSchema> schemas, List<StateEntry> entries,
                         boolean truncated, List<String> warnings) {
        this(schemas, entries, truncated, warnings,
                new PageInfo(0, entries.size(), entries.size(), truncated, !truncated && warnings.isEmpty()));
    }

    /** Complete describes reader coverage, independently of whether another page exists. */
    public record PageInfo(long offset, long nextOffset, long scanned, boolean hasMore, boolean complete) {}

    public record StateSchema(String name, String type, String keySerializer,
                               String namespaceSerializer, String valueSerializer) {}
    public record StateEntry(String stateName, int keyGroup, String key, String namespace,
                              String value, String keyHex, String valueHex, String decodeStatus,
                              String mapKey, Long ttlTimestamp, Long timerTimestamp, String timerType) {
        public StateEntry(String stateName, int keyGroup, String key, String namespace,
                          String value, String keyHex, String valueHex, String decodeStatus,
                          String mapKey, Long ttlTimestamp) {
            this(stateName, keyGroup, key, namespace, value, keyHex, valueHex, decodeStatus,
                    mapKey, ttlTimestamp, null, null);
        }
    }
}
