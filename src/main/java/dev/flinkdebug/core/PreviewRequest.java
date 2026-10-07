package dev.flinkdebug.core;

/** A page of matching records. Text filters match the complete decoded fields, case sensitively. */
public record PreviewRequest(int limit, long offset, String stateName, String keyContains,
                             String valueContains, String mapKeyContains, Long timerFrom, Long timerTo) {
    public PreviewRequest {
        if (limit < 1 || limit > DataPreviewService.MAX_ENTRIES) {
            throw new IllegalArgumentException("Page size must be between 1 and " + DataPreviewService.MAX_ENTRIES);
        }
        if (offset < 0) throw new IllegalArgumentException("Page offset cannot be negative");
        if (timerFrom != null && timerTo != null && timerFrom > timerTo) {
            throw new IllegalArgumentException("Timer start timestamp must not exceed end timestamp");
        }
        stateName = absentIfBlank(stateName);
        keyContains = absentIfBlank(keyContains);
        valueContains = absentIfBlank(valueContains);
        mapKeyContains = absentIfBlank(mapKeyContains);
    }

    private static String absentIfBlank(String value) { return value == null || value.isBlank() ? null : value; }
}
