package dev.flinkdebug.core;

import dev.flinkdebug.demo.ExampleSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static dev.flinkdebug.core.LocalStreamQueryService.*;
import static org.junit.jupiter.api.Assertions.*;

class LocalStreamQueryTest {
    @TempDir Path temporary;

    @Test void queriesRealRocksDbGeneratedSavepointWithoutChangingSource() throws Exception {
        Path snapshot = temporary.resolve("savepoint");
        ExampleSnapshot.create(snapshot);
        Map<String, String> original = hashes(snapshot);
        try (var session = new SnapshotInspector().open(snapshot, List.of(), List.of())) {
            String operator = session.report().operators().get(0).operatorId();
            var service = new LocalStreamQueryService();
            var values = service.query(session, spec(operator, "order-count", Kind.VALUE, 20));
            assertEquals(Map.of("\"alice\"", "12", "\"bob\"", "3"),
                    values.rows().stream().collect(Collectors.toMap(QueryRow::key, QueryRow::value)));
            assertFalse(values.truncated());
            assertThrows(IllegalArgumentException.class, () -> service.query(session, spec(operator, "missing-state", Kind.VALUE, 20)));
            assertThrows(IllegalArgumentException.class, () -> service.query(session, spec(operator, "order-count", Kind.LIST, 20)));
            var lists = service.query(session, spec(operator, "order-values", Kind.LIST, 20));
            assertEquals(2, lists.rows().size());
            assertTrue(lists.rows().stream().anyMatch(row -> row.key().equals("\"alice\"") && row.value().equals("[7,5]")));
            var maps = service.query(session, spec(operator, "order-map", Kind.MAP, 20));
            assertTrue(maps.rows().stream().anyMatch(row -> row.key().equals("\"alice\"") && row.value().contains("\"value\":5")));
            var bounded = service.query(session, spec(operator, "order-count", Kind.VALUE, 1));
            assertEquals(1, bounded.rows().size());
            assertTrue(bounded.truncated());
            assertEquals(original, hashes(snapshot));
        }
    }

    @Test void refusesInvalidOutputLimitsAndOperatorIds() {
        assertThrows(IllegalArgumentException.class, () -> spec("bad", "count", Kind.VALUE, 10));
        assertThrows(IllegalArgumentException.class, () -> spec("0".repeat(32), "count", Kind.VALUE, 10001));
        assertThrows(IllegalArgumentException.class, () -> spec("0".repeat(32), "", Kind.VALUE, 10));
    }

    private QuerySpec spec(String operator, String state, Kind kind, int limit) {
        return new QuerySpec(operator, state, kind, ScalarType.STRING, ScalarType.LONG, ScalarType.STRING, limit);
    }

    private static Map<String, String> hashes(Path directory) throws Exception {
        var hashes = new TreeMap<String, String>();
        try (var files = Files.walk(directory)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                hashes.put(directory.relativize(file).toString(), HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
            }
        }
        return hashes;
    }
}
