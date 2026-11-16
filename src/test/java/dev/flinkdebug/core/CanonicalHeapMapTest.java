package dev.flinkdebug.core;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CanonicalHeapMapTest {
    @TempDir Path temporary;

    @Test void readsNullMapEntryFromFlinksCanonicalHeapSavepoint() throws Exception {
        Configuration config = new Configuration();
        for (String option : List.of("jobmanager.bind-host", "jobmanager.rpc.address", "taskmanager.bind-host",
                "taskmanager.host", "rest.address", "rest.bind-address")) {
            config.setString(option, "127.0.0.1");
        }
        config.setString("rest.bind-port", "0");
        config.setString("taskmanager.memory.managed.size", "128m");
        config.setString("taskmanager.memory.network.min", "16m");
        config.setString("taskmanager.memory.network.max", "16m");
        var environment = StreamExecutionEnvironment.createLocalEnvironment(config);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment.setParallelism(1);
        environment.getConfig().disableClosureCleaner();
        environment.getConfig().enableObjectReuse();
        var bootstrap = OperatorTransformation.bootstrapWith(environment.fromElements("fixture-key"))
                .setMaxParallelism(128).keyBy(key -> key, Types.STRING).transform(new NullMapBootstrap());
        Path savepoint = temporary.resolve("heap generated savepoint");
        String output = new org.apache.flink.core.fs.Path(savepoint.toUri()).toString();
        SavepointWriter.newSavepoint(environment, new HashMapStateBackend(), 128)
                .withOperator(OperatorIdentifier.forUid("fixture-map"), bootstrap).write(output);
        environment.execute("Canonical heap MapState validation");

        try (var session = new SnapshotInspector().open(savepoint, List.of(), List.of())) {
            String operator = session.report().operators().get(0).operatorId();
            var report = new DataPreviewService().preview(session, operator, 0,
                    new PreviewRequest(10, 0, "fixture-map", "fixture-key", null, null, null, null));
            assertTrue(report.page().complete(), () -> report.warnings().toString());
            assertFalse(report.page().hasMore());
            var values = report.entries().stream().collect(Collectors.toMap(
                    PreviewReport.StateEntry::mapKey, PreviewReport.StateEntry::value));
            assertEquals(Map.of("unassigned", "null", "assigned", "example"), values);
            var missingValue = report.entries().stream().filter(row -> row.mapKey().equals("unassigned")).findFirst().orElseThrow();
            assertEquals("0100", missingValue.valueHex(), "Flink heap wrote the null flag and StringSerializer's null payload");
        }
    }

    public static final class NullMapBootstrap extends KeyedStateBootstrapFunction<String, String> {
        private transient MapState<String, String> state;

        @Override public void open(OpenContext context) {
            state = getRuntimeContext().getMapState(new MapStateDescriptor<>("fixture-map", Types.STRING, Types.STRING));
        }

        @Override public void processElement(String key, Context context) throws Exception {
            state.put("unassigned", null);
            state.put("assigned", "example");
        }
    }
}
