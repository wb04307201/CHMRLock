package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonMetricsExporterTest {

    @Test
    void emptyExportProducesValidJson() {
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        String json = exp.exportToString(global, Map.of());
        // Should contain global metrics
        assertTrue(json.contains("\"global\""));
        assertTrue(json.contains("\"totalLocks\":0"));
        assertTrue(json.contains("\"perKey\""));
        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}"));
    }

    @Test
    void globalMetricsIncluded() {
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(10, 7, 3, 5000);
        String json = exp.exportToString(global, Map.of());
        assertTrue(json.contains("\"totalLocks\":10"));
        assertTrue(json.contains("\"successLocks\":7"));
        assertTrue(json.contains("\"failedLocks\":3"));
        assertTrue(json.contains("\"totalWaitTime\":5000"));
        // Computed fields
        assertTrue(json.contains("\"successRate\":0.7"));
        // 5000 / 10 = 500.0
        assertTrue(json.contains("\"avgWaitTime\":500.0"));
    }

    @Test
    void perKeyMetricsIncluded() {
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        Map<String, KeyStatistics> perKey = new LinkedHashMap<>();
        perKey.put("k1", new KeyStatistics(
                "k1", 5, 4, 1, 1_000_000L, 1000L, 2000L, 1, 42L));
        perKey.put("k2", new KeyStatistics(
                "k2", 3, 3, 0, 0L, 3000L, 4000L, 0, -1L));
        String json = exp.exportToString(global, perKey);
        assertTrue(json.contains("\"k1\""));
        assertTrue(json.contains("\"k2\""));
        assertTrue(json.contains("\"acquireCount\":5"));
        assertTrue(json.contains("\"successCount\":4"));
        assertTrue(json.contains("\"failedCount\":1"));
        assertTrue(json.contains("\"totalWaitNanos\":1000000"));
        assertTrue(json.contains("\"currentHoldCount\":1"));
        assertTrue(json.contains("\"currentHolderThreadId\":42"));
        // Computed
        assertTrue(json.contains("\"successRate\":0.8"));
        // 1_000_000 / 5 / 1_000_000 = 0.2
        assertTrue(json.contains("\"avgWaitTimeMillis\":0.2"));
    }

    @Test
    void interfaceExportWritesToAppendable() {
        StringBuilder sb = new StringBuilder();
        MetricsExporter exp = new JsonMetricsExporter(sb);
        MonitorMetrics global = new MonitorMetrics(1, 1, 0, 100);
        exp.export(global, Map.of());
        String result = sb.toString();
        assertFalse(result.isEmpty());
        assertTrue(result.contains("\"totalLocks\":1"));
    }

    @Test
    void perKeyMapHandlesNullValue() {
        // Map.of() doesn't allow null values, but real callers might pass a null
        // if they wired metrics wrong. The exporter should not NPE — log and skip.
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        Map<String, KeyStatistics> perKey = new LinkedHashMap<>();
        perKey.put("k1", null);
        String json = exp.exportToString(global, perKey);
        // Should not throw, should produce valid JSON
        assertTrue(json.startsWith("{"));
    }
}
