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

    @Test
    void keyWithSpecialCharsIsEscaped() {
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        Map<String, KeyStatistics> perKey = new LinkedHashMap<>();
        // Key with quote, backslash, newline, tab, and a control char (U+0001)
        perKey.put("k\"\\\n\t", new KeyStatistics(
                "k\"\\\n\t", 1, 1, 0, 0L, 0L, 0L, 0, -1L));
        String json = exp.exportToString(global, perKey);
        // Verify all control chars are properly escaped per RFC 8259
        assertTrue(json.contains("\\\""));
        assertTrue(json.contains("\\\\"));
        assertTrue(json.contains("\\n"));
        assertTrue(json.contains("\\t"));
        assertTrue(json.contains("\\u0001"));
        // Should NOT contain raw control char bytes
        assertFalse(json.contains(""));
    }

    @Test
    void multiKeyPreservesInsertionOrder() {
        JsonMetricsExporter exp = new JsonMetricsExporter();
        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        // LinkedHashMap preserves insertion order
        Map<String, KeyStatistics> perKey = new LinkedHashMap<>();
        perKey.put("zebra", new KeyStatistics("zebra", 0, 0, 0, 0, 0, 0, 0, -1));
        perKey.put("alpha", new KeyStatistics("alpha", 0, 0, 0, 0, 0, 0, 0, -1));
        perKey.put("mango", new KeyStatistics("mango", 0, 0, 0, 0, 0, 0, 0, -1));
        String json = exp.exportToString(global, perKey);
        int zebraIdx = json.indexOf("\"zebra\"");
        int alphaIdx = json.indexOf("\"alpha\"");
        int mangoIdx = json.indexOf("\"mango\"");
        assertTrue(zebraIdx > 0, "zebra should appear in JSON");
        assertTrue(alphaIdx > 0, "alpha should appear in JSON");
        assertTrue(mangoIdx > 0, "mango should appear in JSON");
        // Insertion order: zebra, alpha, mango
        assertTrue(zebraIdx < alphaIdx, "zebra should appear before alpha (insertion order)");
        assertTrue(alphaIdx < mangoIdx, "alpha should appear before mango (insertion order)");
    }
}
