package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockExportMetricsTest {

    @Test
    void exportMetricsInvokesExporter() {
        try (CHMRLock lock = new CHMRLock(
                CHMRLockConfig.builder().enablePerKeyMetrics(true).build())) {
            lock.tryLock("k1");
            lock.unlock("k1");

            List<String> calls = new ArrayList<>();
            MetricsExporter exporter = (global, perKey) -> {
                calls.add("global.total=" + global.getTotalLocks());
                calls.add("perKey.size=" + perKey.size());
            };

            lock.exportMetrics(exporter);

            assertEquals(2, calls.size());
            assertTrue(calls.get(0).contains("global.total=1"));
            assertTrue(calls.get(1).contains("perKey.size=1"));
        }
    }

    @Test
    void exportMetricsSwallowsExporterException() {
        try (CHMRLock lock = new CHMRLock()) {
            MetricsExporter bad = (global, perKey) -> {
                throw new RuntimeException("export failed");
            };
            // Should not throw
            assertDoesNotThrow(() -> lock.exportMetrics(bad));
        }
    }

    @Test
    void exportMetricsNullExporterThrows() {
        try (CHMRLock lock = new CHMRLock()) {
            assertThrows(NullPointerException.class, () -> lock.exportMetrics(null));
        }
    }

    @Test
    void exportMetricsCapturesEmptyStats() {
        try (CHMRLock lock = new CHMRLock()) {
            AtomicInteger callCount = new AtomicInteger();
            lock.exportMetrics((global, perKey) -> {
                callCount.incrementAndGet();
                assertEquals(0, global.getTotalLocks());
                assertEquals(0, perKey.size());
            });
            assertEquals(1, callCount.get());
        }
    }

    @Test
    void jsonExporterCanBeTriggered() {
        try (CHMRLock lock = new CHMRLock(
                CHMRLockConfig.builder().enablePerKeyMetrics(true).build())) {
            lock.tryLock("resource_id");
            // Just verify it runs without exception; we don't capture output here
            assertDoesNotThrow(() -> lock.exportMetrics(new JsonMetricsExporter()));
            lock.unlock("resource_id");
        }
    }
}
