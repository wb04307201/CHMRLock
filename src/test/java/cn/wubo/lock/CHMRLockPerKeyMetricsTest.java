package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockPerKeyMetricsTest {

    private CHMRLock newWithMetrics() {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .build();
        return new CHMRLock(cfg);
    }

    @Test
    void perKeyCountersUpdatedOnTryLock() {
        try (CHMRLock lock = newWithMetrics()) {
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            KeyStatistics ks = lock.getStatistics("k1").orElseThrow();
            assertEquals("k1", ks.key());
            assertEquals(1L, ks.acquireCount());
            assertEquals(1L, ks.successCount());
            assertEquals(0L, ks.failedCount());
        }
    }

    @Test
    void perKeyCountersOnFailedTryLock() throws Exception {
        try (CHMRLock lock = newWithMetrics()) {
            assertTrue(lock.tryLock("k1"));
            // In another thread, attempt tryLock with short wait and verify it fails
            Thread contender = new Thread(() -> {
                assertFalse(lock.tryLock("k1", 50));
            });
            contender.start();
            contender.join();
            // Unlock from main thread
            lock.unlock("k1");
            KeyStatistics ks = lock.getStatistics("k1").orElseThrow();
            // Both attempts should be recorded
            assertEquals(2L, ks.acquireCount());
            assertEquals(1L, ks.successCount());
            assertEquals(1L, ks.failedCount());
            assertTrue(ks.totalWaitNanos() > 0, "wait nanos should accumulate");
            // success rate = 0.5
            assertEquals(0.5, ks.getSuccessRate(), 0.001);
        }
    }

    @Test
    void lastReleaseTimeSetOnUnlock() {
        try (CHMRLock lock = newWithMetrics()) {
            assertTrue(lock.tryLock("k1"));
            long beforeRelease = System.currentTimeMillis();
            lock.unlock("k1");
            KeyStatistics ks = lock.getStatistics("k1").orElseThrow();
            assertTrue(ks.lastReleaseEpochMs() >= beforeRelease, "lastRelease should be >= before-release time");
        }
    }

    @Test
    void getStatisticsForUnknownKeyReturnsEmpty() {
        try (CHMRLock lock = newWithMetrics()) {
            Optional<KeyStatistics> r = lock.getStatistics("never-locked");
            assertTrue(r.isEmpty());
        }
    }

    @Test
    void getAllStatisticsReturnsAllEverLockedKeys() {
        try (CHMRLock lock = newWithMetrics()) {
            lock.tryLock("a");
            lock.unlock("a");
            lock.tryLock("b");
            lock.unlock("b");
            Map<String, KeyStatistics> all = lock.getAllStatistics();
            assertEquals(2, all.size());
            assertTrue(all.containsKey("a"));
            assertTrue(all.containsKey("b"));
            assertEquals(1, all.get("a").acquireCount());
        }
    }

    @Test
    void getAllStatisticsEmptyByDefault() {
        try (CHMRLock lock = newWithMetrics()) {
            assertTrue(lock.getAllStatistics().isEmpty());
        }
    }

    @Test
    void perKeyMetricsDisabledByDefault() {
        // Default config has enablePerKeyMetrics=false
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            // Should return empty (metrics disabled)
            assertTrue(lock.getStatistics("k1").isEmpty());
            assertTrue(lock.getAllStatistics().isEmpty());
        }
    }
}
