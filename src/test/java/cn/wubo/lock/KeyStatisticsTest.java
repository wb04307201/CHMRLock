package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KeyStatisticsTest {

    @Test
    void basicFieldsAccessible() {
        KeyStatistics ks = new KeyStatistics(
                "k1", 10L, 8L, 2L, 1000L, 1000L, 2000L, 1, 42L);
        assertEquals("k1", ks.key());
        assertEquals(10L, ks.acquireCount());
        assertEquals(8L, ks.successCount());
        assertEquals(2L, ks.failedCount());
        assertEquals(1000L, ks.totalWaitNanos());
        assertEquals(1000L, ks.lastAcquireEpochMs());
        assertEquals(2000L, ks.lastReleaseEpochMs());
        assertEquals(1, ks.currentHoldCount());
        assertEquals(42L, ks.currentHolderThreadId());
    }

    @Test
    void successRateComputed() {
        KeyStatistics ks = new KeyStatistics("k", 10, 7, 3, 0, 0, 0, 0, -1);
        assertEquals(0.7, ks.getSuccessRate(), 0.001);
    }

    @Test
    void successRateZeroWhenNoAcquires() {
        KeyStatistics ks = new KeyStatistics("k", 0, 0, 0, 0, 0, 0, 0, -1);
        assertEquals(0.0, ks.getSuccessRate());
    }

    @Test
    void avgWaitTimeComputed() {
        KeyStatistics ks = new KeyStatistics("k", 4, 4, 0, 3_000_000, 0, 0, 0, -1);
        // 3_000_000 nanos / 4 = 0.75ms
        assertEquals(0.75, ks.getAvgWaitTimeMillis(), 0.001);
    }

    @Test
    void avgWaitTimeZeroWhenNoAcquires() {
        KeyStatistics ks = new KeyStatistics("k", 0, 0, 0, 0, 0, 0, 0, -1);
        assertEquals(0.0, ks.getAvgWaitTimeMillis());
    }
}
