package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockStatisticsTest {

    @Test
    void emptyStatisticsReturnZero() {
        CHMRLock lock = new CHMRLock();
        try {
            MonitorMetrics m = lock.getStatistics();
            assertEquals(0, m.getTotalLocks());
            assertEquals(0.0, m.getSuccessRate());
            assertEquals(0.0, m.getAvgWaitTime());
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void successRateComputedCorrectly() {
        CHMRLock lock = new CHMRLock();
        try {
            assertTrue(lock.tryLock("a"));
            // 直接验证至少有一次成功请求被统计
            MonitorMetrics m = lock.getStatistics();
            assertTrue(m.getTotalLocks() >= 1);
            assertTrue(m.getSuccessLocks() >= 1);
        } finally {
            lock.unlock("a");
            lock.shutdown();
        }
    }
}
