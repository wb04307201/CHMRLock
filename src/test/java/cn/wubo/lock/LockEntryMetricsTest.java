package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LockEntryMetricsTest {

    @Test
    void initialCountersAreZero() {
        LockEntry e = new LockEntry();
        assertEquals(0L, e.getAcquireCount());
        assertEquals(0L, e.getSuccessCount());
        assertEquals(0L, e.getFailedCount());
        assertEquals(0L, e.getTotalWaitNanos());
    }

    @Test
    void recordAcquireSuccessIncrements() {
        LockEntry e = new LockEntry();
        e.recordAcquireAttempt();
        e.recordAcquireSuccess(1_000_000L);
        assertEquals(1L, e.getAcquireCount());
        assertEquals(1L, e.getSuccessCount());
        assertEquals(0L, e.getFailedCount());
        assertEquals(1_000_000L, e.getTotalWaitNanos());
    }

    @Test
    void recordAcquireFailureIncrements() {
        LockEntry e = new LockEntry();
        e.recordAcquireAttempt();
        e.recordAcquireFailure(2_000_000L);
        e.recordAcquireAttempt();
        e.recordAcquireSuccess(500_000L);
        assertEquals(2L, e.getAcquireCount());
        assertEquals(1L, e.getSuccessCount());
        assertEquals(1L, e.getFailedCount());
        assertEquals(2_500_000L, e.getTotalWaitNanos());
    }

    @Test
    void recordReleaseSetsLastReleaseTime() {
        LockEntry e = new LockEntry();
        e.recordRelease(123456789L);
        assertEquals(123456789L, e.getLastReleaseTime());
    }

    @Test
    void initialLastReleaseTimeIsZero() {
        LockEntry e = new LockEntry();
        assertEquals(0L, e.getLastReleaseTime());
    }

    @Test
    void fairLockConstructorStillWorks() {
        LockEntry e = new LockEntry(true);
        assertNotNull(e.lock);
    }
}
