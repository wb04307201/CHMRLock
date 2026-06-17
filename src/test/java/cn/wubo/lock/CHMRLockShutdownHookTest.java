package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockShutdownHookTest {

    @Test
    void shutdownIsIdempotent() {
        CHMRLock lock = new CHMRLock();
        lock.shutdown();
        assertDoesNotThrow(lock::shutdown, "shutdown 必须是幂等的");
    }

    @Test
    void isShutdownReflectsState() {
        CHMRLock lock = new CHMRLock();
        assertFalse(lock.isShutdown());
        lock.shutdown();
        assertTrue(lock.isShutdown());
    }

    @Test
    void closeCallsShutdown() {
        CHMRLock lock = new CHMRLock();
        lock.close();
        assertTrue(lock.isShutdown());
    }
}
