package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockFairLockTest {

    @Test
    void fairLockCanBeEnabled() {
        CHMRLock lock = new CHMRLock(CHMRLockConfig.builder().fairLock(true).build());
        try {
            assertTrue(lock.tryLock("k"));
            lock.unlock("k");
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void defaultIsNonFair() {
        CHMRLock lock = new CHMRLock();
        try {
            assertTrue(lock.tryLock("k"));
            lock.unlock("k");
        } finally {
            lock.shutdown();
        }
    }
}
