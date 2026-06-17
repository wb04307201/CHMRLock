package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockMaxKeysTest {

    @Test
    void tryLockRejectsWhenMaxExceeded() {
        CHMRLock lock = new CHMRLock(CHMRLockConfig.builder().maxKeys(2).build());
        try {
            assertTrue(lock.tryLock("a"));
            assertTrue(lock.tryLock("b"));
            assertFalse(lock.tryLock("c"), "超过 maxKeys 时应拒绝新 key");
            assertEquals(2, lock.getActiveKeys().size());
            lock.unlock("a");
            assertTrue(lock.tryLock("c"), "释放后应能再获取");
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void maxKeysZeroMeansUnlimited() {
        CHMRLock lock = new CHMRLock(CHMRLockConfig.builder().maxKeys(0).build());
        try {
            for (int i = 0; i < 100; i++) {
                assertTrue(lock.tryLock("k" + i));
            }
            assertEquals(100, lock.getActiveKeys().size());
        } finally {
            lock.shutdown();
        }
    }
}
