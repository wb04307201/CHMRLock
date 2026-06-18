package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockQueryTest {

    private CHMRLock lock;

    @BeforeEach
    void setUp() { lock = new CHMRLock(); }

    @AfterEach
    void tearDown() { lock.shutdown(); }

    @Test
    void isLockedFalseForUnknownKey() {
        assertFalse(lock.isLocked("nope"));
    }

    @Test
    void isLockedTrueAfterAcquire() {
        assertTrue(lock.tryLock("k"));
        assertTrue(lock.isLocked("k"));
    }

    @Test
    void isLockedFalseAfterRelease() {
        assertTrue(lock.tryLock("k"));
        lock.unlock("k");
        assertFalse(lock.isLocked("k"));
    }

    @Test
    void isHeldByCurrentThreadDetectsSelf() {
        assertTrue(lock.tryLock("k"));
        assertTrue(lock.isHeldByCurrentThread("k"));
        lock.unlock("k");
        assertFalse(lock.isHeldByCurrentThread("k"));
    }

    @Test
    void isHeldByCurrentThreadFalseForUnknownKey() {
        assertFalse(lock.isHeldByCurrentThread("nope"));
    }

    @Test
    void getHoldCountReflectsReentrancy() {
        assertTrue(lock.tryLock("k"));
        assertTrue(lock.tryLock("k"));  // 重入
        assertEquals(2, lock.getHoldCount("k"));
        lock.unlock("k");
        assertEquals(1, lock.getHoldCount("k"));
        lock.unlock("k");
        assertEquals(0, lock.getHoldCount("k"));
    }

    @Test
    void getHoldCountZeroForUnknownKey() {
        assertEquals(0, lock.getHoldCount("nope"));
    }

    @Test
    void getActiveKeysEmptyInitially() {
        assertTrue(lock.getActiveKeys().isEmpty());
    }

    @Test
    void getActiveKeysContainsHeld() {
        lock.tryLock("a");
        lock.tryLock("b");
        Set<String> keys = lock.getActiveKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
    }

    @Test
    void getActiveKeysIsImmutable() {
        lock.tryLock("a");
        Set<String> keys = lock.getActiveKeys();
        assertThrows(UnsupportedOperationException.class, () -> keys.add("b"));
    }
}
