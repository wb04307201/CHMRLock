package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the cross-thread {@code forceUnlock(key)} capability.
 *
 * <p>The underlying {@link java.util.concurrent.locks.ReentrantLock} is non-transferable
 * (its {@code unlock} requires the owner thread). The CHMRLock-level {@code forceUnlock}
 * therefore clears the CHMRLock state and lets {@link CHMRLock#isLocked(String)} report
 * the lock as released. The original owner thread's {@code unlock(key)} will subsequently
 * throw {@link IllegalMonitorStateException} (which mirrors the existing lease-expiry
 * cleanup behaviour in {@code scheduleLeaseExpiry}).</p>
 */
class CHMRLockForceUnlockTest {

    @Test
    void forceUnlockClearsChmrLockState() {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            assertTrue(lock.tryLock("k1"));
            assertTrue(lock.isLocked("k1"));
            lock.forceUnlock("k1");
            // CHMRLock state is cleared
            assertFalse(lock.isLocked("k1"), "isLocked should return false after forceUnlock");
            // owner is cleared
            assertNull(lock.getOwnerThreadId("k1"), "owner should be cleared after forceUnlock");
        }
    }

    @Test
    void forceUnlockOnUnknownKeyIsNoOp() {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            // Should not throw on a key that was never acquired.
            assertDoesNotThrow(() -> lock.forceUnlock("never-locked"));
        }
    }

    @Test
    void forceUnlockOnUnlockedKeyIsNoOp() {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            // Touch the key (creates entry) then release normally.
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            // Now forceUnlock on an already-released key — must be a no-op, not an exception.
            assertDoesNotThrow(() -> lock.forceUnlock("k1"));
        }
    }

    @Test
    void forceUnlockDisabledByDefault() {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            // forceUnlockEnabled defaults to false — should throw
            assertThrows(UnsupportedOperationException.class, () -> lock.forceUnlock("k1"));
        }
    }

    @Test
    void forceUnlockFromOtherThreadClearsChmrLockState() throws InterruptedException {
        // Cross-thread force-unlock: even though ReentrantLock is non-transferable,
        // CHMRLock's isLocked must report the lock as released.
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            assertTrue(lock.tryLock("k1"));
            assertTrue(lock.isLocked("k1"));
            Thread other = new Thread(() -> {
                try {
                    lock.forceUnlock("k1");
                } catch (UnsupportedOperationException e) {
                    fail("forceUnlock should be enabled: " + e.getMessage());
                }
            });
            other.start();
            other.join(2000);
            // From CHMRLock's perspective, the lock is released.
            assertFalse(lock.isLocked("k1"),
                    "CHMRLock should report lock as released after forceUnlock");
            assertNull(lock.getOwnerThreadId("k1"),
                    "Owner should be cleared after forceUnlock from other thread");
        }
    }
}
