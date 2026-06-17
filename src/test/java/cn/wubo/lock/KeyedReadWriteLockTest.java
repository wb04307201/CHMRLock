package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KeyedReadWriteLockTest {

    /** Use a StampedLock-backed implementation for testing. */
    private KeyedReadWriteLock newLock() {
        return new StampedKeyedReadWriteLock();
    }

    @Test
    void readLockAllowsMultipleReaders() throws Exception {
        KeyedReadWriteLock lock = newLock();
        long r1 = lock.readLock();
        long r2 = lock.readLock();
        // Both should be valid (concurrent readers allowed)
        assertNotEquals(0L, r1);
        assertNotEquals(0L, r2);
        assertNotEquals(r1, r2, "stamps should differ");
        lock.unlockRead(r1);
        lock.unlockRead(r2);
    }

    @Test
    void writeLockIsExclusive() throws Exception {
        KeyedReadWriteLock lock = newLock();
        long w = lock.writeLock();
        // While write is held, read should fail with tryReadLock
        assertEquals(0L, lock.tryReadLock(), "tryReadLock should fail during write");
        // And tryWriteLock should also fail
        assertEquals(0L, lock.tryWriteLock(), "tryWriteLock should fail during write");
        lock.unlockWrite(w);
        // After unlock, read should succeed
        long r = lock.tryReadLock();
        assertNotEquals(0L, r);
        lock.unlockRead(r);
    }

    @Test
    void tryWriteLockSucceedsWhenUnlocked() {
        KeyedReadWriteLock lock = newLock();
        long w = lock.tryWriteLock();
        assertNotEquals(0L, w);
        lock.unlockWrite(w);
    }

    @Test
    void optimisticReadValidates() throws Exception {
        KeyedReadWriteLock lock = newLock();
        long stamp = lock.tryOptimisticRead();
        assertNotEquals(0L, stamp);
        // No writes yet, should validate
        assertTrue(lock.validate(stamp));
        // After acquiring/releasing write, the old stamp should not validate
        long w = lock.writeLock();
        assertFalse(lock.validate(stamp), "stale stamp should not validate after write");
        lock.unlockWrite(w);
    }

    @Test
    void writeLockBlocksReaders() throws Exception {
        KeyedReadWriteLock lock = newLock();
        long w = lock.writeLock();
        CountDownLatch acquired = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            try {
                long r = lock.readLock();  // blocks
                acquired.countDown();
                lock.unlockRead(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reader.start();
        // Should not acquire while write held
        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS),
                "reader should be blocked while write is held");
        lock.unlockWrite(w);
        // Should acquire now
        assertTrue(acquired.await(2, TimeUnit.SECONDS),
                "reader should acquire after write released");
        reader.join(2000);
    }
}
