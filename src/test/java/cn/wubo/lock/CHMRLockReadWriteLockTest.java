package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockReadWriteLockTest {

    @Test
    void readWriteLockReturnsSameInstanceForSameKey() {
        try (CHMRLock lock = new CHMRLock()) {
            KeyedReadWriteLock a = lock.readWriteLock("k1");
            KeyedReadWriteLock b = lock.readWriteLock("k1");
            assertSame(a, b, "readWriteLock should return the same instance for the same key");
        }
    }

    @Test
    void readWriteLockReturnsDifferentInstancesForDifferentKeys() {
        try (CHMRLock lock = new CHMRLock()) {
            assertNotSame(lock.readWriteLock("k1"), lock.readWriteLock("k2"));
        }
    }

    @Test
    void readWriteLockIsIndependentOfMainLock() {
        // Holding a tryLock on a key should NOT block a readLock on the same key
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            // readWriteLock should still work for k1
            KeyedReadWriteLock rw = lock.readWriteLock("k1");
            long stamp = rw.tryReadLock();
            assertNotEquals(0L, stamp,
                    "readWriteLock should work independently of the main ReentrantLock");
            rw.unlockRead(stamp);
            lock.unlock("k1");
        }
    }

    @Test
    void readWriteLockDoesNotRegisterInLockMap() {
        // Calling readWriteLock alone should NOT create a lockMap entry
        try (CHMRLock lock = new CHMRLock()) {
            lock.readWriteLock("k1");
            assertFalse(lock.getActiveKeys().contains("k1"),
                    "readWriteLock should not add to main lockMap");
        }
    }

    @Test
    void concurrentReadersOnReadWriteLock() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            KeyedReadWriteLock rw = lock.readWriteLock("k1");
            AtomicInteger concurrentReaders = new AtomicInteger(0);
            AtomicInteger maxConcurrent = new AtomicInteger(0);
            int readerCount = 5;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(readerCount);
            for (int i = 0; i < readerCount; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        long stamp = rw.readLock();
                        int current = concurrentReaders.incrementAndGet();
                        maxConcurrent.updateAndGet(m -> Math.max(m, current));
                        Thread.sleep(50);
                        concurrentReaders.decrementAndGet();
                        rw.unlockRead(stamp);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(readerCount, maxConcurrent.get(),
                    "all readers should hold the read lock concurrently");
        }
    }

    @Test
    void writeLockExcludesReaders() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            KeyedReadWriteLock rw = lock.readWriteLock("k1");
            long w = rw.writeLock();
            // While write is held, tryReadLock should fail
            assertEquals(0L, rw.tryReadLock(),
                    "tryReadLock should fail while write is held");
            // And a blocking read should not acquire
            CountDownLatch acquired = new CountDownLatch(1);
            Thread reader = new Thread(() -> {
                try {
                    long stamp = rw.readLock();
                    acquired.countDown();
                    rw.unlockRead(stamp);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            reader.start();
            assertFalse(acquired.await(100, TimeUnit.MILLISECONDS),
                    "reader should be blocked while write is held");
            rw.unlockWrite(w);
            // Now reader should acquire
            assertTrue(acquired.await(2, TimeUnit.SECONDS),
                    "reader should acquire after write released");
            reader.join(2000);
        }
    }
}
