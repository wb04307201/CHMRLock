package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockMultiLockTest {

    @Test
    void multiLockAcquiresAllKeys() {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryMultiLock("k1", "k2", "k3"));
            assertTrue(lock.isLocked("k1"));
            assertTrue(lock.isLocked("k2"));
            assertTrue(lock.isLocked("k3"));
            lock.unlock("k1");
            lock.unlock("k2");
            lock.unlock("k3");
        }
    }

    @Test
    void multiLockEmptyArrayReturnsTrue() {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryMultiLock(new String[0]));
        }
    }

    @Test
    void multiLockSingleKeyBehavesLikeTryLock() {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryMultiLock("k1"));
            assertTrue(lock.isLocked("k1"));
            lock.unlock("k1");
        }
    }

    @Test
    void multiLockRollsBackOnPartialFailure() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            // Hold k2 in a background thread (different thread so reentrancy doesn't mask contention)
            CountDownLatch holderReady = new CountDownLatch(1);
            CountDownLatch releaseHolder = new CountDownLatch(1);
            Thread holder = new Thread(() -> {
                lock.tryLock("k2");
                holderReady.countDown();
                try { releaseHolder.await(); } catch (InterruptedException e) {}
                lock.unlock("k2");
            });
            holder.start();
            assertTrue(holderReady.await(2, TimeUnit.SECONDS));

            // Attempt multiLock with k1, k2, k3 — should fail on k2 (held by other thread)
            assertFalse(lock.tryMultiLock(50, TimeUnit.MILLISECONDS, "k1", "k2", "k3"));
            // k1 should NOT be held (rolled back)
            assertFalse(lock.isLocked("k1"), "k1 should be released after rollback");
            assertTrue(lock.isLocked("k2"), "k2 still held by holder thread");
            assertFalse(lock.isLocked("k3"));

            releaseHolder.countDown();
            holder.join(2_000);
        }
    }

    @Test
    void multiLockSortsKeysToPreventDeadlock() throws Exception {
        // Two threads acquire keys in opposite orders — with sorted-order acquisition,
        // they cannot deadlock.
        try (CHMRLock lock = new CHMRLock()) {
            String[] keys = {"a", "b", "c", "d", "e"};
            CountDownLatch t1Done = new CountDownLatch(1);
            CountDownLatch t2Done = new CountDownLatch(1);

            Thread t1 = new Thread(() -> {
                lock.tryMultiLock(2, TimeUnit.SECONDS,
                        keys[0], keys[1], keys[2], keys[3], keys[4]);
                t1Done.countDown();
                // Hold for a moment
                try { Thread.sleep(100); } catch (InterruptedException e) {}
                lock.unlock(keys[0]);
                lock.unlock(keys[1]);
                lock.unlock(keys[2]);
                lock.unlock(keys[3]);
                lock.unlock(keys[4]);
            });

            Thread t2 = new Thread(() -> {
                try { Thread.sleep(20); } catch (InterruptedException e) {}  // delay so t1 starts first
                // Acquire in REVERSE order — with sorted order internally, no deadlock
                lock.tryMultiLock(2, TimeUnit.SECONDS,
                        keys[4], keys[3], keys[2], keys[1], keys[0]);
                t2Done.countDown();
                lock.unlock(keys[4]);
                lock.unlock(keys[3]);
                lock.unlock(keys[2]);
                lock.unlock(keys[1]);
                lock.unlock(keys[0]);
            });

            t1.start();
            t2.start();
            // Both should complete (no deadlock)
            assertTrue(t1Done.await(5, TimeUnit.SECONDS));
            assertTrue(t2Done.await(5, TimeUnit.SECONDS));
            t1.join();
            t2.join();
        }
    }

    @Test
    void multiLockTimeoutRollsBack() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            // Hold k1 in a background thread (different thread so reentrancy doesn't mask contention)
            CountDownLatch holderReady = new CountDownLatch(1);
            CountDownLatch releaseHolder = new CountDownLatch(1);
            Thread holder = new Thread(() -> {
                lock.tryLock("k1");
                holderReady.countDown();
                try { releaseHolder.await(); } catch (InterruptedException e) {}
                lock.unlock("k1");
            });
            holder.start();
            assertTrue(holderReady.await(2, TimeUnit.SECONDS));

            // Try multiLock with 50ms timeout on k1 + k2 — should timeout on k1
            assertFalse(lock.tryMultiLock(50, TimeUnit.MILLISECONDS, "k1", "k2"));
            // k2 should be released (never acquired, but if it was, rolled back)
            assertFalse(lock.isLocked("k2"));
            assertTrue(lock.isLocked("k1"), "k1 still held by holder thread");

            releaseHolder.countDown();
            holder.join(2_000);
        }
    }

    @Test
    void multiLockDedupesKeys() {
        try (CHMRLock lock = new CHMRLock()) {
            // Passing the same key twice should be a no-op (deduped)
            assertTrue(lock.tryMultiLock("k1", "k1", "k1"));
            assertEquals(1, lock.getHoldCount("k1"), "should be acquired once, not 3x");
            lock.unlock("k1");
        }
    }
}