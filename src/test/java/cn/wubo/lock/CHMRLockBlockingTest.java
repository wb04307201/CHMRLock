package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockBlockingTest {

    @Test
    void lockBlocksUntilReleased() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            CountDownLatch acquired = new CountDownLatch(1);
            Thread holder = new Thread(() -> {
                try {
                    lock.lock("k1");
                    acquired.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            holder.start();
            // Should NOT acquire while we hold k1
            assertFalse(acquired.await(200, TimeUnit.MILLISECONDS),
                    "lock should not acquire while k1 is held");
            // Release
            lock.unlock("k1");
            // Now should acquire
            assertTrue(acquired.await(2, TimeUnit.SECONDS),
                    "lock should acquire after release");
            holder.join(2000);
        }
    }

    @Test
    void lockWithLeaseSetsLease() throws Exception {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            lock.lock("k1", 30, TimeUnit.SECONDS);
            // Verify lease is set
            KeyStatistics ks = lock.getStatistics("k1").orElseThrow();
            assertTrue(ks.lastAcquireEpochMs() > 0,
                    "lastAcquireEpochMs should be set after lock()");
            assertTrue(lock.isLocked("k1"));
        }
    }

    @Test
    void lockInterruptiblyRespondsToInterrupt() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            AtomicReference<Throwable> caught = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch reachedLockInterruptibly = new CountDownLatch(1);
            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    // Signal that we're about to enter lockInterruptibly
                    reachedLockInterruptibly.countDown();
                    lock.lockInterruptibly("k1", 30, TimeUnit.SECONDS);
                } catch (Throwable t) {
                    caught.set(t);
                }
            });
            waiter.start();
            assertTrue(started.await(1, TimeUnit.SECONDS));
            // Wait until the waiter is definitely inside lockInterruptibly
            assertTrue(reachedLockInterruptibly.await(1, TimeUnit.SECONDS),
                    "waiter should have reached lockInterruptibly");
            waiter.interrupt();
            waiter.join(2000);
            assertInstanceOf(InterruptedException.class, caught.get(),
                    "expected InterruptedException, got: " + caught.get());
            lock.unlock("k1");
        }
    }

    @Test
    void lockOnContendedKeyWaitsForever() throws Exception {
        // No timeout: this is the blocking semantic
        try (CHMRLock lock = new CHMRLock()) {
            assertTrue(lock.tryLock("k1"));
            CountDownLatch acquired = new CountDownLatch(1);
            Thread holder = new Thread(() -> {
                try {
                    lock.lock("k1");  // blocks forever
                    acquired.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            holder.start();
            // Wait a while
            Thread.sleep(100);
            assertFalse(acquired.getCount() == 0, "lock should still be blocked");
            // Release from main
            lock.unlock("k1");
            // Now it acquires
            assertTrue(acquired.await(2, TimeUnit.SECONDS));
            holder.join(2000);
        }
    }

    @Test
    void lockUncontendedAcquiresImmediately() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            long start = System.nanoTime();
            lock.lock("k1");
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 1000, "uncontended lock should be < 1000ms, was: " + elapsedMs);
            assertTrue(lock.isLocked("k1"));
        }
    }
}
