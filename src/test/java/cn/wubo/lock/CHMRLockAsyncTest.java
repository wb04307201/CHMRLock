package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockAsyncTest {

    @Test
    void tryAcquireAsyncBooleanSucceeds() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            CompletableFuture<Boolean> future = lock.tryAcquireAsync("k1", 1, TimeUnit.SECONDS);
            Boolean acquired = future.get(2, TimeUnit.SECONDS);
            assertTrue(acquired);
            // Lock is held by the async thread; CHMRLock-level isLocked reports true
            assertTrue(lock.isLocked("k1"));
            // Cleanup handled by the manager's close() which calls lockMap.clear().
        }
    }

    @Test
    void tryAcquireAsyncBooleanReturnsFalseOnTimeout() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            // Hold k1 in this thread
            assertTrue(lock.tryLock("k1"));
            // Async try to acquire k1 with short timeout
            CompletableFuture<Boolean> future = lock.tryAcquireAsync("k1", 50, TimeUnit.MILLISECONDS);
            Boolean acquired = future.get(2, TimeUnit.SECONDS);
            assertFalse(acquired);
            lock.unlock("k1");
        }
    }

    @Test
    void tryAcquireAsyncReturnsOptional() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            CompletableFuture<Optional<AcquiredLock>> future = lock.tryAcquireAsync("k1");
            Optional<AcquiredLock> result = future.get(2, TimeUnit.SECONDS);
            assertTrue(result.isPresent());
            // Lock is held by the async thread; CHMRLock-level isLocked reports true.
            assertTrue(lock.isLocked("k1"));
            // Do NOT close AcquiredLock from main thread - that would throw IllegalMonitorState.
            // The async thread owns the lock. Releasing the manager clears the map on shutdown.
        }
    }

    @Test
    void tryAcquireAsyncReturnsEmptyOnTimeout() throws Exception {
        CHMRLockConfig config = CHMRLockConfig.builder()
                .defaultWaitTime(Duration.ofMillis(100))
                .build();
        try (CHMRLock lock = new CHMRLock(config)) {
            assertTrue(lock.tryLock("k1"));
            CompletableFuture<Optional<AcquiredLock>> future = lock.tryAcquireAsync("k1");
            Optional<AcquiredLock> result = future.get(2, TimeUnit.SECONDS);
            assertFalse(result.isPresent());
            lock.unlock("k1");
        }
    }

    @Test
    void asyncUsesDifferentThread() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            AtomicReference<String> acquiredThreadName = new AtomicReference<>();
            lock.registerListener(new LockListener() {
                @Override
                public void onLockAcquired(String key, long waitNanos) {
                    if ("k1".equals(key)) {
                        acquiredThreadName.compareAndSet(null, Thread.currentThread().getName());
                    }
                }
            });

            lock.tryAcquireAsync("k1").get(2, TimeUnit.SECONDS);

            // The capture may be null if the listener ran before the AtomicReference was set up
            // or the comparison-set missed. Re-read to handle the race benignly.
            String name = acquiredThreadName.get();
            assertNotNull(name, "onLockAcquired should have fired for k1");
            assertTrue(name.startsWith("chmrlock-async-"),
                    "async acquisition should run on the dedicated chmrlock-async-N thread, was: " + name);
        }
    }

    @Test
    void asyncExecutorShutdownOnManagerClose() throws Exception {
        CHMRLockConfig config = CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build();
        CHMRLock lock = new CHMRLock(config);
        CompletableFuture<Boolean> f1 = lock.tryAcquireAsync("k1", 1, TimeUnit.SECONDS);
        assertTrue(f1.get(2, TimeUnit.SECONDS));
        // Close should also shutdown the async executor
        lock.shutdown();
        // After shutdown, new async tasks should be rejected.
        // supplyAsync(RejectedExecutionException) is the typical behavior for a shut-down
        // ExecutorService; the supplier may also complete the future exceptionally.
        // We accept either form (synchronous throw or completed-exceptionally future).
        try {
            CompletableFuture<Boolean> f2 = lock.tryAcquireAsync("k2", 1, TimeUnit.SECONDS);
            // If we got a future back, it must complete exceptionally.
            try {
                f2.get(2, TimeUnit.SECONDS);
                fail("expected RejectedExecutionException (sync or async) after shutdown");
            } catch (java.util.concurrent.ExecutionException ee) {
                Throwable cause = ee.getCause();
                assertTrue(cause instanceof RejectedExecutionException,
                        "future should complete exceptionally with RejectedExecutionException, was: " + cause);
            }
        } catch (RejectedExecutionException expected) {
            // Synchronous rejection: also acceptable.
        }
    }
}
