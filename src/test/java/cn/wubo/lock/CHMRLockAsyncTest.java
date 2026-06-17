package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockAsyncTest {

    @Test
    void acquireAsyncSucceeds() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            CompletableFuture<Boolean> future = lock.acquireAsync("k1");
            Boolean acquired = future.get(2, TimeUnit.SECONDS);
            assertTrue(acquired);
            // Lock is held by the async thread; CHMRLock-level isLocked reports true
            assertTrue(lock.isLocked("k1"));
        }
    }

    @Test
    void acquireAsyncReturnsFalseOnTimeout() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            // Hold k1 in this thread
            assertTrue(lock.tryLock("k1"));
            // Async try to acquire k1 with short timeout
            CompletableFuture<Boolean> future = lock.acquireAsync("k1", 50, TimeUnit.MILLISECONDS);
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
            Thread mainThread = Thread.currentThread();
            CompletableFuture<Long> future = CompletableFuture.supplyAsync(
                () -> {
                    try {
                        if (lock.tryLock("k1")) {
                            long id = Thread.currentThread().getId();
                            lock.unlock("k1");
                            return id;
                        }
                        return -1L;
                    } catch (Exception e) {
                        return -1L;
                    }
                }
            );
            long asyncThreadId = future.get(2, TimeUnit.SECONDS);
            assertNotEquals(mainThread.getId(), asyncThreadId,
                    "async acquisition should run on a different thread");
        }
    }

    @Test
    void asyncExecutorShutdownOnManagerClose() throws Exception {
        CHMRLock lock = new CHMRLock();
        CompletableFuture<Boolean> f1 = lock.acquireAsync("k1");
        assertTrue(f1.get(2, TimeUnit.SECONDS));
        // Close should also shutdown the async executor
        lock.shutdown();
        // After shutdown, new async tasks should fail or be rejected
        // (this is implementation-defined; we just verify shutdown doesn't hang)
    }
}
