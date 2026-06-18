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
    void tryAcquireAsyncWithTimeoutSucceeds() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            CompletableFuture<Optional<AcquiredLock>> future = lock.tryAcquireAsync("k1", 1, TimeUnit.SECONDS);
            Optional<AcquiredLock> acquired = future.get(2, TimeUnit.SECONDS);
            assertTrue(acquired.isPresent());
            // Lock is held by the async thread; CHMRLock-level isLocked reports true
            assertTrue(lock.isLocked("k1"));
            // Cleanup handled by the manager's close() which calls lockMap.clear().
        }
    }

    @Test
    void tryAcquireAsyncWithTimeoutReturnsEmptyOnTimeout() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            // Hold k1 in this thread
            assertTrue(lock.tryLock("k1"));
            // Async try to acquire k1 with short timeout
            CompletableFuture<Optional<AcquiredLock>> future = lock.tryAcquireAsync("k1", 50, TimeUnit.MILLISECONDS);
            Optional<AcquiredLock> acquired = future.get(2, TimeUnit.SECONDS);
            assertFalse(acquired.isPresent());
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
        CompletableFuture<Optional<AcquiredLock>> f1 = lock.tryAcquireAsync("k1", 1, TimeUnit.SECONDS);
        assertTrue(f1.get(2, TimeUnit.SECONDS).isPresent());
        // Close should also shutdown the async executor
        lock.shutdown();
        // R6 修复(R-2):asyncExecutor 自定义 handler 改为"shutdown 抛 REE,队列满时调用线程执行"。
        // shutdown 后业务方应显式捕获并降级处理(不再被调用线程同步阻塞 3 秒+)。
        // 业务方正确处理方式:捕获 REE 后转为同步 tryAcquire 或放弃。
        assertThrows(RejectedExecutionException.class,
                () -> lock.tryAcquireAsync("k2", 1, TimeUnit.SECONDS),
                "R6 修复后 shutdown 应抛 RejectedExecutionException");
    }
}
