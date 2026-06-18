package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CHMRLock的单元测试类.
 */
public class CHMRLockTest {

    private CHMRLock lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new CHMRLock();
    }

    @AfterEach
    void tearDown() {
        lockManager.shutdown();
    }

    /**
     * 测试大量并发锁请求的正确性
     */
    @Test
    public void testHighConcurrencyLockRequests() throws InterruptedException {
        final int threadCount = 50;
        final int requestsPerThread = 100;
        final String lockKey = "test_key";

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount * requestsPerThread);
        AtomicInteger successCounter = new AtomicInteger(0);

        // 提交大量并发任务
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    try {
                        if (lockManager.tryLock(lockKey, 50, TimeUnit.MILLISECONDS)) {
                            try {
                                // 模拟临界区操作
                                Thread.sleep(1);
                                successCounter.incrementAndGet();
                            } finally {
                                lockManager.unlock(lockKey);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        // 等待所有任务完成
        assertTrue(latch.await(30, TimeUnit.SECONDS), "所有锁请求应在30秒内完成");
        executor.shutdown();

        // 验证结果
        MonitorMetrics metrics = lockManager.getStatistics();
        assertEquals(threadCount * requestsPerThread, metrics.getTotalLocks(),
                "总锁请求数应等于线程数乘以每个线程的请求数");
        assertTrue(metrics.getSuccessLocks() > 0, "应至少成功获取一次锁");
        assertEquals(metrics.getSuccessLocks() + metrics.getFailedLocks(), metrics.getTotalLocks(),
                "成功和失败的锁请求总数应等于总请求数");
    }

    /**
     * 测试大量不同key的锁管理
     */
    @Test
    public void testMultipleKeysManagement() throws InterruptedException {
        final int keyCount = 1000;
        final int threadCount = 10;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(keyCount);

        // 为每个key创建一个任务
        for (int i = 0; i < keyCount; i++) {
            final String key = "multi_key_" + i;
            executor.submit(() -> {
                try {
                    if (lockManager.tryLock(key, 100, TimeUnit.MILLISECONDS)) {
                        try {
                            // 模拟工作
                            Thread.sleep(1);
                        } finally {
                            lockManager.unlock(key);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有任务完成
        assertTrue(latch.await(30, TimeUnit.SECONDS), "所有锁操作应在30秒内完成");
        executor.shutdown();

        // 验证统计信息
        MonitorMetrics metrics = lockManager.getStatistics();
        assertEquals(keyCount, metrics.getTotalLocks(), "总锁请求数应等于key数量");
    }

    /**
     * 测试shutdown方法是否正确清理资源。
     *
     * <p>R6 修复(R-1):shutdown 后 tryLock 抛 {@link IllegalStateException},
     * 防止新 entry 加入已停止清理线程的 lockMap。之前的测试行为("shutdown 后
     * 仍可获取新锁")是已知的内存泄漏,现在改为显式失败。</p>
     */
    @Test
    public void testShutdownResourceCleanup() {
        // 先获取几个锁
        assertTrue(lockManager.tryLock("cleanup_key1"));
        assertTrue(lockManager.tryLock("cleanup_key2"));
        assertTrue(lockManager.tryLock("cleanup_key3"));

        // 验证锁存在
        MonitorMetrics beforeShutdown = lockManager.getStatistics();
        assertEquals(3, beforeShutdown.getTotalLocks());

        // 执行清理
        lockManager.shutdown();

        // R6 修复(R-1):shutdown 后 tryLock 抛 IllegalStateException
        assertThrows(IllegalStateException.class, () -> lockManager.tryLock("cleanup_key1"),
                "shutdown 后应拒绝新增 entry(R6 修复 R-1)");
        assertThrows(IllegalStateException.class, () -> lockManager.tryLock("cleanup_key4"));
    }

    /**
     * 测试锁的公平性（虽然ReentrantLock默认非公平，但我们验证其行为）
     */
    @Test
    public void testLockFairness() throws InterruptedException {
        final String key = "fairness_test_key";
        final int threadCount = 10;
        List<Long> acquisitionOrder = new ArrayList<>();

        // 先获取锁，确保其他线程需要等待
        assertTrue(lockManager.tryLock(key));

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // 启动多个线程尝试获取锁
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    if (lockManager.tryLock(key, 1000, TimeUnit.MILLISECONDS)) {
                        try {
                            // 记录获取锁的顺序
                            acquisitionOrder.add(Thread.currentThread().getId());
                            Thread.sleep(10); // 持有锁一小段时间
                        } finally {
                            lockManager.unlock(key);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 释放初始锁，让其他线程竞争
        Thread.sleep(100); // 确保所有线程都已启动并等待
        lockManager.unlock(key);

        // 等待所有线程完成
        assertTrue(latch.await(5, TimeUnit.SECONDS), "所有线程应在5秒内完成");
        executor.shutdown();

        // 验证至少有一些线程成功获取了锁
        assertFalse(acquisitionOrder.isEmpty(), "应有线程成功获取锁");
    }

    @Test
    public void testUnlockUnknownKeyIsNoop() {
        // 未知 key 的 unlock 应为幂等 no-op(避免 AcquiredLock.close() 在清理后失败)
        assertDoesNotThrow(() -> lockManager.unlock("never_locked"));
    }

    @Test
    public void testUnlockAfterReleaseThrows() {
        assertTrue(lockManager.tryLock("temp"));
        lockManager.unlock("temp");
        // 释放后 entry 仍存在但 lock 未持有
        assertThrows(IllegalMonitorStateException.class,
                () -> lockManager.unlock("temp"));
    }
}
