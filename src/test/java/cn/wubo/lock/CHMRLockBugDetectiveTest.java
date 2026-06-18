package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 4 轮测试:隐蔽 bug 探测。
 *
 * 通过深入代码审计,针对可疑路径写专项测试:
 *
 * 1. forceUnlockOnLeaseExpiry=false 时,租约到期后是否仍自动释放?(Bug)
 * 2. tryMultiLock 在 tryLock 自身抛出意外异常时,前面的锁是否回滚?
 * 3. AcquiredLock.close() 被调用多次,是否每次都触发 LockListener.onLockReleased?
 * 4. unlock 一个 forceUnlocked 的 entry 后,getHoldCount 是否正确归零?
 * 5. 在 tryLock 内部并发 forceUnlock,哨兵路径是否安全?
 * 6. cleanup tick 中 tryLock/tryUnlock 的"借锁"操作是否影响统计?
 * 7. shutdown 后 unlock 已 forceUnlocked 的 key 行为?
 * 8. getAllStatistics 包含从未释放的 key 的统计信息?
 */
public class CHMRLockBugDetectiveTest {

    private CHMRLock lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new CHMRLock();
    }

    @AfterEach
    void tearDown() {
        if (lockManager != null && !lockManager.isShutdown()) {
            lockManager.shutdown();
        }
    }

    @Test
    @DisplayName("BUG:forceUnlockOnLeaseExpiry=false 不被尊重,租约到期仍自动释放")
    void test_bug01_forceUnlockOnLeaseExpiryIgnored() throws InterruptedException {
        lockManager.shutdown();
        // 用户意图:不希望租约到期时自动释放,保留给业务侧处理
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockOnLeaseExpiry(false)
                .cleanupInterval(Duration.ofMillis(20))
                .build());
        assertTrue(lockManager.tryLock("k", 50, 50, TimeUnit.MILLISECONDS));
        // 等待租约到期
        Thread.sleep(300);
        // 由于 forceUnlockOnLeaseExpiry=false,isLocked 应仍为 true (持有者仍持有)
        // 但实际:cleanup tick 仍会 markForceUnlocked,isLocked 变为 false
        boolean stillLocked = lockManager.isLocked("k");
        assertTrue(stillLocked,
                "forceUnlockOnLeaseExpiry=false 时租约到期后应仍 isLocked=true,但实际: " + stillLocked);
        // 清理
        lockManager.unlock("k");
    }

    @Test
    @DisplayName("forceUnlock 之后,持有线程调 unlock:CHMRLock 状态清理且不抛异常")
    void test_bug02_forceUnlockHoldCount() throws Exception {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                assertTrue(lockManager.tryLock("k"));
                assertTrue(lockManager.tryLock("k"));
                assertEquals(2, lockManager.getHoldCount("k"));
                entered.countDown();
                release.await();
                // 在 forceUnlocked 哨兵已置位的情况下 unlock
                lockManager.unlock("k");
                lockManager.unlock("k");
            } catch (Throwable t) {
                err.set(t);
            }
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        lockManager.forceUnlock("k");
        assertFalse(lockManager.isLocked("k"));
        assertNull(lockManager.getOwnerThreadId("k"),
                "forceUnlock 后 owner 应已被清理");

        release.countDown();
        holder.join(2000);
        assertNull(err.get(),
                "持有线程 unlock 在 forceUnlock 之后不应抛异常,实际: " + err.get());
        // 已知设计限制:跨线程 forceUnlock 时,ReentrantLock 持有计数无法跨线程减少
        // (底层 ReentrantLock 是 holder 所有,JavaDoc 已说明)。
        // 这里只验证 CHMRLock 状态:isLocked=false,owner=null。
    }

    @Test
    @DisplayName("forceUnlock 后 isLocked=false 时 unlock 已 forceUnlocked 哨兵的 key:幂等")
    void test_bug03_unlockAfterForceUnlock() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.forceUnlock("k");
        // 多次 unlock 都应 no-op
        assertDoesNotThrow(() -> {
            lockManager.unlock("k");
            lockManager.unlock("k");
            lockManager.unlock("k");
        });
    }

    @Test
    @DisplayName("shutdown 之前和之后调用 forceUnlock:未知 key 都安全")
    void test_bug04_forceUnlockBeforeAndAfterShutdown() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        // entry 还在(等待清理)
        lockManager.forceUnlock("k");  // 已释放,no-op
        lockManager.forceUnlock("never");  // 未知,no-op
        lockManager.shutdown();
        lockManager.forceUnlock("k");  // shutdown 后,no-op(lockMap 已 clear)
        lockManager.forceUnlock("never");
    }

    @Test
    @DisplayName("tryMultiLock:大量 keys 中混入重复,实际只 unique 一次")
    void test_bug05_multiLockWithManyDuplicates() {
        boolean ok = lockManager.tryMultiLock(
                "a", "b", "a", "c", "b", "a", "d");
        assertTrue(ok);
        Set<String> active = lockManager.getActiveKeys();
        assertEquals(4, active.size(), "去重后应只有 4 个 key");
        assertTrue(active.contains("a"));
        assertTrue(active.contains("b"));
        assertTrue(active.contains("c"));
        assertTrue(active.contains("d"));
        assertEquals(1, lockManager.getHoldCount("a"));
        lockManager.unlock("a");
        lockManager.unlock("b");
        lockManager.unlock("c");
        lockManager.unlock("d");
    }

    @Test
    @DisplayName("tryMultiLock 顺序无关:同一组 keys 不同顺序结果一致")
    void test_bug06_multiLockOrderInvariant() {
        String[] order1 = {"alpha", "beta", "gamma", "delta"};
        String[] order2 = {"delta", "gamma", "beta", "alpha"};
        String[] order3 = {"beta", "alpha", "delta", "gamma"};
        assertTrue(lockManager.tryMultiLock(100, TimeUnit.MILLISECONDS, order1));
        assertTrue(lockManager.isLocked("alpha"));
        assertTrue(lockManager.isLocked("beta"));
        assertTrue(lockManager.isLocked("gamma"));
        assertTrue(lockManager.isLocked("delta"));
        for (String k : order1) lockManager.unlock(k);

        assertTrue(lockManager.tryMultiLock(100, TimeUnit.MILLISECONDS, order2));
        for (String k : order2) lockManager.unlock(k);

        assertTrue(lockManager.tryMultiLock(100, TimeUnit.MILLISECONDS, order3));
        for (String k : order3) lockManager.unlock(k);
    }

    @Test
    @DisplayName("cleanupTick 借锁操作不增加 failedLocks 计数(它实际上成功获取又立即释放)")
    void test_bug07_cleanupBorrowLockDoesNotPolluteStats() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build());

        // 占用一个 key 长时间,等其超过 idle 阈值
        assertTrue(lockManager.tryLock("k"));
        Thread.sleep(200);  // 远超 idleThreshold
        // 清理 tick 借锁检查,但因为 isLocked=true 应跳过
        Thread.sleep(100);
        MonitorMetrics m = lockManager.getStatistics();
        // 持有者只 tryLock 一次,清理 tick 不应污染统计
        assertEquals(1, m.getTotalLocks(), "清理 tick 借锁不应增加 totalLocks");
        assertEquals(1, m.getSuccessLocks());
        lockManager.unlock("k");
    }

    @Test
    @DisplayName("并发 tryLock + forceUnlock:哨兵路径不会双重清理 owner/lease")
    void test_bug08_concurrentForceUnlock() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());

        int n = 100;
        AtomicReference<Throwable> err = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch done = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    String key = "k-" + idx;
                    if (lockManager.tryLock(key, 50, TimeUnit.MILLISECONDS)) {
                        // 不同线程并发 forceUnlock 后 unlock
                        lockManager.forceUnlock(key);
                        try { Thread.sleep(0, 100_000); } catch (InterruptedException ignored) {}
                        lockManager.unlock(key);
                    }
                } catch (Throwable t) {
                    err.set(t);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        assertNull(err.get(), "并发 forceUnlock + unlock 不应抛异常: " + err.get());
    }

    @Test
    @DisplayName("forceUnlock + 后续 unlock:仅触发一次 onLockReleased(R5 修复)")
    void test_bug09_multipleUnlockOnlyFiresOnce() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        AtomicInteger released = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockReleased(String k, long held) { released.incrementAndGet(); }
        });
        assertTrue(lockManager.tryLock("k"));
        lockManager.forceUnlock("k");
        // forceUnlock 触发一次 onLockReleased
        assertEquals(1, released.get(), "forceUnlock 应触发一次 onLockReleased");
        // R5 修复:forceUnlock 已 markReleaseEventFired,后续 unlock 走 no-op 路径
        // 不再重复触发 onLockReleased
        lockManager.unlock("k");
        assertEquals(1, released.get(),
                "R5 修复:forceUnlock 后的 unlock 不应再触发 onLockReleased");
        // 多次 unlock 仍然只触发一次
        lockManager.unlock("k");
        lockManager.unlock("k");
        assertEquals(1, released.get(),
                "多次 unlock 应保持 fireReleased 次数不变");
    }

    @Test
    @DisplayName("shutdown 之后 tryAcquireAsync 抛 RejectedExecutionException(R6 修复 R-2)")
    void test_bug10_asyncAfterShutdown() {
        lockManager.shutdown();
        // R6 修复(R-2):asyncExecutor 自定义 handler 改为"shutdown 抛 REE,队列满时调用线程执行"。
        // shutdown 后业务方应显式捕获并降级处理(不再被调用线程同步阻塞)。
        assertThrows(java.util.concurrent.RejectedExecutionException.class,
                () -> lockManager.tryAcquireAsync("k", 1, java.util.concurrent.TimeUnit.SECONDS),
                "R6 修复后 shutdown 应抛 RejectedExecutionException,业务方需显式处理");
    }

    @Test
    @DisplayName("Config 校验:cleanupInterval 极小值(如 1ms)不应崩溃")
    void test_bug11_extremeCleanupInterval() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .cleanupInterval(Duration.ofMillis(1))
                .idleThreshold(Duration.ofMillis(10))
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        Thread.sleep(50);
        assertFalse(lockManager.getActiveKeys().contains("k"),
                "极短清理间隔下空闲 entry 应被清理");
    }

    @Test
    @DisplayName("getStatistics 返回不可变快照,后续操作不影响已返回的快照")
    void test_bug12_statisticsSnapshotIsImmutable() {
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        MonitorMetrics m1 = lockManager.getStatistics();
        long t1 = m1.getTotalLocks();
        for (int i = 0; i < 10; i++) {
            lockManager.tryLock("k");
            lockManager.unlock("k");
        }
        MonitorMetrics m2 = lockManager.getStatistics();
        assertEquals(t1, m1.getTotalLocks(),
                "先获取的快照 m1 不应受后续操作影响");
        assertEquals(t1 + 10, m2.getTotalLocks());
    }

    @Test
    @DisplayName("registerDistributedLock 重复 name 覆盖")
    void test_bug13_distributedLockRegisterOverride() {
        DistributedLock dl1 = new cn.wubo.lock.DistributedLock() {
            @Override public boolean tryLock(String k, long w, TimeUnit u) { return true; }
            @Override public boolean tryLock(String k, long w, long l, TimeUnit u) { return true; }
            @Override public java.util.Optional<cn.wubo.lock.DistributedAcquiredLock> tryAcquire(String k, long w, TimeUnit u) { return java.util.Optional.empty(); }
            @Override public void unlock(String k) {}
            @Override public boolean isLocked(String k) { return false; }
            @Override public boolean forceUnlock(String k) { return true; }
        };
        DistributedLock dl2 = new cn.wubo.lock.DistributedLock() {
            @Override public boolean tryLock(String k, long w, TimeUnit u) { return false; }
            @Override public boolean tryLock(String k, long w, long l, TimeUnit u) { return false; }
            @Override public java.util.Optional<cn.wubo.lock.DistributedAcquiredLock> tryAcquire(String k, long w, TimeUnit u) { return java.util.Optional.empty(); }
            @Override public void unlock(String k) {}
            @Override public boolean isLocked(String k) { return false; }
            @Override public boolean forceUnlock(String k) { return true; }
        };
        lockManager.registerDistributedLock("x", dl1);
        assertSame(dl1, lockManager.getDistributedLock("x").get());
        lockManager.registerDistributedLock("x", dl2);  // 覆盖
        assertSame(dl2, lockManager.getDistributedLock("x").get());
        assertTrue(lockManager.unregisterDistributedLock("x"));
        assertFalse(lockManager.unregisterDistributedLock("x"));
    }

    @Test
    @DisplayName("registerDistributedLock 拒绝 null 参数")
    void test_bug14_distributedLockNull() {
        assertThrows(NullPointerException.class,
                () -> lockManager.registerDistributedLock(null,
                        new cn.wubo.lock.DistributedLock() {
                            @Override public boolean tryLock(String k, long w, TimeUnit u) { return false; }
                            @Override public boolean tryLock(String k, long w, long l, TimeUnit u) { return false; }
                            @Override public java.util.Optional<cn.wubo.lock.DistributedAcquiredLock> tryAcquire(String k, long w, TimeUnit u) { return java.util.Optional.empty(); }
                            @Override public void unlock(String k) {}
                            @Override public boolean isLocked(String k) { return false; }
                            @Override public boolean forceUnlock(String k) { return false; }
                        }));
        assertThrows(NullPointerException.class,
                () -> lockManager.registerDistributedLock("x", null));
        assertThrows(NullPointerException.class, () -> lockManager.getDistributedLock(null));
    }

    @Test
    @DisplayName("cleanupTick 在 tryLock 借锁期间的并发获取不应被永久阻塞")
    void test_bug15_concurrentAcquireDuringCleanup() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(10))
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        // 让一个线程并发 tryLock,与 cleanupTick 借锁竞争
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch done = new CountDownLatch(20);
        AtomicInteger success = new AtomicInteger();
        for (int i = 0; i < 20; i++) {
            pool.submit(() -> {
                try {
                    if (lockManager.tryLock("k", 200, TimeUnit.MILLISECONDS)) {
                        success.incrementAndGet();
                        lockManager.unlock("k");
                    }
                } finally {
                    done.countDown();
                }
            });
            Thread.sleep(5);  // 交错触发
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        assertTrue(success.get() > 0, "至少有一次 tryLock 应成功");
    }

    @Test
    @DisplayName("forceUnlock 在 lockMap 为空(被清理)时调用是 no-op")
    void test_bug16_forceUnlockAfterCleanup() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        // 等清理线程移除
        long deadline = System.currentTimeMillis() + 1000;
        while (lockManager.getActiveKeys().contains("k") && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertFalse(lockManager.getActiveKeys().contains("k"));
        assertDoesNotThrow(() -> lockManager.forceUnlock("k"));
    }

    @Test
    @DisplayName("LockListener 注册/注销 null 行为(R6 修复 A-7:register 抛 NPE,unregister 静默)")
    void test_bug17_listenerNullTolerance() {
        // R6 修复(A-7):registerListener(null) 现在抛 NPE(与 registerDistributedLock 一致)
        assertThrows(NullPointerException.class,
                () -> lockManager.registerListener(null),
                "R6 修复:registerListener(null) 抛 NPE");
        // unregisterListener(null) 仍保持静默 no-op(注销语义对 null 无意义)
        assertDoesNotThrow(() -> lockManager.unregisterListener(null));
        // 注销未注册的实例是 no-op
        LockListener l = new LockListener() {};
        assertDoesNotThrow(() -> lockManager.unregisterListener(l));
    }

    @Test
    @DisplayName("多次注册同一 listener 实例:每个事件触发多次")
    void test_bug18_listenerDuplicateRegistration() {
        AtomicInteger count = new AtomicInteger();
        LockListener l = new LockListener() {
            @Override public void onLockAcquired(String k, long w) { count.incrementAndGet(); }
        };
        lockManager.registerListener(l);
        lockManager.registerListener(l);
        lockManager.registerListener(l);
        lockManager.tryLock("k");
        lockManager.unlock("k");
        // 重复注册 3 次,每个事件触发 3 次
        assertEquals(3, count.get());
    }
}
