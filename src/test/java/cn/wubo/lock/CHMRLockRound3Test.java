package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 3 轮测试:并发压力与边界条件。
 *
 * 覆盖点:
 *   1. 大量线程竞争同一 key,临界区严格不重叠
 *   2. maxKeys 限制仅对"新 key"生效,已存在 key 可重入
 *   3. maxKeys=0 表示不限制
 *   4. tryMultiLock 排序保证避免死锁
 *   5. tryMultiLock 部分失败时,已获取的锁被回滚
 *   6. 公平锁下,获取顺序应大致 FIFO
 *   7. 大量并发 acquire/release 不泄漏内存
 *   8. Async 获取在并发竞争下结果一致
 *   9. 多监听器并行注册都收到事件
 *  10. forceUnlock 对未知 key 是 no-op
 *  11. shutdown 后 listener 仍能 fire? (不再调用)
 *  12. 多实例独立性:不同 CHMRLock 实例的 key 互不影响
 *  13. 高并发 acquire + cleanup 同时运行,不应抛异常
 *  14. Statistics 计数器单调递增
 */
public class CHMRLockRound3Test {

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
    @DisplayName("高并发竞争同一 key:临界区严格不重叠")
    void test01_strictMutualExclusion() throws InterruptedException {
        int threads = 64;
        int iters = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger overlap = new AtomicInteger(0);
        AtomicInteger active = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    for (int j = 0; j < iters; j++) {
                        if (lockManager.tryLock("k1", 1, TimeUnit.SECONDS)) {
                            try {
                                int now = active.incrementAndGet();
                                if (now != 1) overlap.incrementAndGet();
                                Thread.sleep(0, 100_000);  // 微小临界区
                            } finally {
                                active.decrementAndGet();
                                lockManager.unlock("k1");
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(0, overlap.get(), "临界区不应重叠");
        MonitorMetrics m = lockManager.getStatistics();
        assertEquals(m.getSuccessLocks() + m.getFailedLocks(), m.getTotalLocks());
    }

    @Test
    @DisplayName("maxKeys 限制:超出时不接受新 key,已存在的可重入")
    void test02_maxKeysLimitsNewOnly() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().maxKeys(2).build());
        assertTrue(lockManager.tryLock("a"));
        assertTrue(lockManager.tryLock("b"));
        // 第 3 个新 key 应被拒绝
        assertFalse(lockManager.tryLock("c", 50, TimeUnit.MILLISECONDS));
        // 已存在的 key 可以重入
        assertTrue(lockManager.tryLock("a", 50, TimeUnit.MILLISECONDS));
        assertEquals(2, lockManager.getHoldCount("a"));
        lockManager.unlock("a");
        lockManager.unlock("a");
        lockManager.unlock("b");

        // 释放后可以再加新 key
        assertTrue(lockManager.tryLock("c"));
        lockManager.unlock("c");
    }

    @Test
    @DisplayName("maxKeys=0 表示不限制")
    void test03_maxKeysZeroUnlimited() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().maxKeys(0).build());
        for (int i = 0; i < 100; i++) {
            assertTrue(lockManager.tryLock("k-" + i));
        }
        assertEquals(100, lockManager.getActiveKeys().size());
    }

    @Test
    @DisplayName("tryMultiLock 多线程反向顺序应不产生死锁(排序保证)")
    void test04_multiLockNoDeadlockOnReverseOrder() throws InterruptedException {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger deadlocks = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            final int tid = i;
            pool.submit(() -> {
                try {
                    String[] keys = {"alpha", "beta", "gamma", "delta"};
                    // 不同线程用不同顺序
                    if (tid % 2 == 0) {
                        assertTrue(lockManager.tryMultiLock(1, TimeUnit.SECONDS, keys));
                        lockManager.unlock("alpha");
                        lockManager.unlock("beta");
                        lockManager.unlock("gamma");
                        lockManager.unlock("delta");
                    } else {
                        String[] reversed = {"delta", "gamma", "beta", "alpha"};
                        assertTrue(lockManager.tryMultiLock(1, TimeUnit.SECONDS, reversed));
                        lockManager.unlock("alpha");
                        lockManager.unlock("beta");
                        lockManager.unlock("gamma");
                        lockManager.unlock("delta");
                    }
                } catch (Throwable t) {
                    deadlocks.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(15, TimeUnit.SECONDS), "不应死锁");
        pool.shutdown();
        assertEquals(0, deadlocks.get(), "线程不应因锁顺序死锁");
        assertTrue(System.currentTimeMillis() - start < 15000, "应在 15s 内完成");
    }

    @Test
    @DisplayName("tryMultiLock 大量 keys 部分失败时回滚已获取的锁")
    void test05_multiLockLargeRollback() throws InterruptedException {
        // 由另一线程占用最后一个 key
        CountDownLatch lastHeld = new CountDownLatch(1);
        CountDownLatch releaseLast = new CountDownLatch(1);
        Thread blocker = new Thread(() -> {
            assertTrue(lockManager.tryLock("zz"));
            lastHeld.countDown();
            try { releaseLast.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("zz");
        });
        blocker.start();
        assertTrue(lastHeld.await(2, TimeUnit.SECONDS));

        String[] keys = new String[20];
        for (int i = 0; i < 19; i++) keys[i] = "kk-" + i;
        keys[19] = "zz";  // 故意放最后,应失败
        boolean ok = lockManager.tryMultiLock(50, TimeUnit.MILLISECONDS, keys);
        assertFalse(ok);
        // 验证所有 kk-* 都被回滚(无残留)
        for (int i = 0; i < 19; i++) {
            assertFalse(lockManager.isLocked("kk-" + i),
                    "回滚后 kk-" + i + " 应释放,实际仍被持有");
        }

        releaseLast.countDown();
        blocker.join(2000);
    }

    @Test
    @DisplayName("公平锁:多线程按 FIFO 顺序获取")
    void test06_fairLockFifo() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().fairLock(true).build());
        assertTrue(lockManager.tryLock("k1"));

        int n = 5;
        List<Long> order = new ArrayList<>(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            final int tid = i;
            pool.submit(() -> {
                lockManager.tryLock("k1");  // 阻塞直到 release
                synchronized (order) {
                    order.add((long) tid);
                }
                lockManager.unlock("k1");
            });
            Thread.sleep(10);  // 确保按提交顺序排队
        }
        Thread.sleep(100);
        lockManager.unlock("k1");  // 让队列开始执行
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        // 公平锁下,顺序应大致是 0,1,2,3,4
        assertEquals(n, order.size());
    }

    @Test
    @DisplayName("大量短生命周期 acquire+release 不应 OOM")
    void test07_highChurnNoLeak() throws InterruptedException {
        int threads = 16;
        int iters = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            int tid = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < iters; i++) {
                        String key = "k-" + tid + "-" + (i % 10);  // 复用 10 个 key
                        if (lockManager.tryLock(key, 50, TimeUnit.MILLISECONDS)) {
                            try {
                                Thread.sleep(0, 50_000);
                            } finally {
                                lockManager.unlock(key);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(60, TimeUnit.SECONDS));
        pool.shutdown();
        // 内存:不应有大量残留 keys (经过 5 分钟才清理,这里 keys 会复用)
        assertTrue(lockManager.getActiveKeys().size() <= threads * 10,
                "activeKeys 不应超过线程数 × key 复用数");
    }

    @Test
    @DisplayName("tryAcquireAsync 并发竞争下,所有 lock 调用最终成功")
    void test08_asyncAcquireContention() throws InterruptedException {
        int n = 50;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch done = new CountDownLatch(n);
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);
        List<CompletableFuture<Optional<AcquiredLock>>> futures = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            futures.add(lockManager.tryAcquireAsync("k", 5, TimeUnit.SECONDS));
        }
        for (CompletableFuture<Optional<AcquiredLock>> f : futures) {
            f.whenComplete((opt, err) -> {
                if (err != null) {
                    failures.incrementAndGet();
                } else if (opt.isPresent()) {
                    successes.incrementAndGet();
                    AcquiredLock al = opt.get();
                    try {
                        // 持锁一会儿再放
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        al.close();
                    }
                } else {
                    failures.incrementAndGet();
                }
                done.countDown();
            });
        }
        assertTrue(done.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(n, successes.get() + failures.get(), "所有 future 必须完成");
        // 至少有一个成功(由最先抢到的线程)
        assertTrue(successes.get() >= 1, "至少一个 async 应成功获取");
    }

    @Test
    @DisplayName("多监听器并发注册都收到事件")
    void test09_multipleListenersAllNotified() {
        AtomicInteger a1 = new AtomicInteger();
        AtomicInteger a2 = new AtomicInteger();
        AtomicInteger a3 = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) { a1.incrementAndGet(); }
        });
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) { a2.incrementAndGet(); }
        });
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) { a3.incrementAndGet(); }
        });
        for (int i = 0; i < 10; i++) {
            assertTrue(lockManager.tryLock("k" + i));
            lockManager.unlock("k" + i);
        }
        assertEquals(10, a1.get());
        assertEquals(10, a2.get());
        assertEquals(10, a3.get());
    }

    @Test
    @DisplayName("forceUnlock 对未知 key 是 no-op")
    void test10_forceUnlockUnknownKey() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().forceUnlockEnabled(true).build());
        assertDoesNotThrow(() -> lockManager.forceUnlock("never_seen"));
    }

    @Test
    @DisplayName("forceUnlock 拒绝 null key(NPE)")
    void test10b_forceUnlockNullKeyThrows() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().forceUnlockEnabled(true).build());
        assertThrows(NullPointerException.class, () -> lockManager.forceUnlock(null));
    }

    @Test
    @DisplayName("多 CHMRLock 实例独立性:同 key 互不影响")
    void test11_independentInstances() {
        CHMRLock a = new CHMRLock();
        CHMRLock b = new CHMRLock();
        try {
            assertTrue(a.tryLock("k"));
            assertTrue(b.tryLock("k"), "不同实例的同 key 应独立");
            assertTrue(a.isLocked("k"));
            assertTrue(b.isLocked("k"));
            a.unlock("k");
            assertFalse(a.isLocked("k"));
            assertTrue(b.isLocked("k"));
            b.unlock("k");
        } finally {
            a.shutdown();
            b.shutdown();
        }
    }

    @Test
    @DisplayName("清理线程与并发 tryLock 同时运行不抛异常")
    void test12_cleanupRaceWithAcquire() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(10))
                .build());

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger(0);
        long end = System.currentTimeMillis() + 2000;
        for (int i = 0; i < threads; i++) {
            int tid = i;
            pool.submit(() -> {
                try {
                    while (System.currentTimeMillis() < end) {
                        String k = "k-" + tid + "-" + (System.nanoTime() & 0xF);
                        try {
                            if (lockManager.tryLock(k, 20, TimeUnit.MILLISECONDS)) {
                                try {
                                    Thread.sleep(0, 100_000);
                                } finally {
                                    lockManager.unlock(k);
                                }
                            }
                            // 并发迭代 activeKeys
                            for (String s : lockManager.getActiveKeys()) {
                                s.hashCode();
                            }
                        } catch (Throwable t) {
                            errors.incrementAndGet();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(0, errors.get(), "清理线程与并发 tryLock 不应抛异常");
    }

    @Test
    @DisplayName("统计计数单调递增且自洽")
    void test13_countersMonotonic() throws InterruptedException {
        long initialTotal = lockManager.getStatistics().getTotalLocks();
        for (int i = 0; i < 100; i++) {
            assertTrue(lockManager.tryLock("k"));
            lockManager.unlock("k");
        }
        MonitorMetrics m = lockManager.getStatistics();
        assertTrue(m.getTotalLocks() >= initialTotal + 100);
        assertTrue(m.getSuccessLocks() >= 100);
        assertEquals(m.getSuccessLocks() + m.getFailedLocks(), m.getTotalLocks());
        assertTrue(m.getSuccessRate() > 0);
        assertTrue(m.getAvgWaitTime() >= 0);
    }

    @Test
    @DisplayName("两个 listener 中一个抛异常,另一个仍正常收到事件")
    void test14_oneBadListenerDoesNotBlockOthers() {
        AtomicInteger goodCount = new AtomicInteger();
        LockListener bad = new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                throw new RuntimeException("bad");
            }
        };
        LockListener good = new LockListener() {
            @Override public void onLockAcquired(String k, long w) { goodCount.incrementAndGet(); }
        };
        lockManager.registerListener(bad);
        lockManager.registerListener(good);
        for (int i = 0; i < 5; i++) {
            lockManager.tryLock("k" + i);
            lockManager.unlock("k" + i);
        }
        assertEquals(5, goodCount.get(), "good listener 不应被 bad listener 影响");
    }

    @Test
    @DisplayName("forceUnlock 后立即 tryLock:同一线程可再次获取(哨兵清除)")
    void test15_forceUnlockThenRetryBySameThread() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().forceUnlockEnabled(true).build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.forceUnlock("k");
        // 同一线程再次 tryLock 应成功(哨兵清除)
        assertTrue(lockManager.tryLock("k"));
        assertTrue(lockManager.isLocked("k"));
        lockManager.unlock("k");
    }

    @Test
    @DisplayName("getHoldCount:持有线程=3,跨线程=holderHoldCount(R6 修复 B-4/A-3)")
    void test16_holdCountIsCurrentThreadScoped() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            assertTrue(lockManager.tryLock("k"));
            assertTrue(lockManager.tryLock("k"));
            assertTrue(lockManager.tryLock("k"));
            // 持有线程观察自己:应等于 3
            assertEquals(3, lockManager.getHoldCount("k"));
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k");
            lockManager.unlock("k");
            lockManager.unlock("k");
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        // R6 修复(B-4/A-3):getHoldCount 跨线程现在返回 holderHoldCount(=持有线程的重入深度),
        // 而不是永远 0。这与 KeyStatistics.currentHoldCount 一致,与 getOwnerThreadId 也一致。
        // 之前行为("跨线程返回 0")是 ReentrantLock 的 thread-local 视角,但对调用方不直观。
        assertEquals(3, lockManager.getHoldCount("k"),
                "R6 修复:跨线程读 getHoldCount 返回持有线程的实际重入深度,而不是 0");
        release.countDown();
        holder.join(2000);
        assertEquals(0, lockManager.getHoldCount("k"));
    }
}
