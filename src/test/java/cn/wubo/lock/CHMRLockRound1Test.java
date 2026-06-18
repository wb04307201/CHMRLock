package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 1 轮测试:基础功能与并发安全性。
 * 覆盖点:
 *   1. tryLock/unlock 基本互斥语义
 *   2. ReentrantLock 重入语义
 *   3. 参数校验 (null / 负数)
 *   4. 多次 unlock 行为
 *   5. 跨线程 unlock 异常
 *   6. 统计准确性 (并发压力下)
 *   7. 多 key 独立性
 *   8. isHeldByCurrentThread / getHoldCount
 *   9. getActiveKeys 反映真实状态
 *  10. waitTime=0 非阻塞语义
 */
public class CHMRLockRound1Test {

    private CHMRLock lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new CHMRLock();
    }

    @AfterEach
    void tearDown() {
        lockManager.shutdown();
    }

    @Test
    @DisplayName("单线程基本 tryLock/unlock 闭环")
    void test01_basicTryUnlockRoundTrip() {
        assertTrue(lockManager.tryLock("k1"));
        assertTrue(lockManager.isLocked("k1"));
        assertTrue(lockManager.isHeldByCurrentThread("k1"));
        assertEquals(1, lockManager.getHoldCount("k1"));
        lockManager.unlock("k1");
        assertFalse(lockManager.isLocked("k1"));
    }

    @Test
    @DisplayName("重入:同一线程可多次获取同一 key")
    void test02_reentrancy() {
        assertTrue(lockManager.tryLock("k1"));
        assertTrue(lockManager.tryLock("k1"));
        assertTrue(lockManager.tryLock("k1"));
        assertEquals(3, lockManager.getHoldCount("k1"));
        lockManager.unlock("k1");
        assertEquals(2, lockManager.getHoldCount("k1"));
        lockManager.unlock("k1");
        assertEquals(1, lockManager.getHoldCount("k1"));
        lockManager.unlock("k1");
        assertFalse(lockManager.isLocked("k1"));
    }

    @Test
    @DisplayName("null/负数参数应抛出异常")
    void test03_parameterValidation() {
        assertThrows(NullPointerException.class, () -> lockManager.tryLock(null));
        assertThrows(NullPointerException.class, () -> lockManager.tryLock(null, 10, TimeUnit.MILLISECONDS));
        assertThrows(NullPointerException.class, () -> lockManager.tryLock("k", 10, null));
        assertThrows(NullPointerException.class, () -> lockManager.unlock(null));
        assertThrows(NullPointerException.class, () -> lockManager.isLocked(null));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("k", -1, TimeUnit.MILLISECONDS));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("k", -1, -1, TimeUnit.MILLISECONDS));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("k", 10, -1, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("从未加锁的 key 上 unlock 不抛异常(silent no-op)")
    void test04_unlockNeverLockedKey() {
        // 文档承诺:unlock 对从未加锁的 key 是 silent no-op
        assertDoesNotThrow(() -> lockManager.unlock("never_locked"));
        // 连续多次 no-op
        assertDoesNotThrow(() -> {
            lockManager.unlock("never_locked");
            lockManager.unlock("never_locked");
        });
    }

    @Test
    @DisplayName("跨线程 unlock 抛 IllegalMonitorStateException")
    void test05_crossThreadUnlockThrows() throws InterruptedException {
        assertTrue(lockManager.tryLock("k1"));
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                lockManager.unlock("k1");
            } catch (Throwable e) {
                err.set(e);
            }
        });
        t.start();
        t.join(2000);
        assertNotNull(err.get(), "跨线程 unlock 应抛异常");
        assertInstanceOf(IllegalMonitorStateException.class, err.get(),
                "应抛 IllegalMonitorStateException,实际: " + err.get().getClass());
        // 清理:由持有线程释放
        lockManager.unlock("k1");
    }

    @Test
    @DisplayName("两线程串行:第二个线程必须等到第一个释放后才能获得")
    void test06_twoThreadsSerialization() throws InterruptedException {
        AtomicInteger overlap = new AtomicInteger(0);
        AtomicInteger active = new AtomicInteger(0);
        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch secondCanEnter = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try {
                assertTrue(lockManager.tryLock("k1"));
                active.incrementAndGet();
                firstInside.countDown();
                try {
                    secondCanEnter.await(2, TimeUnit.SECONDS);
                    // 持有 200ms,确保 t2 阻塞
                    Thread.sleep(200);
                } finally {
                    active.decrementAndGet();
                    lockManager.unlock("k1");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                firstInside.await(2, TimeUnit.SECONDS);
                assertTrue(lockManager.tryLock("k1", 2, TimeUnit.SECONDS));
                active.incrementAndGet();
                try {
                    // 进入时 active 必须为 0 (说明 t1 已释放)
                    if (active.get() != 1) overlap.incrementAndGet();
                } finally {
                    active.decrementAndGet();
                    lockManager.unlock("k1");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                finished.countDown();
            }
        });

        t1.start();
        t2.start();
        secondCanEnter.countDown();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(0, overlap.get(), "临界区不应重叠");
        assertTrue(lockManager.getStatistics().getSuccessLocks() >= 2);
    }

    @Test
    @DisplayName("waitTime=0 非阻塞语义:被占用时立即返回 false")
    void test07_nonBlockingTryLock() throws InterruptedException {
        CountDownLatch holderEntered = new CountDownLatch(1);
        CountDownLatch holderCanExit = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            assertTrue(lockManager.tryLock("k1"));
            holderEntered.countDown();
            try {
                holderCanExit.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lockManager.unlock("k1");
            }
        });
        holder.start();
        assertTrue(holderEntered.await(2, TimeUnit.SECONDS));

        long t0 = System.nanoTime();
        boolean got = lockManager.tryLock("k1", 0, TimeUnit.MILLISECONDS);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertFalse(got, "waitTime=0 应立即返回 false");
        assertTrue(elapsedMs < 200, "非阻塞 tryLock 不应等待,实际 " + elapsedMs + " ms");

        holderCanExit.countDown();
        holder.join(2000);
    }

    @Test
    @DisplayName("多 key 独立:k1 持有不影响 k2 获取")
    void test08_multipleKeysIndependent() {
        assertTrue(lockManager.tryLock("k1"));
        assertTrue(lockManager.tryLock("k2"));
        assertTrue(lockManager.tryLock("k3"));
        assertEquals(3, lockManager.getActiveKeys().size());
        Set<String> keys = lockManager.getActiveKeys();
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
        assertTrue(keys.contains("k3"));
    }

    @Test
    @DisplayName("统计准确性:高并发竞争下 success + failed == total")
    void test09_statisticsAccuracyUnderContention() throws InterruptedException {
        int threads = 20;
        int iters = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    for (int j = 0; j < iters; j++) {
                        if (lockManager.tryLock("k1", 20, TimeUnit.MILLISECONDS)) {
                            try {
                                Thread.sleep(1);
                            } finally {
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

        MonitorMetrics m = lockManager.getStatistics();
        assertEquals(threads * iters, m.getTotalLocks());
        assertEquals(m.getSuccessLocks() + m.getFailedLocks(), m.getTotalLocks());
        assertTrue(m.getSuccessLocks() > 0);
        assertTrue(m.getFailedLocks() > 0, "高并发下应有超时失败");
        assertTrue(m.getSuccessRate() > 0 && m.getSuccessRate() < 1.0);
        assertTrue(m.getAvgWaitTime() >= 0);
        assertTrue(m.getTotalWaitTime() > 0);
    }

    @Test
    @DisplayName("getActiveKeys 反映 unlock 后的清理")
    void test10_activeKeysAfterRelease() throws InterruptedException {
        assertTrue(lockManager.tryLock("k1"));
        assertTrue(lockManager.tryLock("k2"));
        assertEquals(2, lockManager.getActiveKeys().size());
        lockManager.unlock("k1");
        lockManager.unlock("k2");

        // 立即检查:清理线程尚未运行,key 仍存在(不是基于 isLocked)
        // 文档:getActiveKeys 返回 lockMap.keySet(),不被 unlock 立即删除
        // 这意味着 unlock 不主动从 map 移除 — 这是设计选择
        Set<String> keys = lockManager.getActiveKeys();
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));

        // 等待清理线程(默认 5 分钟空闲 + 1 秒周期)
        // 不等待那么久 — 改为触发配置使快速清理:
        // 简单做:关闭当前实例,新建一个使用快速清理间隔的实例
        lockManager.shutdown();
        CHMRLock fast = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(java.time.Duration.ofMillis(50))
                .cleanupInterval(java.time.Duration.ofMillis(20))
                .build());
        try {
            assertTrue(fast.tryLock("k1"));
            fast.unlock("k1");
            // 等清理跑至少一次
            Thread.sleep(150);
            assertFalse(fast.getActiveKeys().contains("k1"),
                    "空闲 entry 应被清理,实际 activeKeys=" + fast.getActiveKeys());
        } finally {
            fast.shutdown();
        }
    }

    @Test
    @DisplayName("并发 add/remove activeKeys 不应抛 ConcurrentModificationException")
    void test11_activeKeysConcurrentIteration() throws InterruptedException {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger cmex = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            int id = i;
            pool.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        String key = "k-" + id + "-" + j;
                        try {
                            if (lockManager.tryLock(key, 5, TimeUnit.MILLISECONDS)) {
                                try {
                                    // 在持锁的同时迭代 activeKeys
                                    for (String s : lockManager.getActiveKeys()) {
                                        s.hashCode();
                                    }
                                } finally {
                                    lockManager.unlock(key);
                                }
                            }
                        } catch (java.util.ConcurrentModificationException e) {
                            cmex.incrementAndGet();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(0, cmex.get(),
                "getActiveKeys 返回 unmodifiable 包装,迭代期间底层变更应不抛 CMEX");
    }

    @Test
    @DisplayName("unmodifiable getActiveKeys 拒绝修改")
    void test12_activeKeysUnmodifiable() {
        assertTrue(lockManager.tryLock("k1"));
        Set<String> keys = lockManager.getActiveKeys();
        assertThrows(UnsupportedOperationException.class, () -> keys.add("foo"));
        assertThrows(UnsupportedOperationException.class, () -> keys.remove("k1"));
    }

    @Test
    @DisplayName("大量不同 key 并发 tryLock,每个 key 应恰好被一个线程持有一段时间")
    void test13_manyKeysIndependentUnderContention() throws InterruptedException {
        int keyCount = 200;
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger races = new AtomicInteger(0);

        List<String> keys = new java.util.ArrayList<>();
        for (int i = 0; i < keyCount; i++) keys.add("k-" + i);

        for (int t = 0; t < threads; t++) {
            int tid = t;
            pool.submit(() -> {
                try {
                    for (int j = 0; j < keyCount; j++) {
                        String key = keys.get((tid + j) % keyCount);
                        if (lockManager.tryLock(key, 50, TimeUnit.MILLISECONDS)) {
                            try {
                                if (lockManager.isHeldByCurrentThread(key)) {
                                    // ok
                                } else {
                                    races.incrementAndGet();
                                }
                            } finally {
                                lockManager.unlock(key);
                            }
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(0, races.get(), "每个 key 在持锁期间应只有一个线程持有");
    }

    @Test
    @DisplayName("AcquiredLock.tryWithResources:关闭后锁被释放")
    void test14_acquiredLockReleases() {
        Optional_test();
    }

    private void Optional_test() {
        java.util.Optional<AcquiredLock> opt = lockManager.tryAcquire("k1");
        assertTrue(opt.isPresent());
        AcquiredLock al = opt.get();
        assertEquals("k1", al.key());
        assertTrue(al.isValid());
        assertTrue(lockManager.isLocked("k1"));
        al.close();
        assertFalse(lockManager.isLocked("k1"));
        assertFalse(al.isValid());
    }

    @Test
    @DisplayName("AcquiredLock.close() 幂等")
    void test15_acquiredLockCloseIdempotent() {
        java.util.Optional<AcquiredLock> opt = lockManager.tryAcquire("k1");
        assertTrue(opt.isPresent());
        AcquiredLock al = opt.get();
        assertDoesNotThrow(() -> {
            al.close();
            al.close();
            al.close();
        });
        assertFalse(lockManager.isLocked("k1"));
    }

    @Test
    @DisplayName("非可重入 tryAcquire:同线程再次调用直接返回 empty(不等待)")
    void test16_tryAcquireNonReentrant() {
        java.util.Optional<AcquiredLock> first = lockManager.tryAcquire("k1");
        assertTrue(first.isPresent());
        long t0 = System.nanoTime();
        java.util.Optional<AcquiredLock> second = lockManager.tryAcquire("k1");
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertFalse(second.isPresent(), "同线程二次获取应直接返回 empty");
        assertTrue(elapsedMs < 100, "应 fast-fail,实际 " + elapsedMs + " ms");
        first.get().close();
    }

    @Test
    @DisplayName("空集合 keys 参数:tryMultiLock 返回 true")
    void test17_multiLockEmpty() {
        assertTrue(lockManager.tryMultiLock());
        assertTrue(lockManager.tryMultiLock(new String[0]));
    }

    @Test
    @DisplayName("默认构造函数 + shutdown 至少能正常创建和关闭")
    void test18_defaultConfigShutdown() {
        CHMRLock m = new CHMRLock();
        assertFalse(m.isShutdown());
        m.shutdown();
        assertTrue(m.isShutdown());
        assertDoesNotThrow(m::shutdown);
        assertDoesNotThrow(m::close);
    }
}
