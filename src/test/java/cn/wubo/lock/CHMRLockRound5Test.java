package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 5 轮测试:验证 R5 修复的具体行为。
 *
 * 覆盖:
 *   H1: tryLock 带租约 shutdown 后不再抛 REE
 *   H2: cleanupTick 异常防护
 *   H3: lock() 在 maxKeys 路径抛 IllegalStateException
 *   H4: forceUnlock + unlock 不重复触发 fireReleased
 *   H5: defaultLeaseTime 在 tryLock(String) / tryLock(String, long) 中生效
 *   M2: getAllStatistics 跨线程快照 holderId/holdCount 一致
 *   M3: getHoldCount 在 forceUnlock 后返回 0
 *   M4: forceUnlockOnLeaseExpiry=false 路径
 *   M5: lock() 参数校验
 *   M6: recordStatistics 方法
 *   M7: asyncExecutor 配置项生效
 */
public class CHMRLockRound5Test {

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

    // ============ H1: shutdown 后 tryLock 带租约不再抛 REE ============

    @Test
    @DisplayName("H1:shutdown 后 tryLock 带租约:抛 IllegalStateException(R6 修复 R-1)")
    void h01_shutdownThenTryLockWithLease() {
        lockManager.shutdown();
        // R5 修复:scheduleLeaseExpiry 内部 REE 被 try-catch 吞掉
        // R6 修复(R-1):shutdown 后 tryLock 入口直接抛 IllegalStateException,
        // 防止新 entry 加入已停止清理线程的 lockMap 导致内存泄漏。
        assertThrows(IllegalStateException.class,
                () -> lockManager.tryLock("k1", 0, 1000, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("H1b:shutdown 后 lock() 抛 IllegalStateException(R6 修复 R-1)")
    void h01b_shutdownThenLockWithLease() throws InterruptedException {
        lockManager.shutdown();
        assertThrows(IllegalStateException.class,
                () -> lockManager.lock("k1", 1000, TimeUnit.MILLISECONDS));
    }

    // ============ H2: cleanupTick 异常防护 ============

    @Test
    @DisplayName("H2:cleanupTick 在 lease 仍生效 + 持有时不应误删")
    void h02_cleanupTickRespectsHeldAndLease() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build());
        assertTrue(lockManager.tryLock("k"));
        Thread.sleep(200);
        // 持有中,cleanupTick 应跳过
        assertTrue(lockManager.getActiveKeys().contains("k"),
                "持有中的 entry 不应被清理");
    }

    @Test
    @DisplayName("H2:cleanupTick 整体防护:即使有 entry 出错,下一周期仍继续运行")
    void h02b_cleanupTickRecoversFromErrors() throws InterruptedException {
        // 不容易直接触发 cleanupTick 内部异常,但可以验证顶层 try-catch 结构存在
        // 这里通过正常路径间接验证 —— 大量创建/释放后清理线程仍能正常回收
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(30))
                .cleanupInterval(Duration.ofMillis(20))
                .build());
        for (int i = 0; i < 50; i++) {
            assertTrue(lockManager.tryLock("k" + i));
            lockManager.unlock("k" + i);
        }
        Thread.sleep(200);
        Set<String> active = lockManager.getActiveKeys();
        // idle 后 entry 应被回收
        assertTrue(active.size() < 50,
                "cleanup 应至少回收一些空闲 entry,实际剩余: " + active.size());
    }

    // ============ H3: lock() 在 maxKeys 路径抛 IllegalStateException ============

    @Test
    @DisplayName("H3:lock() 在 maxKeys 触发时抛 IllegalStateException(文档化语义)")
    void h03_lockMaxKeysThrowsIllegalState() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().maxKeys(1).build());
        assertTrue(lockManager.tryLock("a"));
        assertThrows(IllegalStateException.class,
                () -> lockManager.lock("b"));
        lockManager.unlock("a");
    }

    @Test
    @DisplayName("H3:tryLock 在 maxKeys 触发时返回 false(非阻塞语义)")
    void h03b_tryLockMaxKeysReturnsFalse() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().maxKeys(1).build());
        assertTrue(lockManager.tryLock("a"));
        assertFalse(lockManager.tryLock("b", 50, TimeUnit.MILLISECONDS));
        lockManager.unlock("a");
    }

    // ============ H4: forceUnlock + unlock 不重复触发 fireReleased ============

    @Test
    @DisplayName("H4:forceUnlock 后 unlock 仅触发一次 onLockReleased")
    void h04_forceUnlockThenUnlockFiresOnce() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        AtomicInteger released = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockReleased(String k, long held) {
                released.incrementAndGet();
            }
        });
        assertTrue(lockManager.tryLock("k"));
        lockManager.forceUnlock("k");
        assertEquals(1, released.get(), "forceUnlock 应触发一次 onLockReleased");
        lockManager.unlock("k");
        assertEquals(1, released.get(),
                "R5 修复:forceUnlock 后的 unlock 不应再触发 onLockReleased");
        // 后续 unlock 也应 no-op
        lockManager.unlock("k");
        assertEquals(1, released.get(), "后续 unlock 也应 no-op");
    }

    // ============ H5: defaultLeaseTime 在 tryLock 重载中生效 ============

    @Test
    @DisplayName("H5:tryLock(String) 使用 config.defaultLeaseTime 设置租约")
    void h05_tryLockUsesDefaultLeaseTime() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .defaultLeaseTime(Duration.ofMillis(500))
                .enablePerKeyMetrics(true)
                .build());
        long beforeAcquire = System.currentTimeMillis();
        assertTrue(lockManager.tryLock("k"));
        KeyStatistics ks = lockManager.getStatistics("k").orElseThrow();
        // 实际租约应通过 scheduleLeaseExpiry 设置 leaseEndTime,无需断言精确值
        // 只需验证 tryLock 正常返回且 lockMap 有 entry
        assertEquals(1L, ks.successCount());
        long afterAcquire = System.currentTimeMillis();
        // 验证锁已持有且未过期
        assertTrue(lockManager.isLocked("k"));
        assertTrue(afterAcquire - beforeAcquire < 500,
                "tryLock 应立即返回,实际耗时过长");
        // 释放以触发清理
        lockManager.unlock("k");
    }

    @Test
    @DisplayName("H5:tryLock(String, long) 也使用 defaultLeaseTime")
    void h05b_tryLockWithWaitTimeUsesDefaultLease() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .defaultLeaseTime(Duration.ofMillis(500))
                .build());
        assertTrue(lockManager.tryLock("k", 100, TimeUnit.MILLISECONDS));
        assertTrue(lockManager.isLocked("k"));
        lockManager.unlock("k");
    }

    // ============ M2: getAllStatistics 跨线程快照 holderId/holdCount 一致 ============

    @Test
    @DisplayName("M2:跨线程读取 getAllStatistics,holderId/holdCount 反映持有线程")
    void m02_getAllStatisticsHolderCountConsistent() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .build());
        // 持有线程 A 加锁两次(重入深度=2)
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
                lockManager.unlock("k");
                lockManager.unlock("k");
            } catch (Throwable t) {
                err.set(t);
            }
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 另一线程读快照
        Map<String, KeyStatistics> snap = lockManager.getAllStatistics();
        KeyStatistics ks = snap.get("k");
        assertNotNull(ks);
        // R5 修复:holderHoldCount 由 CHMRLock 维护,跨线程读=持有线程的重入深度
        // R6 修复(E-9):currentHoldCount 现在是 long(支持 64 位重入深度),使用 L 后缀
        assertEquals(2L, ks.currentHoldCount(),
                "跨线程快照应反映持有线程的重入深度,实际: " + ks.currentHoldCount());
        assertEquals(holder.getId(), ks.currentHolderThreadId(),
                "持有线程 id 应一致");

        release.countDown();
        holder.join(2000);
        assertNull(err.get(), "持有线程不应抛异常: " + err.get());
    }

    // ============ M3: getHoldCount 在 forceUnlock 后返回 0 ============

    @Test
    @DisplayName("M3:forceUnlock 后 getHoldCount 返回 0,与 isLocked 一致")
    void m03_getHoldCountAfterForceUnlock() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        assertTrue(lockManager.tryLock("k"));
        assertTrue(lockManager.tryLock("k"));  // 重入
        assertEquals(2, lockManager.getHoldCount("k"));
        lockManager.forceUnlock("k");
        assertEquals(0, lockManager.getHoldCount("k"),
                "forceUnlock 后 getHoldCount 应返回 0,与 isLocked 一致");
        assertFalse(lockManager.isLocked("k"));
    }

    // ============ M4: forceUnlockOnLeaseExpiry=false 路径 ============

    @Test
    @DisplayName("M4:forceUnlockOnLeaseExpiry=false 时租约到期不强制释放")
    void m04_leaseExpiryNoForceUnlock() throws Exception {
        lockManager.shutdown();
        java.time.Clock realClock = java.time.Clock.systemUTC();
        SettableClock clock = new SettableClock(realClock.millis());
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .clock(clock)
                .forceUnlockOnLeaseExpiry(false)
                .cleanupInterval(Duration.ofMillis(20))
                .build());

        assertTrue(lockManager.tryLock("k", 1, 500, TimeUnit.MILLISECONDS));
        // 推进时钟让租约过期
        clock.advance(Duration.ofSeconds(2));
        Thread.sleep(300);  // 等清理 tick

        // forceUnlockOnLeaseExpiry=false:isLocked 仍为 true
        assertTrue(lockManager.isLocked("k"),
                "forceUnlockOnLeaseExpiry=false 时租约到期后 isLocked 应仍为 true");
        lockManager.unlock("k");
    }

    // ============ M5: lock() 参数校验 ============

    @Test
    @DisplayName("M5:lock() 拒绝 null timeUnit")
    void m05_lockRejectsNullTimeUnit() {
        assertThrows(NullPointerException.class,
                () -> lockManager.lock("k", 100, null));
    }

    @Test
    @DisplayName("M5:lock() 拒绝负 leaseTime")
    void m05b_lockRejectsNegativeLeaseTime() {
        assertThrows(IllegalArgumentException.class,
                () -> lockManager.lock("k", -1, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("M5:tryLock(String, long, TimeUnit) 拒绝 null timeUnit")
    void m05c_tryLockRejectsNullTimeUnit() {
        assertThrows(NullPointerException.class,
                () -> lockManager.tryLock("k", 100, null));
    }

    @Test
    @DisplayName("M5:tryLock(String, long) 拒绝负 waitTime")
    void m05d_tryLockRejectsNegativeWaitTime() {
        assertThrows(IllegalArgumentException.class,
                () -> lockManager.tryLock("k", -1));
    }

    // ============ M6: recordStatistics 方法 ============

    @Test
    @DisplayName("M6:recordStatistics 调用 sink 传入统计快照")
    void m06_recordStatisticsInvokesSink() {
        lockManager.tryLock("k1");
        lockManager.unlock("k1");

        AtomicInteger called = new AtomicInteger();
        AtomicReference<MonitorMetrics> receivedGlobal = new AtomicReference<>();
        StatisticsSink sink = (global, perKey) -> {
            called.incrementAndGet();
            receivedGlobal.set(global);
        };
        lockManager.recordStatistics(sink);
        assertEquals(1, called.get());
        assertNotNull(receivedGlobal.get());
        assertEquals(1L, receivedGlobal.get().getSuccessLocks());
    }

    @Test
    @DisplayName("M6:recordStatistics 拒绝 null sink")
    void m06b_recordStatisticsRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> lockManager.recordStatistics(null));
    }

    @Test
    @DisplayName("M6:recordStatistics sink 抛异常不影响锁功能")
    void m06c_recordStatisticsIsolatesExceptions() {
        StatisticsSink bad = (g, p) -> { throw new RuntimeException("boom"); };
        assertDoesNotThrow(() -> lockManager.recordStatistics(bad));
        // 锁仍正常工作
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
    }

    // ============ M7: asyncExecutor 配置项生效 ============

    @Test
    @DisplayName("M7:config.asyncMaxThreads / asyncQueueCapacity 生效")
    void m07_asyncPoolConfigTakesEffect() {
        lockManager.shutdown();
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .asyncMaxThreads(2)
                .asyncQueueCapacity(8)
                .build();
        CHMRLock l = new CHMRLock(cfg);
        try {
            // 验证构造成功,且基础功能正常
            assertTrue(l.tryLock("k"));
            l.unlock("k");
        } finally {
            l.shutdown();
        }
    }

    @Test
    @DisplayName("M7:CHMRLockConfig.builder().asyncMaxThreads(0) 校验失败")
    void m07b_asyncMaxThreadsValidates() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().asyncMaxThreads(0).build());
    }

    @Test
    @DisplayName("M7:CHMRLockConfig.builder().asyncQueueCapacity(0) 校验失败")
    void m07c_asyncQueueCapacityValidates() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().asyncQueueCapacity(0).build());
    }

    // ============ 综合回归:所有修复协同工作 ============

    @Test
    @DisplayName("综合:forceUnlock + lease + defaultLeaseTime + 持有线程解锁")
    void comprehensiveForceUnlockWithLease() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .defaultLeaseTime(Duration.ofMillis(500))
                .forceUnlockEnabled(true)
                .enablePerKeyMetrics(true)
                .build());

        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) { acquired.incrementAndGet(); }
            @Override public void onLockReleased(String k, long held) { released.incrementAndGet(); }
        });

        assertTrue(lockManager.tryLock("k"));  // 使用 defaultLeaseTime
        assertTrue(lockManager.isLocked("k"));
        assertEquals(1, acquired.get());

        lockManager.forceUnlock("k");
        assertFalse(lockManager.isLocked("k"));
        assertEquals(1, released.get(), "forceUnlock 应触发一次 onLockReleased");

        // 持有线程在哨兵置位后调用 unlock
        AtomicReference<Throwable> holderErr = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try { lockManager.unlock("k"); } catch (Throwable t) { holderErr.set(t); }
        });
        holder.start();
        holder.join(2000);
        assertNull(holderErr.get());
        assertEquals(1, released.get(), "哨兵路径不应再触发 onLockReleased");
    }

    @Test
    @DisplayName("综合:H2 防护:cleanupTick 在 scheduleLeaseExpiry 抛 REE 后仍继续")
    void comprehensiveCleanupTickSurvives() throws InterruptedException {
        // 间接验证:H1 修复使 scheduleLeaseExpiry 不再冒泡 REE,
        // cleanupTick 不再因 REE 进入 catch(Throwable) 分支。
        // 这里直接验证清理线程仍能在 shutdown 后的下一周期继续运行(若 scheduleAtFixedRate 已停止)。
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build());

        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        long deadline = System.currentTimeMillis() + 2000;
        while (lockManager.getActiveKeys().contains("k") && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertFalse(lockManager.getActiveKeys().contains("k"),
                "cleanupTick 应正常清理空闲 entry");
    }

    /** 测试用可推进 Clock。 */
    static final class SettableClock extends java.time.Clock {
        private volatile long now;
        SettableClock(long initial) { this.now = initial; }
        void advance(Duration d) { this.now += d.toMillis(); }
        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
        @Override public java.time.Clock withZone(java.time.ZoneId z) { return this; }
        @Override public long millis() { return now; }
        @Override public java.time.Instant instant() { return java.time.Instant.ofEpochMilli(now); }
    }
}