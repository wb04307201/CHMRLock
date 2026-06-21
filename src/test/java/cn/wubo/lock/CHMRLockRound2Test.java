package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 2 轮测试:资源管理与生命周期。
 *
 * 覆盖点:
 *   1. 清理线程按预期清理 idle entries
 *   2. 持有中的 entries 不被清理
 *   3. shutdown 幂等 + 清理线程终止
 *   4. shutdown 后再调用 tryLock 应抛异常或按文档行为
 *   5. shutdown 后 active keys 清空
 *   6. 租约到期后 isLocked=false,但 unlock 不抛跨线程异常
 *   7. forceUnlock 跨线程释放
 *   8. AcquiredLock 在 shutdown 后调 close 安全
 *   9. 监听器注册/注销、重复注册、null 容忍
 *  10. 监听器抛异常隔离
 *  11. Clock 注入:固定时钟推进模拟租约过期
 *  12. 多次 shutdown 不应抛异常
 */
public class CHMRLockRound2Test {

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
    @DisplayName("短空闲阈值下,unlock 后 entry 应在数个清理周期内被回收")
    void test01_cleanupRemovesIdleEntries() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(200))
                .cleanupInterval(Duration.ofMillis(50))
                .build());
        assertTrue(lockManager.tryLock("ephemeral"));
        lockManager.unlock("ephemeral");
        assertTrue(lockManager.getActiveKeys().contains("ephemeral"));
        long deadline = System.currentTimeMillis() + 3000;
        while (lockManager.getActiveKeys().contains("ephemeral") && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertFalse(lockManager.getActiveKeys().contains("ephemeral"),
                "空闲 entry 应被清理");
    }

    @Test
    @DisplayName("持有中的 entry 永远不应被清理(尽管超过 idleThreshold)")
    void test02_heldEntriesNotCleanedUp() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(30))
                .build());
        assertTrue(lockManager.tryLock("held"));
        Thread.sleep(300);
        assertTrue(lockManager.getActiveKeys().contains("held"),
                "持有中的 entry 不应被清理");
    }

    @Test
    @DisplayName("shutdown 是幂等的,多次调用安全")
    void test03_shutdownIdempotent() {
        assertFalse(lockManager.isShutdown());
        lockManager.shutdown();
        assertTrue(lockManager.isShutdown());
        assertDoesNotThrow(() -> {
            lockManager.shutdown();
            lockManager.shutdown();
            lockManager.close();
            lockManager.close();
        });
    }

    @Test
    @DisplayName("shutdown 后 activeKeys 为空")
    void test04_shutdownClearsActiveKeys() {
        lockManager.tryLock("k1");
        lockManager.tryLock("k2");
        assertEquals(2, lockManager.getActiveKeys().size());
        lockManager.shutdown();
        assertTrue(lockManager.getActiveKeys().isEmpty());
    }

    @Test
    @DisplayName("shutdown 后调用 tryLock:抛 IllegalStateException(R6 修复 R-1)")
    void test05_shutdownThenTryLock() {
        lockManager.shutdown();
        // R6 修复(R-1):shutdown 后 tryLock 入口直接抛 IllegalStateException,
        // 防止新 entry 加入已停止清理线程的 lockMap 导致内存泄漏。
        // 之前 R5 行为是"不抛 + 返回 true + entry 永远驻留",现在改为显式失败。
        assertThrows(IllegalStateException.class,
                () -> lockManager.tryLock("k1", 0, 1000, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("持有期间被清理(不可能) — 验证清理逻辑正确跳过")
    void test06_cleanupSkipsHeld() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build());
        CountDownLatch holding = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            assertTrue(lockManager.tryLock("long"));
            holding.countDown();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lockManager.unlock("long");
            }
        });
        holder.start();
        assertTrue(holding.await(2, TimeUnit.SECONDS));
        Thread.sleep(300);
        assertTrue(lockManager.getActiveKeys().contains("long"));
        holder.join(2000);
    }

    @Test
    @DisplayName("租约到期:isLocked 返回 false,但 unlock 由原线程完成时安全")
    void test07_leaseExpiryIsLockedBehavior() throws Exception {
        lockManager.shutdown();
        // 用固定时钟推进模拟时间流逝
        java.time.Clock realClock = java.time.Clock.systemUTC();
        SettableClock clock = new SettableClock(realClock.millis());
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .clock(clock)
                .cleanupInterval(Duration.ofMillis(20))
                .build());

        AtomicReference<Throwable> unlockErr = new AtomicReference<>();
        CountDownLatch holding = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            try {
                assertTrue(lockManager.tryLock("lease", 1, 500, TimeUnit.MILLISECONDS));
                holding.countDown();
                Thread.sleep(2000);  // 远超租约 500ms
                lockManager.unlock("lease");
            } catch (Throwable t) {
                unlockErr.set(t);
            }
        });
        holder.start();
        assertTrue(holding.await(2, TimeUnit.SECONDS));

        // 推进时钟让租约过期 (时钟 + 1000ms 跨越租约边界)
        clock.advance(Duration.ofMillis(1000));
        Thread.sleep(300); // 等清理 tick 触发到期任务

        assertFalse(lockManager.isLocked("lease"),
                "租约到期后 isLocked 应返回 false");
        holder.join(3000);
        assertNull(unlockErr.get(),
                "原持有线程 unlock 不应抛异常,实际: " + unlockErr.get());
    }

    @Test
    @DisplayName("forceUnlock:跨线程强制释放,isLocked 立即 false")
    void test08_forceUnlockCrossThread() throws Exception {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .build());
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicReference<Throwable> holderErr = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                assertTrue(lockManager.tryLock("fk"));
                holding.countDown();
                assertTrue(released.await(5, TimeUnit.SECONDS));
                // 释放:可能已被 forceUnlock,走哨兵路径,不应抛
                lockManager.unlock("fk");
            } catch (Throwable t) {
                holderErr.set(t);
            }
        });
        holder.start();
        assertTrue(holding.await(2, TimeUnit.SECONDS));

        lockManager.forceUnlock("fk");
        assertFalse(lockManager.isLocked("fk"));
        released.countDown();
        holder.join(2000);
        assertNull(holderErr.get(), "持有线程在 forceUnlock 后 unlock 不应抛: " + holderErr.get());
    }

    @Test
    @DisplayName("forceUnlock 禁用时抛 UnsupportedOperationException")
    void test09_forceUnlockDisabledThrows() {
        assertThrows(UnsupportedOperationException.class,
                () -> lockManager.forceUnlock("k"));
    }

    @Test
    @DisplayName("监听器注册/注销/重复注册")
    void test10_listenerRegisterUnregister() {
        LockListener l1 = new LockListener() {
            @Override public void onLockAcquired(String k, long w) {}
        };
        LockListener l2 = new LockListener() {};
        lockManager.registerListener(l1);
        lockManager.registerListener(l2);
        lockManager.registerListener(l1);  // 重复注册(应允许)
        lockManager.unregisterListener(l1);
        // 注销 null 忽略
        assertDoesNotThrow(() -> lockManager.unregisterListener(null));
        // 注销未注册的实例是 no-op
        assertDoesNotThrow(() -> lockManager.unregisterListener(new LockListener() {}));
    }

    @Test
    @DisplayName("监听器抛异常应被隔离,不影响锁功能")
    void test11_listenerExceptionIsolated() throws InterruptedException {
        LockListener evil = new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                throw new RuntimeException("boom");
            }
            @Override public void onLockReleased(String k, long held) {
                throw new RuntimeException("boom-rel");
            }
            @Override public void onLockFailed(String k, long w, LockFailureReason reason) {
                throw new RuntimeException("boom-fail");
            }
        };
        lockManager.registerListener(evil);
        // 这些调用不应因监听器异常而失败
        assertDoesNotThrow(() -> assertTrue(lockManager.tryLock("k")));
        assertDoesNotThrow(() -> lockManager.unlock("k"));
        // 让另一线程占用 k,然后非阻塞 tryLock 应返回 false(而非抛异常)
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            assertTrue(lockManager.tryLock("k2"));
            held.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k2");
        });
        t.start();
        assertTrue(held.await(2, TimeUnit.SECONDS));
        assertDoesNotThrow(() -> assertFalse(lockManager.tryLock("k2", 0, TimeUnit.MILLISECONDS)));
        release.countDown();
        t.join(2000);
    }

    @Test
    @DisplayName("Clock 注入:清理逻辑使用配置的时钟(可推进)")
    void test12_clockInjectionUsedByCleanup() throws Exception {
        lockManager.shutdown();
        SettableClock clock = new SettableClock(0L);
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .clock(clock)
                .idleThreshold(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build());
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        assertTrue(lockManager.getActiveKeys().contains("k"));
        // 推进时间到 idleThreshold 之后
        clock.advance(Duration.ofMillis(500));
        // 等至少一个 cleanup tick
        Thread.sleep(150);
        assertFalse(lockManager.getActiveKeys().contains("k"),
                "时钟推进后清理线程应能正确清理空闲 entry");
    }

    @Test
    @DisplayName("AcquiredLock 在清理线程移除了 entry 后调 close 不应抛")
    void test13_acquiredLockCloseAfterCleanup() throws Exception {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(30))
                .build());
        java.util.Optional<AcquiredLock> opt = lockManager.tryAcquire("k");
        assertTrue(opt.isPresent());
        AcquiredLock al = opt.get();
        lockManager.unlock("k");
        // 等清理
        long deadline = System.currentTimeMillis() + 2000;
        while (lockManager.getActiveKeys().contains("k") && System.currentTimeMillis() < deadline) {
            Thread.sleep(30);
        }
        assertFalse(lockManager.getActiveKeys().contains("k"));
        // 此时 close 不应抛 LockNotFoundException
        assertDoesNotThrow(al::close);
    }

    @Test
    @DisplayName("shutdown 后再 shutdown 不影响已注册的 listener 行为?——单纯不应抛")
    void test14_shutdownThenUnlock() {
        lockManager.tryLock("k");
        lockManager.shutdown();
        // shutdown 已经清空 lockMap,unlock 走 silent no-op 路径
        assertDoesNotThrow(() -> lockManager.unlock("k"));
    }

    @Test
    @DisplayName("tryMultiLock 全部成功 vs 部分失败回滚")
    void test15_multiLockRollback() throws InterruptedException {
        // 由另一个线程占用 k2,k3,k4,主线程尝试 [k1,k2,k3,k4] 应整体失败并回滚 k1
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            assertTrue(lockManager.tryLock("k2"));
            assertTrue(lockManager.tryLock("k3"));
            assertTrue(lockManager.tryLock("k4"));
            held.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k2");
            lockManager.unlock("k3");
            lockManager.unlock("k4");
        });
        t.start();
        assertTrue(held.await(2, TimeUnit.SECONDS));

        boolean ok = lockManager.tryMultiLock(50, TimeUnit.MILLISECONDS,
                "k1", "k2", "k3", "k4");
        assertFalse(ok, "部分 key 被占用时整体应失败");
        assertFalse(lockManager.isLocked("k1"),
                "回滚后 k1 应被释放");
        assertTrue(lockManager.isLocked("k2"));
        assertTrue(lockManager.isLocked("k3"));
        assertTrue(lockManager.isLocked("k4"));

        release.countDown();
        t.join(2000);
    }

    @Test
    @DisplayName("tryMultiLock 重复 key 去重")
    void test16_multiLockDedupe() {
        boolean ok = lockManager.tryMultiLock("dup", "dup", "dup");
        assertTrue(ok);
        assertEquals(1, lockManager.getHoldCount("dup"));
        assertEquals(1, lockManager.getActiveKeys().size());
        lockManager.unlock("dup");
    }

    @Test
    @DisplayName("config.defaults() 字段值与文档一致")
    void test17_configDefaults() {
        CHMRLockConfig c = CHMRLockConfig.defaults();
        assertEquals(Duration.ofSeconds(3), c.defaultWaitTime());
        assertEquals(Duration.ZERO, c.defaultLeaseTime());
        assertEquals(Duration.ofMinutes(5), c.idleThreshold());
        assertEquals(Duration.ofSeconds(1), c.cleanupInterval());
        assertEquals(0L, c.maxKeys());
        assertTrue(c.daemonCleanupThread());
        assertTrue(c.forceUnlockOnLeaseExpiry());
        assertFalse(c.fairLock());
        assertFalse(c.enablePerKeyMetrics());
        assertFalse(c.forceUnlockEnabled());
    }

    @Test
    @DisplayName("config.builder 校验:负 maxKeys、零/负 cleanupInterval")
    void test18_configValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().maxKeys(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().cleanupInterval(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().cleanupInterval(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().defaultWaitTime(Duration.ofMillis(-1)).build());
        assertThrows(NullPointerException.class,
                () -> CHMRLockConfig.builder().clock(null).build());
    }

    /** 简单的可推进 Clock,用于测试。 */
    static final class SettableClock extends java.time.Clock {
        private volatile long now;

        SettableClock(long initialMillis) {
            this.now = initialMillis;
        }

        void advance(Duration d) {
            this.now += d.toMillis();
        }

        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
        @Override public java.time.Clock withZone(java.time.ZoneId z) { return this; }
        @Override public long millis() { return now; }
        @Override public java.time.Instant instant() { return java.time.Instant.ofEpochMilli(now); }
    }
}
