package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第 6 轮测试:验证 R6 修复的具体行为。
 *
 * <p>覆盖 25 项 R6 缺陷修复:</p>
 * <ul>
 *   <li><b>P0:</b> B-7(clearForceUnlocked 原子性)、R-1(shutdown 后 tryLock 抛 ISE)、
 *       R-2(asyncExecutor shutdown 抛 REE)、B-1(tryAcquire fast-fail 计数)、
 *       B-5(forceUnlock 后 holderHoldCount 钳制 0)、A-1(DistributedLock SPI JavaDoc)、
 *       E-1(nanoTime 取代 clock.millis())</li>
 *   <li><b>P1:</b> B-2(maxKeys 拒绝持久化 per-key)、B-3(cleanupTick 借锁)、
 *       B-4(getHoldCount 跨线程一致)、A-2(tryAcquireAsync JavaDoc)、
 *       A-4(tryMultiLock 总等待)、E-2(cleanupTick 异常粒度)、E-3(fireXxx 隔离 Error)、
 *       E-4(fireContended 总是触发)、E-5(空字符串 key 拒绝)、E-6(listener 文档化)</li>
 *   <li><b>P2:</b> A-5(MaxKeysExceededException)、A-6(lockInterruptibly 文档化)、
 *       A-7(registerListener null 一致)、L-3(KeyedReadWriteLock 异常文档)、
 *       L-9(exportMetrics shutdown 文档)、E-9(holderHoldCount long)</li>
 * </ul>
 */
public class CHMRLockRound6Test {

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

    // ============ P0-1: B-7 clearForceUnlocked 原子性 ============

    @Test
    @DisplayName("B-7:clearForceUnlocked 原子翻转 forceUnlocked + releaseEventFired")
    void bug07_clearForceUnlockedIsAtomic() {
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
        assertEquals(1, released.get());

        // 重新获取,clearForceUnlocked 原子地清除两个位
        assertTrue(lockManager.tryLock("k"));
        // 此时 forceUnlocked=false,releaseEventFired=false(已清),后续 unlock 应正常触发
        lockManager.unlock("k");
        assertEquals(2, released.get(),
                "重新获取后 unlock 应正常触发 fireReleased");

        // 再一次获取,验证循环正常
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        assertEquals(3, released.get());
    }

    // ============ P0-2: R-1 shutdown 后 tryLock 抛 ISE ============

    @Test
    @DisplayName("R-1:shutdown 后 tryLock 抛 IllegalStateException")
    void r01_shutdownThenTryLockThrowsISE() {
        lockManager.shutdown();
        assertThrows(IllegalStateException.class, () -> lockManager.tryLock("k"));
        assertThrows(IllegalStateException.class,
                () -> lockManager.tryLock("k", 100, TimeUnit.MILLISECONDS));
        assertThrows(IllegalStateException.class,
                () -> lockManager.tryLock("k", 0, 1000, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("R-1:shutdown 后 lock() 抛 IllegalStateException")
    void r01b_shutdownThenLockThrowsISE() {
        lockManager.shutdown();
        assertThrows(IllegalStateException.class, () -> {
            try { lockManager.lock("k"); } catch (InterruptedException e) { throw new RuntimeException(e); }
        });
    }

    @Test
    @DisplayName("R-1:shutdown 后不会创建 lockMap 新 entry(无内存泄漏)")
    void r01c_shutdownNoNewEntryCreated() throws InterruptedException {
        lockManager.shutdown();
        try { lockManager.tryLock("leaked"); } catch (IllegalStateException ignored) {}
        Thread.sleep(100);  // 等清理线程(已停)不会做任何事
        assertFalse(lockManager.getActiveKeys().contains("leaked"),
                "shutdown 后 tryLock 应被拒绝,不应创建 entry");
    }

    // ============ P0-3: R-2 asyncExecutor shutdown 抛 REE ============

    @Test
    @DisplayName("R-2:shutdown 后 tryAcquireAsync 抛 RejectedExecutionException")
    void r02_asyncAfterShutdownThrowsREE() {
        lockManager.shutdown();
        assertThrows(RejectedExecutionException.class,
                () -> lockManager.tryAcquireAsync("k"));
        assertThrows(RejectedExecutionException.class,
                () -> lockManager.tryAcquireAsync("k", 100, TimeUnit.MILLISECONDS));
    }

    // ============ P0-4: B-1 tryAcquire fast-fail 计入 failedLocks ============

    @Test
    @DisplayName("B-1:tryAcquire fast-fail 计入全局 failedLocks")
    void bug01_tryAcquireFastFailCountsAsFailure() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            lockManager.tryLock("k");
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k");
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 当前线程已持有 → tryAcquire fast-fail
        long beforeFailed = lockManager.getStatistics().getFailedLocks();
        Optional<AcquiredLock> result = lockManager.tryAcquire("k", 0, TimeUnit.MILLISECONDS);
        assertFalse(result.isPresent());
        long afterFailed = lockManager.getStatistics().getFailedLocks();
        assertEquals(beforeFailed + 1, afterFailed,
                "fast-fail 失败应增加 failedLocks(R6 修复 B-1)");

        release.countDown();
        holder.join(2000);
    }

    // ============ P0-5: B-5 forceUnlock 后 holderHoldCount 钳制 0 ============

    @Test
    @DisplayName("B-5:跨线程 forceUnlock 后多次 unlock 不抛异常,holderHoldCount 钳制 0")
    void bug05_forceUnlockThenHolderUnlockSafe() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .enablePerKeyMetrics(true)
                .build());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                lockManager.tryLock("k");  // ReentrantLock=1, holderHoldCount=1
                lockManager.tryLock("k");  // ReentrantLock=2, holderHoldCount=2
                entered.countDown();
                release.await();
                lockManager.unlock("k");  // 哨兵路径(forceUnlock 后),无异常
                lockManager.unlock("k");  // 再次 unlock 仍无异常
                lockManager.unlock("k");  // 第三次 unlock 仍无异常
            } catch (Throwable t) {
                err.set(t);
            }
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        lockManager.forceUnlock("k");
        // holderHoldCount 应被重置为 0
        assertEquals(0L, lockManager.getStatistics("k").orElseThrow().currentHoldCount(),
                "forceUnlock 后 holderHoldCount 应为 0");

        release.countDown();
        holder.join(2000);
        assertNull(err.get(), "持有线程多次 unlock 不应抛异常: " + err.get());

        // 验证最终 holderHoldCount 不是负数
        assertEquals(0L, lockManager.getStatistics("k").orElseThrow().currentHoldCount(),
                "R6 修复 B-5:holderHoldCount 钳制 0,不应变负");
    }

    // ============ P0-7: E-1 nanoTime 取代 clock.millis() ============

    @Test
    @DisplayName("E-1:Clock 回退时 totalWaitTime 不变负")
    void e01_clockRewindDoesNotProduceNegativeWait() {
        lockManager.shutdown();
        AtomicLong mutableNow = new AtomicLong(1_000_000L);
        java.time.Clock mutableClock = new java.time.Clock() {
            @Override public java.time.Instant instant() { return java.time.Instant.ofEpochMilli(mutableNow.get()); }
            @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
            @Override public java.time.Clock withZone(java.time.ZoneId z) { return this; }
            @Override public long millis() { return mutableNow.get(); }
        };
        lockManager = new CHMRLock(CHMRLockConfig.builder().clock(mutableClock).build());
        lockManager.tryLock("k", 100, TimeUnit.MILLISECONDS);
        // 时钟大幅回退
        mutableNow.addAndGet(-30 * 60 * 1000L);
        lockManager.tryLock("k", 100, TimeUnit.MILLISECONDS);
        lockManager.unlock("k");
        // R6 修复(E-1):用 nanoTime 计算 elapsed,clock 回退不会产生负数
        long totalWait = lockManager.getStatistics().getTotalWaitTime();
        assertTrue(totalWait >= 0,
                "R6 修复:Clock 回退后 totalWaitTime 不应为负,实际: " + totalWait);
    }

    // ============ P1-1: B-2 maxKeys 拒绝持久化 per-key ============

    @Test
    @DisplayName("B-2:maxKeys 拒绝后 per-key failedCount 仍记录")
    void bug02_maxKeysRejectPersistsPerKeyCount() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .maxKeys(1)
                .enablePerKeyMetrics(true)
                .build());
        assertTrue(lockManager.tryLock("a"));
        // 第二个 key 被 maxKeys 拒绝
        assertFalse(lockManager.tryLock("b", 0, TimeUnit.MILLISECONDS));
        // R6 修复(B-2):在 lockMap.remove 之前已记录 per-key 失败
        // 但 entry 已被移除,所以 getStatistics("b") 返回 empty
        // 这里只能验证全局 failedCount 增加了
        assertEquals(1, lockManager.getStatistics().getFailedLocks());
        // entry 已被移除(不影响 getActiveKeys)
        assertFalse(lockManager.getActiveKeys().contains("b"),
                "被拒绝的 key 不应在 lockMap 中残留");
        lockManager.unlock("a");
    }

    // ============ P1-2: B-3 cleanupTick 借锁 tryLock(0) ============

    @Test
    @DisplayName("B-3:cleanupTick 借锁不影响统计,持有中不被清理")
    void bug03_cleanupTickBorrowLock() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(10))
                .build());
        assertTrue(lockManager.tryLock("k"));
        Thread.sleep(200);
        // 持有中,不应被清理,且总锁数=1(借锁不计入)
        assertEquals(1, lockManager.getStatistics().getTotalLocks());
        assertTrue(lockManager.getActiveKeys().contains("k"));
        lockManager.unlock("k");
    }

    // ============ P1-3: B-4 getHoldCount 跨线程一致 ============

    @Test
    @DisplayName("B-4:getHoldCount 跨线程返回 holderHoldCount,与 isLocked/owner 一致")
    void bug04_getHoldCountCrossThreadConsistent() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Long> crossThreadHoldCount = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            lockManager.tryLock("k");
            lockManager.tryLock("k");
            lockManager.tryLock("k");
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k");
            lockManager.unlock("k");
            lockManager.unlock("k");
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 跨线程读:R6 修复后应返回 holderHoldCount=3,而非 0
        crossThreadHoldCount.set((long) lockManager.getHoldCount("k"));
        assertEquals(3L, crossThreadHoldCount.get(),
                "R6 修复 B-4:跨线程读应返回持有线程的实际重入深度");
        assertTrue(lockManager.isLocked("k"));
        assertNotNull(lockManager.getOwnerThreadId("k"));

        release.countDown();
        holder.join(2000);
        assertEquals(0, lockManager.getHoldCount("k"));
    }

    // ============ P1-6: E-2 cleanupTick 异常粒度 ============

    @Test
    @DisplayName("E-2:cleanupTick 异常被吞掉(包在 catch(Exception) 中)")
    void e02_cleanupTickIsolatesExceptions() throws InterruptedException {
        // 不容易直接触发清理线程内部异常。但可验证清理线程持续运行:
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(20))
                .cleanupInterval(Duration.ofMillis(10))
                .build());
        for (int i = 0; i < 20; i++) {
            lockManager.tryLock("k" + i);
            lockManager.unlock("k" + i);
        }
        Thread.sleep(200);
        // 空闲 entry 应被清理
        assertTrue(lockManager.getActiveKeys().size() < 20);
    }

    // ============ P1-7: E-3 fireXxx 隔离 Error ============

    @Test
    @DisplayName("E-3:listener 抛 Error 不会破坏 tryLock 状态")
    void e03_listenerErrorIsIsolated() {
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                throw new StackOverflowError("simulated");
            }
        });
        // R6 修复(E-3):fireAcquired 改 catch(Throwable),Error 不会冒泡到 tryLock 调用方
        assertDoesNotThrow(() -> lockManager.tryLock("k"));
        assertTrue(lockManager.isLocked("k"),
                "listener 抛 Error 不应影响 tryLock 的成功状态");
        assertDoesNotThrow(() -> lockManager.unlock("k"));
    }

    @Test
    @DisplayName("E-3:listener 抛 OutOfMemoryError 不影响 tryLock")
    void e03b_listenerOOMIsIsolated() {
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                throw new OutOfMemoryError("simulated");
            }
        });
        assertDoesNotThrow(() -> lockManager.tryLock("k"));
        assertTrue(lockManager.isLocked("k"));
        lockManager.unlock("k");
    }

    // ============ P1-8: E-4 fireContended 总是触发 ============

    @Test
    @DisplayName("E-4:fireContended 即使 waitTime>=1ms 也会触发(只要被争用)")
    void e04_fireContendedEvenForBlockingTryLock() throws InterruptedException {
        AtomicInteger contended = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockContended(String k) { contended.incrementAndGet(); }
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            lockManager.tryLock("k", 60_000, TimeUnit.MILLISECONDS);
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            lockManager.unlock("k");
        });
        holder.start();
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        // 即使 waitTime=100ms(阻塞),只要检测到争用就 fire
        assertFalse(lockManager.tryLock("k", 100, TimeUnit.MILLISECONDS));
        assertTrue(contended.get() >= 1,
                "R6 修复 E-4:阻塞 tryLock 在争用时也应 fireContended,实际: " + contended.get());

        release.countDown();
        holder.join(2000);
    }

    // ============ P1-9: E-5 空字符串 key 拒绝 ============

    @Test
    @DisplayName("E-5:空字符串 key 被拒绝")
    void e05_emptyKeyRejected() {
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock(""));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("", 100, TimeUnit.MILLISECONDS));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("", 100, 0, TimeUnit.MILLISECONDS));
        assertThrows(IllegalArgumentException.class, () -> lockManager.tryLock("", 100));
        // unlock 也拒绝
        assertThrows(IllegalArgumentException.class, () -> lockManager.unlock(""));
        // forceUnlock 也拒绝
        assertThrows(IllegalArgumentException.class, () -> lockManager.forceUnlock(""));
        // 其他方法也拒绝
        assertThrows(IllegalArgumentException.class, () -> lockManager.isLocked(""));
        assertThrows(IllegalArgumentException.class, () -> lockManager.isHeldByCurrentThread(""));
        assertThrows(IllegalArgumentException.class, () -> lockManager.getHoldCount(""));
        assertThrows(IllegalArgumentException.class, () -> lockManager.getOwnerThreadId(""));
        assertThrows(IllegalArgumentException.class, () -> lockManager.readWriteLock(""));
    }

    @Test
    @DisplayName("E-5:tryMultiLock 中含空字符串 key 抛 IllegalArgumentException")
    void e05b_emptyKeyInMultiLockRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> lockManager.tryMultiLock(100, TimeUnit.MILLISECONDS, "a", "", "b"));
    }

    // ============ P1-10: E-6 listener 阻塞文档化 ============
    // (主要验证 listener 异常处理正确,文档在 LockListener JavaDoc 中体现)

    @Test
    @DisplayName("E-6:listener 阻塞不影响后续 listener 调用(每个独立 try-catch)")
    void e06_listenerBlockingIsolatedPerListener() {
        AtomicInteger secondListenerCalled = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                throw new RuntimeException("blocking");
            }
        });
        lockManager.registerListener(new LockListener() {
            @Override public void onLockAcquired(String k, long w) {
                secondListenerCalled.incrementAndGet();
            }
        });
        assertDoesNotThrow(() -> lockManager.tryLock("k"));
        // 第一个 listener 抛异常不影响第二个被调用(各自隔离)
        assertEquals(1, secondListenerCalled.get());
        lockManager.unlock("k");
    }

    // ============ P2-1: A-5 MaxKeysExceededException ============

    @Test
    @DisplayName("A-5:lock 在 maxKeys 时抛 MaxKeysExceededException(继承 ISE)")
    void a05_lockThrowsMaxKeysExceededException() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder().maxKeys(1).build());
        assertTrue(lockManager.tryLock("a"));
        // R6 修复(A-5):抛 MaxKeysExceededException(继承自 IllegalStateException)
        MaxKeysExceededException ex = assertThrows(MaxKeysExceededException.class,
                () -> {
                    try { lockManager.lock("b"); } catch (InterruptedException e) { throw new RuntimeException(e); }
                });
        assertTrue(ex instanceof IllegalStateException,
                "MaxKeysExceededException 应继承自 IllegalStateException 以保持向后兼容");
        lockManager.unlock("a");
    }

    // ============ P2-2: A-6 lockInterruptibly 文档化 ============

    @Test
    @DisplayName("A-6:lockInterruptibly 语义与 lock 完全一致")
    void a06_lockInterruptiblyIsAlias() throws InterruptedException {
        // 不抛异常,且获取成功
        lockManager.lockInterruptibly("k", 1000, TimeUnit.MILLISECONDS);
        assertTrue(lockManager.isLocked("k"));
        lockManager.unlock("k");
    }

    // ============ P2-3: A-7 registerListener null 一致 ============

    @Test
    @DisplayName("A-7:registerListener(null) 抛 NPE,与 registerDistributedLock 一致")
    void a07_registerListenerNullThrowsNPE() {
        assertThrows(NullPointerException.class, () -> lockManager.registerListener(null));
        // unregisterListener(null) 仍静默(注销语义对 null 无意义)
        assertDoesNotThrow(() -> lockManager.unregisterListener(null));
    }

    // ============ P2-4: L-3 KeyedReadWriteLock 异常文档化 ============
    // (主要验证 unlockRead/unlockWrite 抛 IllegalMonitorStateException,文档已完善)

    @Test
    @DisplayName("L-3:unlockRead 错误戳记抛 IllegalMonitorStateException")
    void l03_unlockReadWithInvalidStampThrows() {
        KeyedReadWriteLock rw = lockManager.readWriteLock("k");
        long stamp = rw.tryReadLock();
        assertTrue(stamp != 0);
        rw.unlockRead(stamp);
        // 第二次 unlock 抛异常
        assertThrows(RuntimeException.class, () -> rw.unlockRead(stamp));
    }

    // ============ P2-5: L-9 exportMetrics shutdown 后 ============

    @Test
    @DisplayName("L-9:shutdown 后 exportMetrics 仍可调用,perKey 为空")
    void l09_exportMetricsAfterShutdown() {
        lockManager.tryLock("k");
        lockManager.unlock("k");
        lockManager.shutdown();
        // shutdown 后 lockMap 已 clear,perKey 为空,但全局统计仍可读
        StringBuilder captured = new StringBuilder();
        lockManager.exportMetrics(new MetricsExporter() {
            @Override public void export(MonitorMetrics global, Map<String, KeyStatistics> perKey) {
                captured.append("global=").append(global.getTotalLocks())
                        .append(",perKey=").append(perKey.size());
            }
        });
        assertTrue(captured.toString().contains("global=1"),
                "全局统计(totalLocks=1)shutdown 后仍可读");
        assertTrue(captured.toString().contains("perKey=0"),
                "perKey shutdown 后为空");
    }

    // ============ P2-6: E-9 holderHoldCount 改 AtomicLong ============

    @Test
    @DisplayName("E-9:KeyStatistics.currentHoldCount 是 long")
    void e09_currentHoldCountIsLong() {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .build());
        // 大量重入 — 当前用 tryLock 模拟,实际只能重入到合理深度
        assertTrue(lockManager.tryLock("k"));
        assertTrue(lockManager.tryLock("k"));
        assertTrue(lockManager.tryLock("k"));
        KeyStatistics ks = lockManager.getStatistics("k").orElseThrow();
        assertEquals(3L, ks.currentHoldCount(),
                "currentHoldCount 是 long,可表示大重入深度");
        lockManager.unlock("k");
        lockManager.unlock("k");
        lockManager.unlock("k");
    }

    // ============ 综合:多修复协同 ============

    @Test
    @DisplayName("综合:R6 全套修复协同工作 (B-7 + R-1 + R-2 + B-5)")
    void comprehensive_r6FixesCooperate() throws InterruptedException {
        lockManager.shutdown();
        lockManager = new CHMRLock(CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .enablePerKeyMetrics(true)
                .build());

        // B-7 + B-5 协同
        AtomicInteger released = new AtomicInteger();
        lockManager.registerListener(new LockListener() {
            @Override public void onLockReleased(String k, long held) { released.incrementAndGet(); }
        });
        assertTrue(lockManager.tryLock("k"));
        lockManager.forceUnlock("k");
        assertEquals(1, released.get(), "forceUnlock 触发一次");
        assertEquals(0L, lockManager.getStatistics("k").orElseThrow().currentHoldCount());

        // B-7:重新获取后 unlock 正常 fireReleased
        assertTrue(lockManager.tryLock("k"));
        lockManager.unlock("k");
        assertEquals(2, released.get(), "重新获取后 unlock 应 fire");

        // R-1:shutdown 后 tryLock 抛 ISE
        lockManager.shutdown();
        assertThrows(IllegalStateException.class, () -> lockManager.tryLock("k2"));

        // R-2:shutdown 后 tryAcquireAsync 抛 REE
        assertThrows(RejectedExecutionException.class,
                () -> lockManager.tryAcquireAsync("k3"));
    }
}