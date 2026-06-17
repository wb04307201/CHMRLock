package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P2 综合回归测试：验证 v2.0 引入的多个特性（per-key metrics、listener、
 * multi-lock、lease、async、forceUnlock、清理线程）端到端协作。
 *
 * <p>所有测试均使用真实锁、真实监听器与真实计时，不依赖 mock。
 * 单个测试方法应在 2 秒内完成，整类应在 10 秒内完成。</p>
 */
class CHMRLockP2RegressionTest {

    /**
     * 简单记录型 listener，将每个事件追加到一个并发队列，便于按时间顺序断言。
     */
    static class RecordingListener implements LockListener {
        final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
        final CopyOnWriteArrayList<String> snapshotBuffer = new CopyOnWriteArrayList<>();

        @Override public void onLockAcquired(String key, long waitNanos) { events.add("acquired:" + key); }
        @Override public void onLockReleased(String key, long heldMillis) { events.add("released:" + key); }
        @Override public void onLockFailed(String key, long waitNanos, String reason) { events.add("failed:" + key + ":" + reason); }
        @Override public void onLockExpired(String key) { events.add("expired:" + key); }
        @Override public void onLockContended(String key) { events.add("contended:" + key); }
    }

    private CHMRLockConfig metricsAndListenerConfig() {
        return CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .cleanupInterval(Duration.ofSeconds(1))
                .idleThreshold(Duration.ofSeconds(2))
                .build();
    }

    private CHMRLockConfig multiLockAndForceConfig() {
        return CHMRLockConfig.builder()
                .forceUnlockEnabled(true)
                .enablePerKeyMetrics(true)
                .cleanupInterval(Duration.ofSeconds(1))
                .idleThreshold(Duration.ofSeconds(2))
                .build();
    }

    // -----------------------------------------------------------------------
    // 1. Per-key metrics + listener 联合校验：tryLock/tryAcquire/unlock 后
    //    getStatistics(key) 与 listener 事件应保持一致。
    // -----------------------------------------------------------------------
    @Test
    void perKeyMetricsAndListenerConsistent() {
        try (CHMRLock lock = new CHMRLock(metricsAndListenerConfig())) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);

            // 第一次 tryLock: 成功
            assertTrue(lock.tryLock("k1"));
            // 第二次 tryAcquire (非可重入): 当前线程已持有, 应返回空 —— 此路径在
            // CHMRLock.tryAcquire 内部短路, 不进入 tryLock, 因此 listener 不会触发 acquired 事件
            // 也没有 attempt 计数 (这是设计行为, 反映了非可重入的早期检查)
            assertTrue(lock.tryAcquire("k1").isEmpty());
            // 主动释放
            lock.unlock("k1");
            // 第三次 tryLock + 立即释放
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");

            // 监听器: 2 个 acquired + 2 个 released
            long acquiredCount = rec.events.stream().filter(e -> e.equals("acquired:k1")).count();
            long releasedCount = rec.events.stream().filter(e -> e.equals("released:k1")).count();
            assertEquals(2, acquiredCount, "expected 2 acquired events, got: " + rec.events);
            assertEquals(2, releasedCount, "expected 2 released events, got: " + rec.events);

            // Per-key metrics: 2 次成功 (tryAcquire 短路不计入)
            KeyStatistics ks = lock.getStatistics("k1").orElseThrow();
            assertEquals(2L, ks.acquireCount(), "expected 2 acquire attempts (tryAcquire short-circuited)");
            assertEquals(2L, ks.successCount(), "expected 2 successes");
            assertEquals(0L, ks.failedCount(), "expected 0 failures");
        }
    }

    // -----------------------------------------------------------------------
    // 2. tryMultiLock + lease: 获取 2 个 key, 验证 isLocked 与 metrics
    // -----------------------------------------------------------------------
    @Test
    void multiLockWithLeaseAndMetrics() {
        try (CHMRLock lock = new CHMRLock(metricsAndListenerConfig())) {
            // tryMultiLock 内部把 leaseTime 传给 tryLock(..., leaseTime, MILLISECONDS),
            // 因此这里传毫秒值: waitTime=3000ms, leaseTime=1000ms
            boolean ok = lock.tryMultiLock(3000, 1000, TimeUnit.MILLISECONDS, "alpha", "beta");
            assertTrue(ok, "multiLock should succeed");

            // 两个 key 都被锁
            assertTrue(lock.isLocked("alpha"));
            assertTrue(lock.isLocked("beta"));

            // 指标：每个 key 至少 1 次 acquire + 1 次 success
            assertTrue(lock.getStatistics("alpha").isPresent());
            assertTrue(lock.getStatistics("beta").isPresent());
            assertEquals(1L, lock.getStatistics("alpha").orElseThrow().acquireCount());
            assertEquals(1L, lock.getStatistics("alpha").orElseThrow().successCount());
            assertEquals(1L, lock.getStatistics("beta").orElseThrow().acquireCount());
            assertEquals(1L, lock.getStatistics("beta").orElseThrow().successCount());

            // 全局 MonitorMetrics 同样反映 2 次 acquire
            MonitorMetrics global = lock.getStatistics();
            assertEquals(2L, global.getTotalLocks());
            assertEquals(2L, global.getSuccessLocks());

            // 释放 (顺序不影响)
            lock.unlock("alpha");
            lock.unlock("beta");
        }
    }

    // -----------------------------------------------------------------------
    // 3. 跨线程 forceUnlock: 主线程持有, 后台线程释放
    // -----------------------------------------------------------------------
    @Test
    void forceUnlockFromBackgroundThread() throws InterruptedException {
        try (CHMRLock lock = new CHMRLock(multiLockAndForceConfig())) {
            // 主线程持有
            assertTrue(lock.tryLock("k1"));
            assertTrue(lock.isLocked("k1"));
            assertTrue(lock.isHeldByCurrentThread("k1"));

            Thread bg = new Thread(() -> lock.forceUnlock("k1"));
            bg.start();
            bg.join(2000);
            assertFalse(bg.isAlive(), "background thread should complete");

            // 主线程视角: 锁已被释放
            assertFalse(lock.isLocked("k1"), "isLocked should be false after forceUnlock from bg thread");
            assertNull(lock.getOwnerThreadId("k1"));

            // 主线程可重新加锁
            assertTrue(lock.tryLock("k1"),
                    "main thread should re-acquire after forceUnlock cleared the sentinel");
            assertTrue(lock.isLocked("k1"));
            lock.unlock("k1");
        }
    }

    // -----------------------------------------------------------------------
    // 4. 监听器事件顺序: 单一线程下 acquired 必先于 released
    // -----------------------------------------------------------------------
    @Test
    void listenerEventsAreOrdered() {
        try (CHMRLock lock = new CHMRLock(metricsAndListenerConfig())) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);

            // 序列: acquire(k1) -> release(k1) -> acquire(k2) -> release(k2)
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            assertTrue(lock.tryLock("k2"));
            lock.unlock("k2");

            // 期望序列
            assertEquals(
                    java.util.Arrays.asList("acquired:k1", "released:k1", "acquired:k2", "released:k2"),
                    new java.util.ArrayList<>(rec.events),
                    "events should be recorded in order"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 5. tryAcquireAsync + per-key metrics: 异步线程加锁/释放
    // -----------------------------------------------------------------------
    @Test
    void asyncWithPerKeyMetrics() throws Exception {
        // forceUnlockEnabled 让我们可以从主线程跨线程释放异步锁
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .forceUnlockEnabled(true)
                .cleanupInterval(Duration.ofSeconds(1))
                .idleThreshold(Duration.ofSeconds(2))
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            // 异步获取 AcquiredLock 包装
            Optional<AcquiredLock> acquired =
                    lock.tryAcquireAsync("async-key").get(2, TimeUnit.SECONDS);
            assertTrue(acquired.isPresent(), "async tryAcquire should succeed");

            // 主线程视角: 锁已被某线程持有
            assertTrue(lock.isLocked("async-key"));

            // Per-key metrics: 1 次 acquire + 1 次 success (异步路径走 tryLock)
            KeyStatistics ks = lock.getStatistics("async-key").orElseThrow();
            assertEquals(1L, ks.acquireCount());
            assertEquals(1L, ks.successCount());
            assertEquals(0L, ks.failedCount());

            // 跨线程释放: 走 forceUnlock 路径 (主线程非 owner)
            lock.forceUnlock("async-key");
            assertFalse(lock.isLocked("async-key"));

            // Release 后 release counter 已记录
            KeyStatistics after = lock.getStatistics("async-key").orElseThrow();
            assertTrue(after.lastReleaseEpochMs() > 0,
                    "lastRelease should be set after release, was: " + after.lastReleaseEpochMs());
        }
    }

    // -----------------------------------------------------------------------
    // 6. tryMultiLock + 部分 forceUnlock: 仅释放一个, 其余不受影响
    // -----------------------------------------------------------------------
    @Test
    void forceUnlockDoesNotAffectOtherMultiLockKeys() {
        try (CHMRLock lock = new CHMRLock(multiLockAndForceConfig())) {
            assertTrue(lock.tryMultiLock("k1", "k2", "k3"));
            assertTrue(lock.isLocked("k1"));
            assertTrue(lock.isLocked("k2"));
            assertTrue(lock.isLocked("k3"));

            // 仅强制释放 k1
            lock.forceUnlock("k1");

            // k1 已释放, k2/k3 仍持有
            assertFalse(lock.isLocked("k1"), "k1 should be released");
            assertTrue(lock.isLocked("k2"), "k2 should still be locked");
            assertTrue(lock.isLocked("k3"), "k3 should still be locked");

            // 清理 k2/k3
            lock.unlock("k2");
            lock.unlock("k3");
        }
    }

    // -----------------------------------------------------------------------
    // 7. 清理线程不会回收当前持有中的 key
    // -----------------------------------------------------------------------
    @Test
    void cleanupThreadDoesNotRemoveActiveKeys() throws Exception {
        // 短清理间隔 (1s) + 短空闲阈值 (2s), 等待超过一个清理周期
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .cleanupInterval(Duration.ofMillis(500))
                .idleThreshold(Duration.ofSeconds(2))
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            assertTrue(lock.tryLock("active"));
            assertTrue(lock.isLocked("active"));

            // 睡眠 ~1.5 秒, 至少跨过一次清理扫描
            Thread.sleep(1500);

            // isLocked == true 时清理线程不会回收
            assertTrue(lock.isLocked("active"),
                    "active key should survive cleanup");
            assertTrue(lock.getActiveKeys().contains("active"),
                    "active key should still be present in map");

            lock.unlock("active");
        }
    }

    // -----------------------------------------------------------------------
    // 附加: 全链路 sanity check —— 验证一个完整的混合场景
    // -----------------------------------------------------------------------
    @Test
    void endToEndScenario() throws Exception {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .enablePerKeyMetrics(true)
                .forceUnlockEnabled(true)
                .cleanupInterval(Duration.ofSeconds(1))
                .idleThreshold(Duration.ofSeconds(2))
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);

            // 1) 普通加锁
            assertTrue(lock.tryLock("e2e-1"));
            assertTrue(lock.isLocked("e2e-1"));
            // 2) 异步加锁
            Optional<AcquiredLock> asyncResult =
                    lock.tryAcquireAsync("e2e-2").get(2, TimeUnit.SECONDS);
            assertTrue(asyncResult.isPresent(), "async tryAcquire should succeed");
            // 3) 多 key 加锁
            assertTrue(lock.tryMultiLock("e2e-3", "e2e-4"));

            // 全部 4 个 key 都应被锁
            assertTrue(lock.isLocked("e2e-1"));
            assertTrue(lock.isLocked("e2e-2"));
            assertTrue(lock.isLocked("e2e-3"));
            assertTrue(lock.isLocked("e2e-4"));

            // 全局 metrics: 4 次成功
            MonitorMetrics m = lock.getStatistics();
            assertEquals(4L, m.getTotalLocks());
            assertEquals(4L, m.getSuccessLocks());

            // Per-key metrics 全部存在
            Map<String, KeyStatistics> all = lock.getAllStatistics();
            assertEquals(4, all.size());
            assertTrue(all.containsKey("e2e-1"));
            assertTrue(all.containsKey("e2e-2"));
            assertTrue(all.containsKey("e2e-3"));
            assertTrue(all.containsKey("e2e-4"));

            // 4) forceUnlock 释放 e2e-1
            lock.forceUnlock("e2e-1");
            assertFalse(lock.isLocked("e2e-1"));
            assertTrue(lock.isLocked("e2e-2"));
            assertTrue(lock.isLocked("e2e-3"));
            assertTrue(lock.isLocked("e2e-4"));

            // 5) 释放其余 (e2e-2 由异步线程持有, 用 forceUnlock 跨线程释放)
            lock.forceUnlock("e2e-2");
            lock.unlock("e2e-3");
            lock.unlock("e2e-4");

            // 监听器应至少记录 4 个 acquired + 至少 1 个 forceUnlock 触发的 released
            long acquiredCount = rec.events.stream().filter(e -> e.startsWith("acquired:")).count();
            long releasedCount = rec.events.stream().filter(e -> e.startsWith("released:")).count();
            assertTrue(acquiredCount >= 4, "expected >=4 acquired events, got: " + rec.events);
            assertTrue(releasedCount >= 4, "expected >=4 released events, got: " + rec.events);
        }
    }
}
