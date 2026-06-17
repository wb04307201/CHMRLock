package cn.wubo.lock;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * 基于 {@link java.util.concurrent.ConcurrentHashMap} 和 {@link java.util.concurrent.locks.ReentrantLock} 的细粒度单机锁库。
 *
 * <p>每个 String key 维护一把独立的锁。线程安全、零三方依赖、支持 try-with-resources 和租约模式。</p>
 *
 * <p>典型用法：</p>
 * <pre>{@code
 * CHMRLock lock = new CHMRLock();
 * try {
 *     if (lock.tryLock("resource_id")) {
 *         try {
 *             doWork();
 *         } finally {
 *             lock.unlock("resource_id");
 *         }
 *     }
 * } finally {
 *     lock.shutdown();
 * }
 * }</pre>
 *
 * <h2>行为约定</h2>
 * <ul>
 *   <li>默认非公平锁（{@code ReentrantLock(false)}）</li>
 *   <li>清理线程为守护线程（命名 {@code chmrlock-cleanup-N}）</li>
 *   <li>{@link #shutdown()} 幂等，可多次调用</li>
 *   <li>{@link #unlock(String)} 跨线程调用会抛 {@link IllegalMonitorStateException}</li>
 * </ul>
 *
 * @see CHMRLockConfig
 * @see AcquiredLock
 */
public class CHMRLock implements AutoCloseable {
    // 配置
    private final CHMRLockConfig config;

    // 默认等待时间（毫秒）
    private long defaultWaitTime;

    // 存储各个key对应的锁
    private final Map<String, LockEntry> lockMap = new ConcurrentHashMap<>();

    // 统计信息
    private final AtomicLong totalLocks = new AtomicLong(0);
    private final AtomicLong successLocks = new AtomicLong(0);
    private final AtomicLong failedLocks = new AtomicLong(0);
    private final AtomicLong totalWaitTime = new AtomicLong(0);

    // 清理线程池：用于定期执行清理任务
    private final ScheduledExecutorService cleanupExecutor;

    private static final Logger log = Logger.getLogger(CHMRLock.class.getName());
    private final AtomicBoolean shutdownCalled = new AtomicBoolean(false);


    /** 使用默认配置创建实例。详见 {@link CHMRLockConfig#defaults()}。 */
    public CHMRLock() {
        this(CHMRLockConfig.defaults());
    }

    /**
     * 使用指定默认等待时间创建实例，其余配置使用默认值。
     * @param defaultWaitTime 默认等待时长
     * @param timeUnit 时间单位
     */
    public CHMRLock(long defaultWaitTime, TimeUnit timeUnit) {
        this(CHMRLockConfig.builder()
                .defaultWaitTime(Duration.ofMillis(timeUnit.toMillis(defaultWaitTime)))
                .build());
    }

    /**
     * 使用自定义配置创建实例。
     * @param config 锁配置（不可为 null）
     */
    public CHMRLock(CHMRLockConfig config) {
        this.config = config;
        this.defaultWaitTime = config.defaultWaitTime().toMillis();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(config));

        // 启动后台清理线程
        startCleanupThread();
    }

    private static ThreadFactory daemonThreadFactory(CHMRLockConfig config) {
        AtomicInteger seq = new AtomicInteger(1);
        String prefix = "chmrlock-cleanup";
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.getAndIncrement());
            t.setDaemon(config.daemonCleanupThread());
            return t;
        };
    }

    /**
     * 启动后台定时清理线程，定期执行清理任务
     */
    private void startCleanupThread() {
        long intervalMillis = config.cleanupInterval().toMillis();
        cleanupExecutor.scheduleAtFixedRate(this::cleanupTick,
                intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void cleanupTick() {
        long now = config.clock().millis();
        long threshold = config.idleThreshold().toMillis();
        lockMap.entrySet().removeIf(entry -> {
            LockEntry le = entry.getValue();
            if (le.lock.isLocked()) return false;
            if (le.getLeaseEndTime() != Long.MAX_VALUE && le.getLeaseEndTime() < now) {
                return false;  // 租约未到，保留（理论上不会出现这种情况）
            }
            return (now - le.getLastAcquireTime()) > threshold;
        });
    }

    /**
     * 统计当前持有中的 entry 数量（用于 maxKeys 限制）。
     * 注意：仅扫描持有中的锁，释放后的 entry 不占 slot，
     * 避免 unlock → tryLock 新 key 的常见模式被错误阻塞。
     */
    private long countHeldEntries() {
        long count = 0;
        for (LockEntry le : lockMap.values()) {
            if (le.lock.isLocked()) count++;
        }
        return count;
    }

    /**
     * 尝试获取指定 key 的锁，等待最多 {@code defaultWaitTime}（默认 3 秒）。
     * @param key 锁标识
     * @return 是否成功获取
     */
    public boolean tryLock(String key) {
        return tryLock(key, defaultWaitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试获取指定 key 的锁，等待最多 {@code waitTime}。
     * @param key 锁标识
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return 是否成功获取
     */
    public boolean tryLock(String key, long waitTime, TimeUnit timeUnit) {
        return tryLock(key, waitTime, 0, timeUnit);
    }

    /**
     * 获取锁，指定等待时间与租约时间。
     * 租约到期后会通过 {@link #isLocked(String)} 报告锁已释放，但底层 ReentrantLock
     * 仍由原持有线程持有（ReentrantLock.unlock 要求 owner 线程）。
     * 真正跨线程强制释放需调用 {@link #unlock(String)} 或后续版本的 forceUnlock。
     *
     * @param key 锁标识
     * @param waitTime 等待获取的最长时间
     * @param leaseTime 租约时间（毫秒），0 表示无租约
     * @param timeUnit 时间单位
     * @return 是否成功获取
     */
    public boolean tryLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        long startTime = config.clock().millis();
        long startNanos = System.nanoTime();
        totalLocks.incrementAndGet();
        boolean perKeyMetrics = config.enablePerKeyMetrics();

        // maxKeys 限制：仅对"新 key"（不在 map 中）生效，已存在的 key 可重入
        // 计数维度：仅统计当前"持有中"的 entry（isLocked == true），
        // 释放后的 entry 不占 slot，否则 unlock → tryLock 新 key 的常见模式会被阻塞
        if (config.maxKeys() > 0 && !lockMap.containsKey(key)) {
            if (countHeldEntries() >= config.maxKeys()) {
                failedLocks.incrementAndGet();
                long elapsedMillis = config.clock().millis() - startTime;
                totalWaitTime.addAndGet(elapsedMillis);
                if (perKeyMetrics) {
                    LockEntry existing = lockMap.get(key);
                    if (existing != null) {
                        existing.recordAcquireAttempt();
                        existing.recordAcquireFailure(System.nanoTime() - startNanos);
                    }
                }
                return false;
            }
        }

        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry(config.fairLock()));

        if (perKeyMetrics) {
            lockEntry.recordAcquireAttempt();
        }

        try {
            boolean acquired = lockEntry.lock.tryLock(waitTime, timeUnit);
            if (acquired) {
                successLocks.incrementAndGet();
                lockEntry.touchLastAcquireTime();
                lockEntry.setOwnerThreadId(Thread.currentThread().getId());
                if (perKeyMetrics) {
                    lockEntry.recordAcquireSuccess(System.nanoTime() - startNanos);
                }
                if (leaseTime > 0) {
                    long leaseEnd = config.clock().millis() + timeUnit.toMillis(leaseTime);
                    lockEntry.setLeaseEndTime(leaseEnd);
                    scheduleLeaseExpiry(key, leaseEnd);
                }
                return true;
            } else {
                failedLocks.incrementAndGet();
                if (perKeyMetrics) {
                    lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
                }
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedLocks.incrementAndGet();
            if (perKeyMetrics) {
                lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
            }
            return false;
        } finally {
            totalWaitTime.addAndGet(config.clock().millis() - startTime);
        }
    }

    private void scheduleLeaseExpiry(String key, long leaseEndMillis) {
        long delayMillis = Math.max(0, leaseEndMillis - config.clock().millis());
        cleanupExecutor.schedule(() -> {
            LockEntry entry = lockMap.get(key);
            if (entry != null && entry.getLeaseEndTime() == leaseEndMillis) {
                // 仍处于该租约窗口
                if (!entry.lock.isLocked()) {
                    return;  // 已被主动释放
                }
                // 强制释放
                try {
                    entry.lock.unlock();
                    entry.clearOwner();
                    entry.clearLease();
                } catch (IllegalMonitorStateException ignored) {
                    // 已被释放
                }
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试获取指定 key 的锁，等待最多 {@code waitTime} 毫秒。
     * @param key 锁标识
     * @param waitTime 等待获取的最长时间（毫秒）
     * @return 是否成功获取
     */
    public boolean tryLock(String key, long waitTime){
        return tryLock(key, waitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 非可重入版本的 tryLock，返回 AcquiredLock 以支持 try-with-resources。
     * 与 tryLock 不同：若当前线程已持有 key，返回 Optional.empty()。
     * @param key 锁标识
     * @return 成功时返回包含 AcquiredLock 的 Optional，失败返回空
     */
    public Optional<AcquiredLock> tryAcquire(String key) {
        return tryAcquire(key, defaultWaitTime, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取锁并返回 AcquiredLock 包装，支持 try-with-resources。
     * 与 tryLock 不同，tryAcquire 是非可重入的：如果当前线程已经持有 key，
     * 返回 Optional.empty()。这保证了 AcquiredLock 包装与底层锁获取是 1:1 对应。
     *
     * @param key 锁标识
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return 成功则返回包含 AcquiredLock 的 Optional，否则空
     */
    public Optional<AcquiredLock> tryAcquire(String key, long waitTime, TimeUnit timeUnit) {
        return tryAcquire(key, waitTime, 0, timeUnit);
    }

    /**
     * 获取锁并返回 AcquiredLock 包装，支持 try-with-resources，可指定租约。
     * 与 tryLock 不同，tryAcquire 是非可重入的：如果当前线程已经持有 key，返回 Optional.empty()。
     */
    public Optional<AcquiredLock> tryAcquire(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        if (isHeldByCurrentThread(key)) {
            return Optional.empty();
        }
        if (tryLock(key, waitTime, leaseTime, timeUnit)) {
            return Optional.of(new AcquiredLock(this, key));
        }
        return Optional.empty();
    }

    /**
     * 释放锁。key 必须已经通过 tryLock 成功获取。
     * @throws LockNotFoundException key 从未加锁
     * @throws IllegalMonitorStateException 锁未被当前线程持有（跨线程 unlock）
     */
    public void unlock(String key) {
        LockEntry lockEntry = lockMap.get(key);
        if (lockEntry == null) {
            throw new LockNotFoundException(key);
        }
        lockEntry.lock.unlock();
        lockEntry.clearOwner();
        lockEntry.clearLease();
        if (config.enablePerKeyMetrics()) {
            lockEntry.recordRelease(config.clock().millis());
        }
    }

    /**
     * 判断 key 当前是否被锁。注意：若启用了租约，租约到期后此方法会返回 false，
     * 但底层 ReentrantLock 仍由原持有线程持有，直到调用 unlock。
     */
    public boolean isLocked(String key) {
        LockEntry e = lockMap.get(key);
        if (e == null) return false;
        if (!e.lock.isLocked()) return false;
        // 租约到期视为已释放（ReentrantLock 不支持跨线程 force-unlock）
        if (config.clock().millis() > e.getLeaseEndTime()) {
            return false;
        }
        return true;
    }

    /**
     * @param key 锁标识
     * @return 当前线程是否持有该 key；key 不存在返回 false
     */
    public boolean isHeldByCurrentThread(String key) {
        LockEntry e = lockMap.get(key);
        return e != null && e.lock.isHeldByCurrentThread();
    }

    /**
     * @param key 锁标识
     * @return 当前线程对该 key 的重入深度；key 不存在返回 0
     */
    public int getHoldCount(String key) {
        LockEntry e = lockMap.get(key);
        return e == null ? 0 : e.lock.getHoldCount();
    }

    /**
     * @return 当前所有已注册 key 的不可变视图。对返回集合的修改会抛
     *         {@link UnsupportedOperationException}，但底层 map 的后续变更会反映在
     *         下次调用此方法的结果中。
     */
    public Set<String> getActiveKeys() {
        return Collections.unmodifiableSet(lockMap.keySet());
    }

    /** 关闭锁管理器，停止清理线程并清空所有锁。幂等，可多次调用。 */
    public void shutdown() {
        if (shutdownCalled.compareAndSet(false, true)) {
            lockMap.clear();
            if (cleanupExecutor != null) {
                cleanupExecutor.shutdown();
            }
        }
    }

    /** @return 是否已调用过 {@link #shutdown()} */
    public boolean isShutdown() {
        return shutdownCalled.get();
    }

    /** {@link AutoCloseable#close()} 实现，等价于 {@link #shutdown()}。支持 try-with-resources。 */
    @Override
    public void close() {
        shutdown();
    }

    /** @return 全局统计指标快照 */
    public MonitorMetrics getStatistics() {
        return new MonitorMetrics(
                totalLocks.get(),
                successLocks.get(),
                failedLocks.get(),
                totalWaitTime.get()
        );
    }

    /**
     * 返回指定 key 的统计指标快照。需 {@link CHMRLockConfig#enablePerKeyMetrics} 为 true。
     *
     * @param key 锁标识
     * @return 指标快照；若 metrics 未启用或 key 从未加锁，返回 {@link Optional#empty()}
     */
    public Optional<KeyStatistics> getStatistics(String key) {
        if (!config.enablePerKeyMetrics()) return Optional.empty();
        LockEntry e = lockMap.get(key);
        if (e == null) return Optional.empty();
        return Optional.of(snapshotFor(key, e));
    }

    /**
     * 返回所有曾加锁 key 的统计指标快照。需 {@link CHMRLockConfig#enablePerKeyMetrics} 为 true。
     *
     * @return 不可变 Map；key -> 指标快照。未启用 metrics 时返回空 Map。
     */
    public Map<String, KeyStatistics> getAllStatistics() {
        if (!config.enablePerKeyMetrics()) return Collections.emptyMap();
        Map<String, KeyStatistics> snapshot = new HashMap<>(lockMap.size());
        for (Map.Entry<String, LockEntry> entry : lockMap.entrySet()) {
            String k = entry.getKey();
            LockEntry e = entry.getValue();
            snapshot.put(k, snapshotFor(k, e));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    private static KeyStatistics snapshotFor(String key, LockEntry e) {
        Long holderId = e.getOwnerThreadId();
        int holdCount = e.lock.getHoldCount();
        return new KeyStatistics(
                key,
                e.getAcquireCount(),
                e.getSuccessCount(),
                e.getFailedCount(),
                e.getTotalWaitNanos(),
                e.getLastAcquireTime(),
                e.getLastReleaseTime(),
                holdCount,
                holderId == null ? -1L : holderId
        );
    }
}
