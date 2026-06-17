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

    // Per-key StampedLock cache for read-write scenarios
    private final ConcurrentHashMap<String, StampedKeyedReadWriteLock> rwLockMap = new ConcurrentHashMap<>();

    // 统计信息
    private final AtomicLong totalLocks = new AtomicLong(0);
    private final AtomicLong successLocks = new AtomicLong(0);
    private final AtomicLong failedLocks = new AtomicLong(0);
    private final AtomicLong totalWaitTime = new AtomicLong(0);

    // 清理线程池：用于定期执行清理任务
    private final ScheduledExecutorService cleanupExecutor;

    // 锁生命周期监听器列表
    private final List<LockListener> listeners = new CopyOnWriteArrayList<>();

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
                fireFailed(key, System.nanoTime() - startNanos, "maxKeys");
                return false;
            }
        }

        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry(config.fairLock()));

        if (perKeyMetrics) {
            lockEntry.recordAcquireAttempt();
        }

        // 非阻塞 tryLock(waitTime <= 0) 时，若 key 被另一个线程持有（即可重入以外的情况），
        // 则触发 onLockContended 事件。同线程重入不会触发（不会阻塞）。
        if (waitTime <= 0 && lockEntry.lock.isLocked() && !lockEntry.lock.isHeldByCurrentThread()) {
            fireContended(key);
        }

        try {
            boolean acquired = lockEntry.lock.tryLock(waitTime, timeUnit);
            if (acquired) {
                successLocks.incrementAndGet();
                lockEntry.touchLastAcquireTime();
                lockEntry.setOwnerThreadId(Thread.currentThread().getId());
                // 重新获取时复位 forceUnlock 哨兵，使后续 isLocked 反映真实状态
                lockEntry.clearForceUnlocked();
                if (perKeyMetrics) {
                    lockEntry.recordAcquireSuccess(System.nanoTime() - startNanos);
                }
                if (leaseTime > 0) {
                    long leaseEnd = config.clock().millis() + timeUnit.toMillis(leaseTime);
                    lockEntry.setLeaseEndTime(leaseEnd);
                    scheduleLeaseExpiry(key, leaseEnd);
                }
                fireAcquired(key, System.nanoTime() - startNanos);
                return true;
            } else {
                failedLocks.incrementAndGet();
                if (perKeyMetrics) {
                    lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
                }
                fireFailed(key, System.nanoTime() - startNanos, "timeout");
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedLocks.incrementAndGet();
            if (perKeyMetrics) {
                lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
            }
            fireFailed(key, System.nanoTime() - startNanos, "interrupted");
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
                // 租约到期。ReentrantLock 不支持跨线程 unlock（会抛 IllegalMonitorStateException），
                // 因此这里仅清理租约状态和 owner，isLocked 即正确报告锁已过期。
                try {
                    entry.lock.unlock();
                } catch (IllegalMonitorStateException ignored) {
                    // 跨线程 unlock 失败，仅清理租约状态
                }
                entry.clearOwner();
                entry.clearLease();
                // 租约已到期，通知监听器（无论底层 ReentrantLock 是否真正释放）
                fireExpired(key);
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
     * 阻塞获取锁,无限等待,无租约。线程被中断时抛出 {@link InterruptedException}。
     * 实现基于 {@link java.util.concurrent.locks.ReentrantLock#lockInterruptibly()},
     * 因此等待期间可响应中断。
     * @param key 锁标识
     * @throws InterruptedException 等待获取过程中线程被中断
     */
    public void lock(String key) throws InterruptedException {
        lock(key, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 阻塞获取锁,无限等待,可指定租约时长。租约到期后 {@link #isLocked(String)}
     * 会报告锁已释放,但底层 {@link java.util.concurrent.locks.ReentrantLock}
     * 仍由原持有线程持有,需配合 {@link #unlock(String)} 释放。
     *
     * @param key 锁标识
     * @param leaseTime 租约时长(>0);0 表示无租约
     * @param timeUnit 时间单位
     * @throws InterruptedException 等待获取过程中线程被中断
     * @throws IllegalStateException 当 {@code maxKeys} 限制被触发时
     */
    public void lock(String key, long leaseTime, TimeUnit timeUnit) throws InterruptedException {
        long startTime = config.clock().millis();
        long startNanos = System.nanoTime();
        totalLocks.incrementAndGet();
        boolean perKeyMetrics = config.enablePerKeyMetrics();

        // maxKeys 限制:仅对"新 key"生效,已存在的 key 可重入
        if (config.maxKeys() > 0 && !lockMap.containsKey(key)) {
            if (countHeldEntries() >= config.maxKeys()) {
                failedLocks.incrementAndGet();
                totalWaitTime.addAndGet(config.clock().millis() - startTime);
                if (perKeyMetrics) {
                    LockEntry existing = lockMap.get(key);
                    if (existing != null) {
                        existing.recordAcquireAttempt();
                        existing.recordAcquireFailure(System.nanoTime() - startNanos);
                    }
                }
                fireFailed(key, System.nanoTime() - startNanos, "maxKeys");
                throw new IllegalStateException("maxKeys limit reached: " + config.maxKeys());
            }
        }

        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry(config.fairLock()));

        if (perKeyMetrics) {
            lockEntry.recordAcquireAttempt();
        }

        // Fire contended event if held by a different thread (mirrors tryLock semantics)
        if (lockEntry.lock.isLocked() && !lockEntry.lock.isHeldByCurrentThread()) {
            fireContended(key);
        }

        try {
            lockEntry.lock.lockInterruptibly();
            successLocks.incrementAndGet();
            lockEntry.touchLastAcquireTime();
            lockEntry.setOwnerThreadId(Thread.currentThread().getId());
            // 重新获取时复位 forceUnlock 哨兵
            lockEntry.clearForceUnlocked();
            if (leaseTime > 0) {
                long leaseEnd = config.clock().millis() + timeUnit.toMillis(leaseTime);
                lockEntry.setLeaseEndTime(leaseEnd);
                scheduleLeaseExpiry(key, leaseEnd);
            }
            if (perKeyMetrics) {
                lockEntry.recordAcquireSuccess(System.nanoTime() - startNanos);
            }
            fireAcquired(key, System.nanoTime() - startNanos);
        } catch (InterruptedException e) {
            failedLocks.incrementAndGet();
            if (perKeyMetrics) {
                lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
            }
            fireFailed(key, System.nanoTime() - startNanos, "interrupted");
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            totalWaitTime.addAndGet(config.clock().millis() - startTime);
        }
    }

    /**
     * 显式命名的可中断阻塞获取,语义等价于 {@link #lock(String, long, TimeUnit)}。
     * 提供该方法是为了在 API 层显式表达"可中断"语义,便于调用方在需要时区分
     * 普通阻塞 {@code lock} 与可中断阻塞。
     *
     * @param key 锁标识
     * @param leaseTime 租约时长(>0);0 表示无租约
     * @param timeUnit 时间单位
     * @throws InterruptedException 等待获取过程中线程被中断
     * @throws IllegalStateException 当 {@code maxKeys} 限制被触发时
     */
    public void lockInterruptibly(String key, long leaseTime, TimeUnit timeUnit) throws InterruptedException {
        lock(key, leaseTime, timeUnit);
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
     *
     * <p><b>注意:</b>如果该 key 曾被 {@link #forceUnlock(String)} 强制释放,
     * 底层 ReentrantLock 可能仍由本线程持有 — 此 unlock 会成功释放 AQS 状态
     * 并触发 {@link LockListener#onLockReleased} 事件,但 CHMRLock 视角下
     * 锁已被释放。建议在 unlock 前调用 {@link #isLocked(String)} 确认状态。</p>
     *
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
        long heldMillis = config.clock().millis() - lockEntry.getLastAcquireTime();
        fireReleased(key, heldMillis);
    }

    /**
     * 强制释放指定 key 的锁，可从任意线程调用，绕过 owner 检查。
     *
     * <p>由于 {@link java.util.concurrent.locks.ReentrantLock} 的 {@code unlock}
     * 要求 owner 线程，从其他线程真正释放底层锁需要反射 AQS 内部状态（脆弱、依赖 JDK 版本）。
     * 本方法的实际行为是：尝试调用底层 {@code unlock}（若当前线程即 owner 则成功；
     * 若非 owner 则会抛 {@link IllegalMonitorStateException}，被静默忽略），
     * 然后通过 {@link LockEntry} 中的 forceUnlock 哨兵把 owner / lease 状态清空，
     * 并让 {@link #isLocked(String)} 报告该 key 已释放。后续若同一线程再次调用
     * {@link #tryLock(String)} 成功，哨兵会被自动清除。</p>
     *
     * <p>对未知 key 是 no-op（idempotent）。对已释放 key 也是 no-op。</p>
     *
     * <p>需 {@link CHMRLockConfig#forceUnlockEnabled()} 为 true，否则抛
     * {@link UnsupportedOperationException}。</p>
     *
     * @param key 锁标识
     * @throws UnsupportedOperationException 若 {@code config.forceUnlockEnabled()} 为 false
     */
    public void forceUnlock(String key) {
        if (!config.forceUnlockEnabled()) {
            throw new UnsupportedOperationException(
                    "forceUnlock is not enabled. Set CHMRLockConfig.forceUnlockEnabled(true) to enable.");
        }
        LockEntry lockEntry = lockMap.get(key);
        if (lockEntry == null) {
            // 未知 key：幂等 no-op
            return;
        }
        // 尝试底层 unlock：若当前线程即 owner 则成功；非 owner 抛 IllegalMonitorStateException
        try {
            lockEntry.lock.unlock();
        } catch (IllegalMonitorStateException ignored) {
            // 跨线程 unlock 失败，仅清理 CHMRLock 状态
        }
        // 哨兵置位：isLocked 据此返回 false，owner / lease 清空
        lockEntry.markForceUnlocked();
        if (config.enablePerKeyMetrics()) {
            lockEntry.recordRelease(config.clock().millis());
        }
        fireReleased(key, config.clock().millis() - lockEntry.getLastAcquireTime());
    }

    /**
     * 判断 key 当前是否被锁。注意：若启用了租约，租约到期后此方法会返回 false，
     * 但底层 ReentrantLock 仍由原持有线程持有，直到调用 unlock。
     * 调用 {@link #forceUnlock(String)} 后此方法同样返回 false。
     */
    public boolean isLocked(String key) {
        LockEntry e = lockMap.get(key);
        if (e == null) return false;
        // forceUnlock 已置位：从 CHMRLock 视角看已释放
        if (e.isForceUnlocked()) return false;
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
     * @param key 锁标识
     * @return 当前持有该 key 的线程 id；key 不存在或未被持有返回 {@code null}
     */
    public Long getOwnerThreadId(String key) {
        LockEntry e = lockMap.get(key);
        return e == null ? null : e.getOwnerThreadId();
    }

    /**
     * @return 当前所有已注册 key 的不可变视图。对返回集合的修改会抛
     *         {@link UnsupportedOperationException}，但底层 map 的后续变更会反映在
     *         下次调用此方法的结果中。
     */
    public Set<String> getActiveKeys() {
        return Collections.unmodifiableSet(lockMap.keySet());
    }

    /**
     * 获取指定 key 的读写锁。与 tryLock/unlock 互不干扰 — 读写锁是独立的锁实例。
     * 同一 key 多次调用返回同一实例。
     *
     * <p><b>非可重入:</b>底层基于 {@link java.util.concurrent.locks.StampedLock},
     * 不支持重入;同一线程连续获取读/写锁会死锁。</p>
     *
     * <p>典型用法:</p>
     * <pre>{@code
     * KeyedReadWriteLock rw = lock.readWriteLock("resource");
     * long stamp = rw.readLock();
     * try {
     *     // 读取共享数据
     * } finally {
     *     rw.unlockRead(stamp);
     * }
     * }</pre>
     *
     * <p><b>生命周期:</b>读写锁缓存在 {@code rwLockMap} 中,生命周期与 CHMRLock 一致 —
     * 不受清理线程影响。shutdown 后,已返回的读写锁实例仍可使用。</p>
     *
     * @param key 锁标识
     * @return 该 key 对应的读写锁实例(同一 key 共享)
     * @see KeyedReadWriteLock
     */
    public KeyedReadWriteLock readWriteLock(String key) {
        return rwLockMap.computeIfAbsent(key, k -> new StampedKeyedReadWriteLock());
    }

    /**
     * 注册一个锁生命周期监听器。可重复注册同一个实例，但通常不需要。
     * @param listener 监听器实例（{@code null} 被忽略）
     */
    public void registerListener(LockListener listener) {
        if (listener != null) listeners.add(listener);
    }

    /** 注销一个监听器。{@code null} 被忽略；注销未注册的实例是 no-op。 */
    public void unregisterListener(LockListener listener) {
        if (listener == null) return;
        listeners.remove(listener);
    }

    private void fireAcquired(String key, long waitNanos) {
        for (LockListener l : listeners) {
            try { l.onLockAcquired(key, waitNanos); } catch (Exception e) {
                log.warning("LockListener.onLockAcquired threw: " + e);
            }
        }
    }

    private void fireReleased(String key, long heldMillis) {
        for (LockListener l : listeners) {
            try { l.onLockReleased(key, heldMillis); } catch (Exception e) {
                log.warning("LockListener.onLockReleased threw: " + e);
            }
        }
    }

    private void fireFailed(String key, long waitNanos, String reason) {
        for (LockListener l : listeners) {
            try { l.onLockFailed(key, waitNanos, reason); } catch (Exception e) {
                log.warning("LockListener.onLockFailed threw: " + e);
            }
        }
    }

    private void fireExpired(String key) {
        for (LockListener l : listeners) {
            try { l.onLockExpired(key); } catch (Exception e) {
                log.warning("LockListener.onLockExpired threw: " + e);
            }
        }
    }

    private void fireContended(String key) {
        for (LockListener l : listeners) {
            try { l.onLockContended(key); } catch (Exception e) {
                log.warning("LockListener.onLockContended threw: " + e);
            }
        }
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
