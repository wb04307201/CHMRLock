package cn.wubo.lock;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

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


    public CHMRLock() {
        this(CHMRLockConfig.defaults());
    }

    public CHMRLock(long defaultWaitTime, TimeUnit timeUnit) {
        this(CHMRLockConfig.builder()
                .defaultWaitTime(Duration.ofMillis(timeUnit.toMillis(defaultWaitTime)))
                .build());
    }

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
     * 获取锁（使用默认等待时间）
     */
    public boolean tryLock(String key) {
        return tryLock(key, defaultWaitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取锁（指定等待时间）
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
        long startTime = System.currentTimeMillis();
        totalLocks.incrementAndGet();

        // maxKeys 限制：仅对"新 key"（不在 map 中）生效，已存在的 key 可重入
        // 计数维度：仅统计当前"持有中"的 entry（isLocked == true），
        // 释放后的 entry 不占 slot，否则 unlock → tryLock 新 key 的常见模式会被阻塞
        if (config.maxKeys() > 0 && !lockMap.containsKey(key)) {
            if (countHeldEntries() >= config.maxKeys()) {
                failedLocks.incrementAndGet();
                totalWaitTime.addAndGet(System.currentTimeMillis() - startTime);
                return false;
            }
        }

        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry());

        try {
            boolean acquired = lockEntry.lock.tryLock(waitTime, timeUnit);
            if (acquired) {
                successLocks.incrementAndGet();
                lockEntry.touchLastAcquireTime();
                lockEntry.setOwnerThreadId(Thread.currentThread().getId());
                if (leaseTime > 0) {
                    long leaseEnd = System.currentTimeMillis() + timeUnit.toMillis(leaseTime);
                    lockEntry.setLeaseEndTime(leaseEnd);
                    scheduleLeaseExpiry(key, leaseEnd);
                }
                return true;
            } else {
                failedLocks.incrementAndGet();
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedLocks.incrementAndGet();
            return false;
        } finally {
            totalWaitTime.addAndGet(System.currentTimeMillis() - startTime);
        }
    }

    private void scheduleLeaseExpiry(String key, long leaseEndMillis) {
        long delayMillis = Math.max(0, leaseEndMillis - System.currentTimeMillis());
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
     * 获取锁（指定等待时间）
     */
    public boolean tryLock(String key, long waitTime){
        return tryLock(key, waitTime, TimeUnit.MILLISECONDS);
    }

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
        if (System.currentTimeMillis() > e.getLeaseEndTime()) {
            return false;
        }
        return true;
    }

    public boolean isHeldByCurrentThread(String key) {
        LockEntry e = lockMap.get(key);
        return e != null && e.lock.isHeldByCurrentThread();
    }

    public int getHoldCount(String key) {
        LockEntry e = lockMap.get(key);
        return e == null ? 0 : e.lock.getHoldCount();
    }

    public Set<String> getActiveKeys() {
        return Collections.unmodifiableSet(lockMap.keySet());
    }

    public void shutdown() {
        if (shutdownCalled.compareAndSet(false, true)) {
            lockMap.clear();
            if (cleanupExecutor != null) {
                cleanupExecutor.shutdown();
            }
        }
    }

    public boolean isShutdown() {
        return shutdownCalled.get();
    }

    @Override
    public void close() {
        shutdown();
    }

    public MonitorMetrics getStatistics() {
        return new MonitorMetrics(
                totalLocks.get(),
                successLocks.get(),
                failedLocks.get(),
                totalWaitTime.get()
        );
    }
}
