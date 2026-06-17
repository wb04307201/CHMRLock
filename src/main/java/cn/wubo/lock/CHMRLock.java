package cn.wubo.lock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class CHMRLock {
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


    public CHMRLock() {
        this(3_000, TimeUnit.MILLISECONDS);
    }

    public CHMRLock(long defaultWaitTime, TimeUnit timeUnit) {
        this.defaultWaitTime = timeUnit.toMillis(defaultWaitTime);
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();

        // 启动后台清理线程
        startCleanupThread();
    }

    /**
     * 启动后台定时清理线程，定期执行清理任务
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            long expiryThreshold = 5 * 60_000; // 5分钟未使用则清理

            lockMap.entrySet().removeIf(entry -> {
                LockEntry lockEntry = entry.getValue();
                // 检查锁是否未被占用且长时间未使用
                return !lockEntry.lock.isLocked() &&
                        (currentTime - lockEntry.getLastAcquireTime() > expiryThreshold);
            });
        }, 1, 1, TimeUnit.SECONDS); // 每秒执行一次清理
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
        long startTime = System.currentTimeMillis();
        totalLocks.incrementAndGet();
        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry());

        try {
            boolean acquired = lockEntry.lock.tryLock(waitTime, timeUnit);

            if (acquired) {
                successLocks.incrementAndGet();
                lockEntry.lastAcquireTime = System.currentTimeMillis();
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

    /**
     * 获取锁（指定等待时间）
     */
    public boolean tryLock(String key, long waitTime){
        return tryLock(key, waitTime, TimeUnit.MILLISECONDS);
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
    }

    public void shutdown() {
        lockMap.clear();
        if (cleanupExecutor != null) {
            cleanupExecutor.shutdown();
        }
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
