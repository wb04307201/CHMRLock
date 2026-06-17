package cn.wubo.lock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class LockEntry {
    final ReentrantLock lock;
    private final AtomicLong lastAcquireTime = new AtomicLong(0);
    private final AtomicLong ownerThreadId = new AtomicLong(-1);
    private final AtomicLong leaseEndTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong acquireCount = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong totalWaitNanos = new AtomicLong(0);
    /** 释放时间戳(epoch millis); 0 表示从未释放。 */
    private final AtomicLong lastReleaseTime = new AtomicLong(0L);

    LockEntry() {
        this(false);
    }

    LockEntry(boolean fair) {
        this.lock = new ReentrantLock(fair);
    }

    long getLastAcquireTime() {
        return lastAcquireTime.get();
    }

    void touchLastAcquireTime() {
        lastAcquireTime.set(System.currentTimeMillis());
    }

    Long getOwnerThreadId() {
        long id = ownerThreadId.get();
        return id < 0 ? null : id;
    }

    void setOwnerThreadId(long id) {
        ownerThreadId.set(id);
    }

    void clearOwner() {
        ownerThreadId.set(-1);
    }

    long getLeaseEndTime() { return leaseEndTime.get(); }

    void setLeaseEndTime(long epochMillis) { leaseEndTime.set(epochMillis); }

    void clearLease() { leaseEndTime.set(Long.MAX_VALUE); }

    long getAcquireCount() { return acquireCount.get(); }
    long getSuccessCount() { return successCount.get(); }
    long getFailedCount() { return failedCount.get(); }
    long getTotalWaitNanos() { return totalWaitNanos.get(); }
    long getLastReleaseTime() { return lastReleaseTime.get(); }

    void recordAcquireAttempt() { acquireCount.incrementAndGet(); }
    void recordAcquireSuccess(long waitNanos) {
        successCount.incrementAndGet();
        totalWaitNanos.addAndGet(waitNanos);
    }
    void recordAcquireFailure(long waitNanos) {
        failedCount.incrementAndGet();
        totalWaitNanos.addAndGet(waitNanos);
    }
    void recordRelease(long epochMillis) { lastReleaseTime.set(epochMillis); }
}
