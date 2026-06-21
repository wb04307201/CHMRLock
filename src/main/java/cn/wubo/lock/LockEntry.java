package cn.wubo.lock;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个 key 对应的锁条目。包内可见,由 {@link CHMRLock} 持有。
 *
 * <p>字段说明:
 * <ul>
 *   <li>{@link #lock} — 底层 {@link ReentrantLock},可重入,非公平(由 {@link CHMRLockConfig#fairLock()} 决定)</li>
 *   <li>时间戳字段 ({@code lastAcquireTime}, {@code lastReleaseTime}, {@code leaseEndTime}) 使用 wall clock
 *       (来自注入的 {@link Clock});主要用于业务可观测性,不参与并发同步</li>
 *   <li>{@code acquireCount / successCount / failedCount / totalWaitNanos} — per-key 指标计数器</li>
 *   <li>{@code holderHoldCount} — 持有线程的重入深度,独立维护,与 {@link ReentrantLock#getHoldCount()}
 *       在跨线程读时一致(后者只对当前线程有意义,跨线程读为 0)。详细语义见该字段说明</li>
 *   <li>{@code stateFlags} — 哨兵位标记,使用单一 {@link AtomicInteger} 位运算保证
 *       {@code forceUnlocked} 与 {@code releaseEventFired} 的翻转原子性
 *       (修复 R6-B-7: 之前两个独立的 {@link java.util.concurrent.atomic.AtomicBoolean} 非原子翻转
 *       可能导致 onLockReleased 重复触发)</li>
 * </ul>
 *
 * @see CHMRLock
 */
class LockEntry {
    final ReentrantLock lock;
    private final AtomicLong lastAcquireTime = new AtomicLong(0);
    private final AtomicLong ownerThreadId = new AtomicLong(-1);
    private final AtomicLong leaseEndTime = new AtomicLong(Long.MAX_VALUE);
    private final LongAdder acquireCount = new LongAdder();
    private final LongAdder successCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();
    private final LongAdder totalWaitNanos = new LongAdder();
    // 持有线程的重入计数,独立于 ReentrantLock 的 thread-local 计数,
    // 用于 KeyStatistics 跨线程快照(snapshotFor) —
    // ReentrantLock.getHoldCount() 只对当前线程有意义,跨线程读为 0,
    // 与 ownerThreadId 不一致。该字段由 CHMRLock 在加锁/解锁/forceUnlock 时维护。
    //
    // R6 修复(E-9):从 AtomicInteger 改为 AtomicLong,避免极端重入深度下溢出。
    private final AtomicLong holderHoldCount = new AtomicLong(0);
    /** 释放时间戳(epoch millis); 0 表示从未释放。 */
    private final AtomicLong lastReleaseTime = new AtomicLong(0L);

    /**
     * 哨兵位标记。使用单一 {@link AtomicInteger} 位运算,保证 forceUnlocked 与
     * releaseEventFired 翻转的原子性。
     *
     * <p>R6 修复(B-7):之前两个独立的 {@code AtomicBoolean} 在
     * {@code clearForceUnlocked} 中非原子翻转,可能导致:
     * <ol>
     *   <li>另一线程在两次 set 之间看到 forceUnlocked=false 但 releaseEventFired=true,
     *       误判为"已 fire 过" → 不触发 onLockReleased 事件(丢失)</li>
     *   <li>或者看到 forceUnlocked=true 但 releaseEventFired=false,
     *       重复触发 onLockReleased 事件</li>
     * </ol>
     * 位运算 + {@code updateAndGet} 保证两个位在同一原子操作中翻转。</p>
     */
    private final AtomicInteger stateFlags = new AtomicInteger(0);
    /** 强制释放哨兵位:跨线程 forceUnlock 或租约到期(forceUnlockOnLeaseExpiry=true)时被置位。 */
    private static final int FLAG_FORCE_UNLOCKED = 1;
    /** 释放事件触发标记:forceUnlock 已触发一次 onLockReleased,后续 unlock 走 no-op。 */
    private static final int FLAG_RELEASE_EVENT_FIRED = 2;

    LockEntry() {
        this(false);
    }

    LockEntry(boolean fair) {
        this.lock = new ReentrantLock(fair);
    }

    long getLastAcquireTime() {
        return lastAcquireTime.get();
    }

    void touchLastAcquireTime(Clock clock) {
        lastAcquireTime.set(clock.millis());
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

    boolean isForceUnlocked() { return (stateFlags.get() & FLAG_FORCE_UNLOCKED) != 0; }

    /**
     * 标记该 entry 已被 {@code forceUnlock} 释放：清空 owner / lease 并置位哨兵。
     * 幂等:哨兵位一旦置位,后续调用不会改变状态。原子操作。
     *
     * <p>同时置位 {@code releaseEventFired} 位 — {@code forceUnlock} 已 fire
     * 一次 onLockReleased,后续 unlock 看到哨兵 + 此位,直接 no-op 避免重复触发。
     * 此前两阶段(先 markForceUnlocked 再 markReleaseEventFired)非原子,可能
     * 导致 unlock 路径在中间状态看到 releaseEventFired=false 而重复 fire。
     * 修复(B-7)后将两个位翻转合并为一次原子操作。</p>
     */
    void markForceUnlocked() {
        stateFlags.updateAndGet(f -> f | FLAG_FORCE_UNLOCKED | FLAG_RELEASE_EVENT_FIRED);
        clearOwner();
        clearLease();
    }

    /**
     * 清除 forceUnlock 哨兵以及 fireReleased 标记(用于下一次成功获取锁时复位状态)。
     * 原子操作:两个位同时清除。
     */
    void clearForceUnlocked() {
        stateFlags.updateAndGet(f -> f & ~(FLAG_FORCE_UNLOCKED | FLAG_RELEASE_EVENT_FIRED));
    }

    /**
     * 查询 {@code onLockReleased} 事件是否已在哨兵置位后触发过。
     * 用于 unlock 路径避免重复 fireReleased。
     */
    boolean isReleaseEventFired() { return (stateFlags.get() & FLAG_RELEASE_EVENT_FIRED) != 0; }

    /**
     * 标记哨兵置位后的 {@code onLockReleased} 事件已触发。原子置位,
     * 后续 unlock 看到此标记 + 哨兵粘性 → 直接 no-op。
     */
    void markReleaseEventFired() {
        stateFlags.updateAndGet(f -> f | FLAG_RELEASE_EVENT_FIRED);
    }

    long getAcquireCount() { return acquireCount.sum(); }
    long getSuccessCount() { return successCount.sum(); }
    long getFailedCount() { return failedCount.sum(); }
    long getTotalWaitNanos() { return totalWaitNanos.sum(); }
    long getLastReleaseTime() { return lastReleaseTime.get(); }

    void recordAcquireAttempt() { acquireCount.increment(); }
    void recordAcquireSuccess(long waitNanos) {
        successCount.increment();
        totalWaitNanos.add(waitNanos);
    }
    void recordAcquireFailure(long waitNanos) {
        failedCount.increment();
        totalWaitNanos.add(waitNanos);
    }
    void recordRelease(long epochMillis) { lastReleaseTime.set(epochMillis); }

    /** 持有线程的重入深度。{@link ReentrantLock#getHoldCount} 仅对当前线程有意义,
     *  本字段为跨线程快照提供一致的持有深度。 */
    long getHolderHoldCount() { return holderHoldCount.get(); }
    /** 加锁一次(包括重入)时调用。 */
    void incrementHoldCount() { holderHoldCount.incrementAndGet(); }
    /**
     * 解锁一次(包括重入)时调用。forceUnlock 重置为 0。
     *
     * <p>R6 修复(B-5):使用 CAS 循环将值钳制到 {@code >= 0}。
     * 在跨线程 forceUnlock 场景下,持有线程的 ReentrantLock 计数与 CHMRLock
     * 维护的 {@code holderHoldCount} 可能分裂:
     * <ul>
     *   <li>T1 持锁(ReentrantLock holdCount=1, CHMRLock=1)</li>
     *   <li>T2 forceUnlock: CHMRLock 重置为 0,ReentrantLock 不变(底层 lock 仍由 T1 持有)</li>
     *   <li>T1 重新 tryLock 成功: ReentrantLock holdCount=2, CHMRLock=1</li>
     *   <li>T1 unlock 一次: ReentrantLock 2→1, CHMRLock 1→0 ✓</li>
     *   <li>T1 unlock 再次: ReentrantLock 1→0, CHMRLock 0→-1 ✗ (溢出)</li>
     * </ul>
     * 钳制到 0 避免 holderHoldCount 变成负数(会被 {@link KeyStatistics#currentHoldCount()}
     * 暴露给监控)。底层 ReentrantLock 的 holdCount 仍然是正确的,因为跨线程 forceUnlock
     * 不可能修改它 — 这是 ReentrantLock 的根本限制,只能文档化。</p>
     */
    void decrementHoldCount() {
        long prev;
        do {
            prev = holderHoldCount.get();
            if (prev <= 0) {
                holderHoldCount.set(0);
                return;
            }
        } while (!holderHoldCount.compareAndSet(prev, prev - 1));
    }
    /** forceUnlock 路径:重置持有深度为 0。 */
    void resetHoldCount() { holderHoldCount.set(0); }
}