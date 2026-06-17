package cn.wubo.lock;


/**
 * 全局锁监控指标快照(不可变)。由 {@link CHMRLock#getStatistics()} 返回,
 * 反映 CHMRLock 实例自创建以来的累计统计。
 *
 * <p>所有计数与时间均为单调累计值,不含重置语义。{@link #getSuccessRate()} 与
 * {@link #getAvgWaitTime()} 按需计算,不做缓存。</p>
 */
public class MonitorMetrics {
    private final long totalLocks;
    private final long successLocks;
    private final long failedLocks;
    private final long totalWaitTime;

    public MonitorMetrics(long totalLocks, long successLocks, long failedLocks, long totalWaitTime) {
        this.totalLocks = totalLocks;
        this.successLocks = successLocks;
        this.failedLocks = failedLocks;
        this.totalWaitTime = totalWaitTime;
    }

    /** @return 累计的获取锁请求总数(含成功与失败) */
    public long getTotalLocks() {
        return totalLocks;
    }

    /** @return 累计的成功获取锁次数 */
    public long getSuccessLocks() {
        return successLocks;
    }

    /** @return 累计的获取失败次数(超时 / maxKeys / 中断) */
    public long getFailedLocks() {
        return failedLocks;
    }

    /** @return 累计等待时间(毫秒),包含成功与失败两种获取的总等待时长 */
    public long getTotalWaitTime() {
        return totalWaitTime;
    }

    /** @return 成功率,范围 {@code [0.0, 1.0]};{@link #getTotalLocks()} 为 0 时返回 0.0 */
    public double getSuccessRate() {
        return totalLocks > 0 ? (double) successLocks / totalLocks : 0.0;
    }

    /** @return 平均每次获取的等待时长(毫秒);{@link #getTotalLocks()} 为 0 时返回 0.0 */
    public double getAvgWaitTime() {
        return totalLocks > 0 ? (double) totalWaitTime / totalLocks : 0.0;
    }
}
