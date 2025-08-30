package cn.wubo.lock;


/**
 * 监控指标类
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

    public long getTotalLocks() {
        return totalLocks;
    }

    public long getSuccessLocks() {
        return successLocks;
    }

    public long getFailedLocks() {
        return failedLocks;
    }

    public long getTotalWaitTime() {
        return totalWaitTime;
    }

    public double getSuccessRate() {
        return totalLocks > 0 ? (double) successLocks / totalLocks : 0.0;
    }

    public long getAvgWaitTime() {
        return totalLocks > 0 ? totalWaitTime / totalLocks : 0;
    }
}
