package cn.wubo.lock;

/**
 * 单个 key 的统计指标。
 * 由 CHMRLock.getStatistics(String key) 返回，仅在 CHMRLockConfig.enablePerKeyMetrics=true 时收集。
 */
public record KeyStatistics(
        String key,
        long acquireCount,
        long successCount,
        long failedCount,
        /**
         * 累计等待时间(纳秒)。通过 System.nanoTime() 测量，精度为纳秒级。
         * 注意:包含成功与失败两种获取的总等待时间。
         */
        long totalWaitNanos,
        long lastAcquireEpochMs,
        long lastReleaseEpochMs,
        int currentHoldCount,
        long currentHolderThreadId
) {

    public double getSuccessRate() {
        return acquireCount > 0 ? (double) successCount / acquireCount : 0.0;
    }

    public double getAvgWaitTimeMillis() {
        return acquireCount > 0 ? (double) totalWaitNanos / acquireCount / 1_000_000.0 : 0.0;
    }
}
