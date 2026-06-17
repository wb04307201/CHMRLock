package cn.wubo.lock;

/**
 * 单个 key 的统计指标。
 * 由 CHMRLock.getStatistics(String key) 返回，仅在 CHMRLockConfig.enablePerKeyMetrics=true 时收集。
 */
public record KeyStatistics(
        /** 锁标识。 */
        String key,
        /** 累计的获取尝试次数(含成功与失败)。 */
        long acquireCount,
        /** 累计的成功获取次数。 */
        long successCount,
        /** 累计的获取失败次数(超时 / maxKeys / 中断)。 */
        long failedCount,
        /**
         * 累计等待时间(纳秒)。通过 System.nanoTime() 测量，精度为纳秒级。
         * 注意:包含成功与失败两种获取的总等待时间。
         */
        long totalWaitNanos,
        /** 最近一次成功获取锁的 epoch 毫秒时间戳;0 表示从未成功获取。 */
        long lastAcquireEpochMs,
        /** 最近一次成功释放锁的 epoch 毫秒时间戳;0 表示从未释放。 */
        long lastReleaseEpochMs,
        /** 当前线程对该 key 的重入深度(0 表示未持有)。 */
        int currentHoldCount,
        /** 当前持有该 key 的线程 id;-1 表示未被持有。 */
        long currentHolderThreadId
) {

    /** @return 成功率,范围 {@code [0.0, 1.0]};{@link #acquireCount()} 为 0 时返回 0.0 */
    public double getSuccessRate() {
        return acquireCount > 0 ? (double) successCount / acquireCount : 0.0;
    }

    /** @return 平均每次获取的等待时长(毫秒);{@link #acquireCount()} 为 0 时返回 0.0 */
    public double getAvgWaitTimeMillis() {
        return acquireCount > 0 ? (double) totalWaitNanos / acquireCount / 1_000_000.0 : 0.0;
    }
}
