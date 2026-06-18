package cn.wubo.lock;

import java.util.Map;

/**
 * 指标导出 SPI。实现类负责把 CHMRLock 的指标序列化为目标格式( JSON / Prometheus / StatsD 等)。
 *
 * <p>调用方通过 {@link CHMRLock#exportMetrics(MetricsExporter)} 主动触发导出 —
 * CHMRLock 当前不内置周期性导出线程,如有需要请在外部自行调度。
 * 若需要持续累积的持久化存储,请使用 {@link StatisticsSink} 并自行管理调度。</p>
 *
 * @see JsonMetricsExporter
 * @see StatisticsSink
 * @since 1.2.0
 */
public interface MetricsExporter {
    /**
     * 导出指标快照。实现类应保证调用本方法为轻量操作(只读快照),
     * 不得修改传入的 {@code global} 或 {@code perKey}。
     *
     * @param global 全局统计指标
     * @param perKey 各 key 的统计指标(可为空;表示 metrics 未启用)
     */
    void export(MonitorMetrics global, Map<String, KeyStatistics> perKey);
}
