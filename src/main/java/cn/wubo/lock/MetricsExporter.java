package cn.wubo.lock;

import java.util.Map;

/**
 * 指标导出 SPI。实现类负责把 CHMRLock 的指标序列化为目标格式( JSON / Prometheus / StatsD 等)。
 *
 * <p>调用方通过 {@code CHMRLock.registerMetricsExporter} 注册(将在 Task 38 完成)
 * 或通过 {@code CHMRLock.exportMetrics(exporter)} 主动触发(将在 Task 38 完成)。</p>
 */
public interface MetricsExporter {
    /**
     * 导出指标快照。
     * @param global 全局统计指标
     * @param perKey 各 key 的统计指标(可为空;表示 metrics 未启用)
     */
    void export(MonitorMetrics global, Map<String, KeyStatistics> perKey);
}
