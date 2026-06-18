package cn.wubo.lock;

import java.util.Map;

/**
 * 统计持久化 SPI 接口。与 {@link MetricsExporter} 的区别:
 * <ul>
 *   <li>{@code MetricsExporter} —— 一次性的指标导出(适合推送到 Prometheus / StatsD 等)</li>
 *   <li>{@code StatisticsSink} —— 持续累积的持久化存储(适合写入数据库 / 时序数据库 / 日志文件等)</li>
 * </ul>
 *
 * <p>实现应保证:</p>
 * <ul>
 *   <li>线程安全:多线程并发调用 {@link #record(MonitorMetrics, Map)} 都必须安全</li>
 *   <li>异常隔离:底层持久化系统的异常应捕获并记录,不应传播</li>
 *   <li>幂等性:同一次 record 调用失败时不应影响后续 record 调用</li>
 * </ul>
 *
 * <p>典型用法(JDBC 实现):</p>
 * <pre>{@code
 * public class JdbcStatisticsSink implements StatisticsSink {
 *     private final DataSource ds;
 *     public JdbcStatisticsSink(DataSource ds) { this.ds = ds; }
 *
 *     public void record(MonitorMetrics global, Map<String, KeyStatistics> perKey) {
 *         try (Connection c = ds.getConnection();
 *              PreparedStatement ps = c.prepareStatement(
 *                  "INSERT INTO lock_stats (ts, total, success, failed) VALUES (?, ?, ?, ?)")) {
 *             ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
 *             ps.setLong(2, global.getTotalLocks());
 *             ps.setLong(3, global.getSuccessLocks());
 *             ps.setLong(4, global.getFailedLocks());
 *             ps.executeUpdate();
 *         } catch (SQLException e) {
 *             log.warn("record stats failed", e);
 *         }
 *     }
 * }
 *
 * // CHMRLock 调用(由调用方自行周期性触发,CHMRLock 当前不内置调度):
 * ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
 * CHMRLock lockManager = new CHMRLock();
 * scheduler.scheduleAtFixedRate(() -> {
 *     lockManager.getAllStatistics();  // 获取全局+perKey 快照
 *     // ...调用 sink.record(...) 由调用方自行决定如何传入
 * }, 0, 30, TimeUnit.SECONDS);
 * }</pre>
 *
 * @since 2.0.0
 */
public interface StatisticsSink {

    /**
     * 记录一次统计快照。
     * @param global 全局统计指标
     * @param perKey 每个 key 的统计指标(key 集合可能为空)
     */
    void record(MonitorMetrics global, Map<String, KeyStatistics> perKey);
}
