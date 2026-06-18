/**
 * CHMRLock —— 基于 {@link java.util.concurrent.ConcurrentHashMap}
 * 与 {@link java.util.concurrent.locks.ReentrantLock} 的细粒度单机锁库。
 *
 * <p>主要入口: {@link cn.wubo.lock.CHMRLock}</p>
 *
 * <h2>核心能力</h2>
 * <ul>
 *   <li>细粒度 key 加锁,不同 key 互不影响</li>
 *   <li>租约模式,超时自动报告释放</li>
 *   <li>多 key 原子加锁(死锁防护)</li>
 *   <li>异步获取与强制释放</li>
 *   <li>StampedLock 实现的非可重入读写锁</li>
 *   <li>per-key 与全局统计</li>
 *   <li>事件监听与指标导出</li>
 *   <li>分布式锁 SPI 接入</li>
 * </ul>
 *
 * @since 1.0.0
 */
package cn.wubo.lock;
