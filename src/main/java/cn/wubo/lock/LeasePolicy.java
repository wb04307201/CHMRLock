package cn.wubo.lock;

import java.time.Duration;

/**
 * 按 key 动态决定租约时长的策略接口。函数式接口，可用 lambda 简洁实现。
 *
 * <p>典型用法：
 * <pre>{@code
 * CHMRLockConfig cfg = CHMRLockConfig.builder()
 *     .leasePolicy(key -> {
 *         if (key.startsWith("config:")) return Duration.ofHours(1);
 *         if (key.startsWith("session:")) return Duration.ofSeconds(30);
 *         return Duration.ofMinutes(5);  // 默认
 *     })
 *     .build();
 * CHMRLock lock = new CHMRLock(cfg);
 * }</pre>
 *
 * <p>若策略返回 {@code null} 或 {@code Duration.ZERO}，表示该 key 不启用租约。
 * 若未配置 leasePolicy（默认），则使用 {@link CHMRLockConfig#defaultLeaseTime()}。
 *
 * @since 2.1.0
 */
@FunctionalInterface
public interface LeasePolicy {

    /**
     * 返回指定 key 的租约时长。
     *
     * @param key 锁标识（不会为 null 或空串）
     * @return 租约时长；返回 {@code null} 或 {@code Duration.ZERO} 表示不启用租约
     */
    Duration leaseFor(String key);
}
