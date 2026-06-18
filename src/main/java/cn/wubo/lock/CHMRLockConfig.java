package cn.wubo.lock;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * CHMRLock 的不可变配置记录。通过 {@link Builder} 构造,使用 {@link #defaults()} 获取默认配置。
 *
 * <p>默认配置见 {@link #defaults()}:等待 3 秒、租约 0 (无)、空闲 5 分钟、
 * 清理间隔 1 秒、不限 key 数、守护清理线程、租约到期强制释放 (仅清理状态)、
 * 非公平锁、不收集 per-key metrics、禁用 forceUnlock、系统 UTC 时钟、
 * 异步池最多 {@code Runtime.getRuntime().availableProcessors() * 4} 线程、
 * 异步任务队列 1024。</p>
 *
 * @see CHMRLock#CHMRLock(CHMRLockConfig)
 */
public record CHMRLockConfig(
        Duration defaultWaitTime,
        Duration defaultLeaseTime,
        Duration idleThreshold,
        Duration cleanupInterval,
        long maxKeys,
        boolean daemonCleanupThread,
        boolean forceUnlockOnLeaseExpiry,
        boolean fairLock,
        boolean enablePerKeyMetrics,
        boolean forceUnlockEnabled,
        Clock clock,
        int asyncMaxThreads,
        int asyncQueueCapacity
) {
    public CHMRLockConfig {
        Objects.requireNonNull(defaultWaitTime, "defaultWaitTime");
        Objects.requireNonNull(defaultLeaseTime, "defaultLeaseTime");
        Objects.requireNonNull(idleThreshold, "idleThreshold");
        Objects.requireNonNull(cleanupInterval, "cleanupInterval");
        Objects.requireNonNull(clock, "clock");
        if (maxKeys < 0) throw new IllegalArgumentException("maxKeys 必须 >= 0");
        if (defaultWaitTime.isNegative()) throw new IllegalArgumentException("defaultWaitTime 不能为负");
        if (defaultLeaseTime.isNegative()) throw new IllegalArgumentException("defaultLeaseTime 不能为负");
        if (idleThreshold.isNegative()) throw new IllegalArgumentException("idleThreshold 不能为负");
        if (cleanupInterval.isZero() || cleanupInterval.isNegative()) throw new IllegalArgumentException("cleanupInterval 必须为正");
        if (asyncMaxThreads <= 0) throw new IllegalArgumentException("asyncMaxThreads 必须 > 0");
        if (asyncQueueCapacity <= 0) throw new IllegalArgumentException("asyncQueueCapacity 必须 > 0");
    }

    /** @return 包含所有默认值的 {@link CHMRLockConfig} 实例。 */
    public static CHMRLockConfig defaults() {
        int defaultMaxThreads = Math.max(1, Runtime.getRuntime().availableProcessors() * 4);
        return new CHMRLockConfig(
                Duration.ofSeconds(3), Duration.ZERO,
                Duration.ofMinutes(5), Duration.ofSeconds(1),
                0L, true, true, false, false, false, Clock.systemUTC(),
                defaultMaxThreads, 1024);
    }

    /** @return 新的 {@link Builder} 实例,所有字段预填为默认值。 */
    public static Builder builder() { return new Builder(); }

    /**
     * {@link CHMRLockConfig} 的流式构建器。每个 setter 立即返回 {@code this},便于链式调用。
     */
    public static final class Builder {
        private Duration defaultWaitTime = Duration.ofSeconds(3);
        private Duration defaultLeaseTime = Duration.ZERO;
        private Duration idleThreshold = Duration.ofMinutes(5);
        private Duration cleanupInterval = Duration.ofSeconds(1);
        private long maxKeys = 0L;
        private boolean daemonCleanupThread = true;
        private boolean forceUnlockOnLeaseExpiry = true;
        private boolean fairLock = false;
        private boolean enablePerKeyMetrics = false;
        private boolean forceUnlockEnabled = false;
        private Clock clock = Clock.systemUTC();
        private int asyncMaxThreads = Math.max(1, Runtime.getRuntime().availableProcessors() * 4);
        private int asyncQueueCapacity = 1024;

        /**
         * @param d {@code tryLock(key)} 的默认等待时长,不能为 null,不能为负
         * @throws NullPointerException 若 {@code d} 为 null
         * @throws IllegalArgumentException 若 {@code d} 为负
         */
        public Builder defaultWaitTime(Duration d) {
            Objects.requireNonNull(d, "defaultWaitTime must not be null");
            if (d.isNegative()) throw new IllegalArgumentException("defaultWaitTime must not be negative");
            this.defaultWaitTime = d; return this;
        }
        /**
         * @param d 默认租约时长,0 表示无租约,不能为 null,不能为负
         * @throws NullPointerException 若 {@code d} 为 null
         * @throws IllegalArgumentException 若 {@code d} 为负
         */
        public Builder defaultLeaseTime(Duration d) {
            Objects.requireNonNull(d, "defaultLeaseTime must not be null");
            if (d.isNegative()) throw new IllegalArgumentException("defaultLeaseTime must not be negative");
            this.defaultLeaseTime = d; return this;
        }
        /**
         * @param d 空闲 entry 的清理阈值(超过此时间且未被持有则被清理),不能为 null,不能为负
         * @throws NullPointerException 若 {@code d} 为 null
         * @throws IllegalArgumentException 若 {@code d} 为负
         */
        public Builder idleThreshold(Duration d) {
            Objects.requireNonNull(d, "idleThreshold must not be null");
            if (d.isNegative()) throw new IllegalArgumentException("idleThreshold must not be negative");
            this.idleThreshold = d; return this;
        }
        /**
         * @param d 清理线程的执行周期,不能为 null,必须为正(零或负都会立即抛异常)
         * @throws NullPointerException 若 {@code d} 为 null
         * @throws IllegalArgumentException 若 {@code d} 为零或负
         */
        public Builder cleanupInterval(Duration d) {
            Objects.requireNonNull(d, "cleanupInterval must not be null");
            if (d.isZero() || d.isNegative())
                throw new IllegalArgumentException("cleanupInterval must be positive");
            this.cleanupInterval = d; return this;
        }
        /**
         * @param n 同时持有的 key 数量上限(0 表示不限制);仅对"新 key"生效,已存在 key 可重入
         * @throws IllegalArgumentException 若 {@code n} 为负
         */
        public Builder maxKeys(long n) {
            if (n < 0) throw new IllegalArgumentException("maxKeys must be >= 0");
            this.maxKeys = n; return this;
        }
        /** @param b 清理线程是否设为守护线程(JVM 退出时自动终止) */
        public Builder daemonCleanupThread(boolean b) { this.daemonCleanupThread = b; return this; }
        /** @param b 租约到期时是否清理 owner / lease 状态(底层 ReentrantLock 仍由原线程持有) */
        public Builder forceUnlockOnLeaseExpiry(boolean b) { this.forceUnlockOnLeaseExpiry = b; return this; }
        /** @param b 是否使用公平锁(影响吞吐,仅在确有 FIFO 需求时启用) */
        public Builder fairLock(boolean b) { this.fairLock = b; return this; }
        /**
         * @param b 是否启用 per-key 统计;启用后 {@link CHMRLock#getStatistics(String)} 与
         *          {@link CHMRLock#getAllStatistics()} 返回真实数据,否则返回空
         */
        public Builder enablePerKeyMetrics(boolean b) { this.enablePerKeyMetrics = b; return this; }
        /**
         * @param b 是否启用 {@link CHMRLock#forceUnlock(String)};出于安全考虑默认关闭,
         *          启用后跨线程强制释放才会生效,否则抛 {@link UnsupportedOperationException}
         */
        public Builder forceUnlockEnabled(boolean b) { this.forceUnlockEnabled = b; return this; }
        /**
         * @param c 用于获取当前时间的 {@link Clock};不能为 null
         * @throws NullPointerException 若 {@code c} 为 null
         */
        public Builder clock(Clock c) {
            Objects.requireNonNull(c, "clock must not be null");
            this.clock = c; return this;
        }
        /**
         * @param n 异步获取锁执行器的最大线程数(默认 {@code availableProcessors * 4});
         *          必须 &gt; 0。超过该线程数且队列满时,新任务将由调用线程执行以提供背压
         *          (避免无界 cached 线程池在高并发下创建过多线程)。
         * @throws IllegalArgumentException 若 {@code n} &lt;= 0
         */
        public Builder asyncMaxThreads(int n) {
            if (n <= 0) throw new IllegalArgumentException("asyncMaxThreads must be > 0");
            this.asyncMaxThreads = n; return this;
        }
        /**
         * @param n 异步获取锁任务队列容量(默认 1024);必须 &gt; 0。
         *          队列满时,新任务将由调用线程执行。
         * @throws IllegalArgumentException 若 {@code n} &lt;= 0
         */
        public Builder asyncQueueCapacity(int n) {
            if (n <= 0) throw new IllegalArgumentException("asyncQueueCapacity must be > 0");
            this.asyncQueueCapacity = n; return this;
        }
        /**
         * 构造不可变配置。任意字段非法(null / 负值)都会抛 {@link IllegalArgumentException}。
         *
         * <p>R6 修复(L-1/L-2):Builder setter 现在也立即校验字段合法性,提供更早的反馈;
         * 之前校验只在 {@code build()} / record canonical constructor 阶段,可能导致
         * 在 builder 链中累积多个错误,排查不友好。</p>
         *
         * @return 校验后的 {@link CHMRLockConfig}
         */
        public CHMRLockConfig build() {
            return new CHMRLockConfig(defaultWaitTime, defaultLeaseTime,
                    idleThreshold, cleanupInterval, maxKeys,
                    daemonCleanupThread, forceUnlockOnLeaseExpiry,
                    fairLock, enablePerKeyMetrics, forceUnlockEnabled, clock,
                    asyncMaxThreads, asyncQueueCapacity);
        }
    }
}
