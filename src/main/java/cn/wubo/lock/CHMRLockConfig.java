package cn.wubo.lock;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

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
        Clock clock
) {
    public CHMRLockConfig {
        Objects.requireNonNull(defaultWaitTime, "defaultWaitTime");
        Objects.requireNonNull(defaultLeaseTime, "defaultLeaseTime");
        Objects.requireNonNull(idleThreshold, "idleThreshold");
        Objects.requireNonNull(cleanupInterval, "cleanupInterval");
        Objects.requireNonNull(clock, "clock");
        if (maxKeys < 0) throw new IllegalArgumentException("maxKeys 必须 >= 0");
        if (defaultWaitTime.isNegative()) throw new IllegalArgumentException("defaultWaitTime 不能为负");
    }

    public static CHMRLockConfig defaults() {
        return new CHMRLockConfig(
                Duration.ofSeconds(3), Duration.ZERO,
                Duration.ofMinutes(5), Duration.ofSeconds(1),
                0L, true, true, false, false, Clock.systemUTC());
    }

    public static Builder builder() { return new Builder(); }

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
        private Clock clock = Clock.systemUTC();

        public Builder defaultWaitTime(Duration d) { this.defaultWaitTime = d; return this; }
        public Builder defaultLeaseTime(Duration d) { this.defaultLeaseTime = d; return this; }
        public Builder idleThreshold(Duration d) { this.idleThreshold = d; return this; }
        public Builder cleanupInterval(Duration d) { this.cleanupInterval = d; return this; }
        public Builder maxKeys(long n) { this.maxKeys = n; return this; }
        public Builder daemonCleanupThread(boolean b) { this.daemonCleanupThread = b; return this; }
        public Builder forceUnlockOnLeaseExpiry(boolean b) { this.forceUnlockOnLeaseExpiry = b; return this; }
        public Builder fairLock(boolean b) { this.fairLock = b; return this; }
        public Builder enablePerKeyMetrics(boolean b) { this.enablePerKeyMetrics = b; return this; }
        public Builder clock(Clock c) { this.clock = c; return this; }
        public CHMRLockConfig build() {
            return new CHMRLockConfig(defaultWaitTime, defaultLeaseTime,
                    idleThreshold, cleanupInterval, maxKeys,
                    daemonCleanupThread, forceUnlockOnLeaseExpiry,
                    fairLock, enablePerKeyMetrics, clock);
        }
    }
}
