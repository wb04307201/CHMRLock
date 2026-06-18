package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockConfigTest {

    @Test
    void defaultsAreValid() {
        CHMRLockConfig cfg = CHMRLockConfig.defaults();
        assertNotNull(cfg);
        assertEquals(Duration.ofSeconds(3), cfg.defaultWaitTime());
    }

    @Test
    void rejectsNullDefaultWaitTime() {
        assertThrows(NullPointerException.class,
                () -> CHMRLockConfig.builder().defaultWaitTime(null).build());
    }

    @Test
    void rejectsNullClock() {
        assertThrows(NullPointerException.class,
                () -> CHMRLockConfig.builder().clock(null).build());
    }

    @Test
    void rejectsNegativeMaxKeys() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().maxKeys(-1).build());
    }

    @Test
    void rejectsNegativeDefaultWaitTime() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().defaultWaitTime(Duration.ofMillis(-1)).build());
    }

    @Test
    void rejectsNegativeIdleThreshold() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().idleThreshold(Duration.ofMillis(-1)).build());
    }

    @Test
    void rejectsNegativeCleanupInterval() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().cleanupInterval(Duration.ofMillis(-1)).build());
    }

    @Test
    void rejectsNegativeDefaultLeaseTime() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMRLockConfig.builder().defaultLeaseTime(Duration.ofMillis(-1)).build());
    }

    @Test
    void allowsZeroMaxKeys() {
        // 0 = 不限
        assertDoesNotThrow(() -> CHMRLockConfig.builder().maxKeys(0).build());
    }

    @Test
    void allowsZeroDurations() {
        // 0 表示无租约 / 不清理
        assertDoesNotThrow(() -> CHMRLockConfig.builder()
                .defaultLeaseTime(Duration.ZERO)
                .build());
    }
}
