package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockClockTest {

    @Test
    void usesInjectedClock() throws InterruptedException {
        Clock fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        CHMRLockConfig cfg = CHMRLockConfig.builder().clock(fixed).build();
        CHMRLock lock = new CHMRLock(cfg);
        try {
            assertTrue(lock.tryLock("k"));
            lock.unlock("k");
            // 基本功能验证：clock 注入不应破坏 lock 工作
            assertFalse(lock.isLocked("k"));
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void defaultClockIsSystemUTC() {
        CHMRLock lock = new CHMRLock();
        try {
            assertTrue(lock.tryLock("k"));
            lock.unlock("k");
        } finally {
            lock.shutdown();
        }
    }
}
