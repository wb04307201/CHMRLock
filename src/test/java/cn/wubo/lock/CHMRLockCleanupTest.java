package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockCleanupTest {

    @Test
    void idleEntryIsCleanedUp() throws InterruptedException {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(200))
                .cleanupInterval(Duration.ofMillis(100))
                .build();
        CHMRLock lock = new CHMRLock(cfg);
        try {
            assertTrue(lock.tryLock("ephemeral"));
            lock.unlock("ephemeral");
            assertTrue(lock.getActiveKeys().contains("ephemeral"));
            // 等待清理线程回收（idle=200ms + cleanup=100ms + 余量）
            long deadline = System.currentTimeMillis() + 2000;
            while (!lock.getActiveKeys().isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(lock.getActiveKeys().isEmpty(), "空闲 entry 应被清理");
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void heldEntryIsNotCleanedUp() throws InterruptedException {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .idleThreshold(Duration.ofMillis(200))
                .cleanupInterval(Duration.ofMillis(100))
                .build();
        CHMRLock lock = new CHMRLock(cfg);
        try {
            assertTrue(lock.tryLock("held"));
            Thread.sleep(500);
            assertTrue(lock.getActiveKeys().contains("held"),
                    "持有中的 entry 不应被清理");
        } finally {
            lock.unlock("held");
            lock.shutdown();
        }
    }
}
