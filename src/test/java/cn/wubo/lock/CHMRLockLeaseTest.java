package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockLeaseTest {

    private CHMRLock lock;

    @BeforeEach
    void setUp() { lock = new CHMRLock(); }

    @AfterEach
    void tearDown() { lock.shutdown(); }

    @Test
    void leaseTimeZeroMeansNoLease() throws InterruptedException {
        assertTrue(lock.tryLock("k", 100, 0, TimeUnit.MILLISECONDS));
        Thread.sleep(150);
        // 没有租约，锁仍然被持有（外部没释放）
        assertTrue(lock.isLocked("k"));
        lock.unlock("k");
    }

    @Test
    void leaseExpiresForcesUnlock() throws InterruptedException {
        assertTrue(lock.tryLock("k", 100, 100, TimeUnit.MILLISECONDS));
        assertTrue(lock.isLocked("k"));
        // 等待租约到期（清理线程每秒跑一次，可能有最多 1s 延迟）
        long deadline = System.currentTimeMillis() + 3000;
        while (lock.isLocked("k") && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertFalse(lock.isLocked("k"), "租约到期后锁应被强制释放");
    }

    @Test
    void leaseExtensionByRelock() throws InterruptedException {
        // 短租约 100ms
        assertTrue(lock.tryLock("k", 100, 100, TimeUnit.MILLISECONDS));
        Thread.sleep(50);
        // 重入会刷新租约
        assertTrue(lock.tryLock("k", 0, 500, TimeUnit.MILLISECONDS));
        Thread.sleep(200);  // 超过原租约 100ms
        assertTrue(lock.isLocked("k"), "重入应刷新租约");
        lock.unlock("k");
        lock.unlock("k");
    }

    @Test
    void manualUnlockBeforeLeaseWorks() throws InterruptedException {
        assertTrue(lock.tryLock("k", 100, 10_000, TimeUnit.MILLISECONDS));
        lock.unlock("k");
        assertFalse(lock.isLocked("k"));
    }
}
