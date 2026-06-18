package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockInterruptTest {

    @Test
    void interruptedTryLockReturnsFalseAndPreservesInterrupt() throws InterruptedException {
        CHMRLock lock = new CHMRLock();
        try {
            assertTrue(lock.tryLock("k"));
            Thread.currentThread().interrupt();
            boolean got = lock.tryLock("k", 100, TimeUnit.MILLISECONDS);
            assertFalse(got);
            assertTrue(Thread.currentThread().isInterrupted(),
                    "中断状态应被保留");
            Thread.interrupted();  // 清除标志
        } finally {
            lock.unlock("k");
            lock.shutdown();
        }
    }
}
