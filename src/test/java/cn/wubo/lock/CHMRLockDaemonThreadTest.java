package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class CHMRLockDaemonThreadTest {

    @Test
    void cleanupThreadIsDaemon() throws InterruptedException {
        CHMRLock lock = new CHMRLock();
        try {
            Thread t = findCleanupThread();
            assertNotNull(t, "应能找到清理线程");
            assertTrue(t.isDaemon(), "清理线程必须是守护线程，避免阻止 JVM 退出");
        } finally {
            lock.shutdown();
        }
    }

    @Test
    void cleanupThreadHasMeaningfulName() throws InterruptedException {
        CHMRLock lock = new CHMRLock();
        try {
            Thread t = findCleanupThread();
            assertNotNull(t);
            assertTrue(t.getName().toLowerCase().contains("chmr"),
                    "清理线程名应包含 chmrlock 便于诊断: " + t.getName());
        } finally {
            lock.shutdown();
        }
    }

    private Thread findCleanupThread() throws InterruptedException {
        // 等一秒让清理线程启动并设置名字
        Thread.sleep(100);
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        int n = Thread.enumerate(threads);
        for (int i = 0; i < n; i++) {
            if (threads[i].getName().toLowerCase().contains("chmr")) {
                return threads[i];
            }
        }
        return null;
    }
}
