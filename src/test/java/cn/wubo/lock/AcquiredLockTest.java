package cn.wubo.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class AcquiredLockTest {

    private CHMRLock lock;

    @BeforeEach
    void setUp() { lock = new CHMRLock(); }

    @AfterEach
    void tearDown() { lock.shutdown(); }

    @Test
    void tryAcquireSuccessReturnsPresent() {
        Optional<AcquiredLock> opt = lock.tryAcquire("k");
        assertTrue(opt.isPresent());
        assertEquals("k", opt.get().key());
        assertTrue(lock.isLocked("k"));
    }

    @Test
    void tryAcquireFailureReturnsEmpty() {
        assertTrue(lock.tryLock("k"));
        try {
            Optional<AcquiredLock> opt = lock.tryAcquire("k", 0, TimeUnit.MILLISECONDS);
            assertFalse(opt.isPresent());
        } finally {
            lock.unlock("k");
        }
    }

    @Test
    void tryWithResourcesAutoUnlocks() {
        String key = "auto";
        try (AcquiredLock al = lock.tryAcquire(key).orElseThrow()) {
            assertTrue(lock.isLocked(key));
        }
        assertFalse(lock.isLocked(key), "try-with-resources 关闭后应自动释放");
    }

    @Test
    void closeIsIdempotent() {
        AcquiredLock al = lock.tryAcquire("k").orElseThrow();
        al.close();
        assertDoesNotThrow(al::close);
    }
}
