package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 {@link DistributedLock} SPI 的契约(InMemoryDistributedLock 测试)。
 */
class DistributedLockTest {

    /** 测试用内存实现,仅供本测试使用。 */
    static class InMemoryDistributedLock implements DistributedLock {
        private final java.util.Map<String, Holder> map = new java.util.concurrent.ConcurrentHashMap<>();
        private final ReentrantLock guard = new ReentrantLock();

        private static class Holder {
            Thread owner;
            long expiresAt;
        }

        @Override
        public boolean tryLock(String key, long waitTime, TimeUnit timeUnit) {
            long deadlineNanos = System.nanoTime() + timeUnit.toNanos(waitTime);
            while (System.nanoTime() < deadlineNanos) {
                guard.lock();
                try {
                    Holder h = map.get(key);
                    long now = System.currentTimeMillis();
                    if (h == null || h.expiresAt < now) {
                        map.put(key, new Holder());
                        map.get(key).owner = Thread.currentThread();
                        map.get(key).expiresAt = Long.MAX_VALUE;
                        return true;
                    }
                } finally {
                    guard.unlock();
                }
                try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
            }
            return false;
        }

        @Override
        public boolean tryLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
            long deadlineNanos = System.nanoTime() + timeUnit.toNanos(waitTime);
            while (System.nanoTime() < deadlineNanos) {
                guard.lock();
                try {
                    Holder h = map.get(key);
                    long now = System.currentTimeMillis();
                    if (h == null || h.expiresAt < now) {
                        Holder nh = new Holder();
                        nh.owner = Thread.currentThread();
                        nh.expiresAt = leaseTime > 0 ? now + timeUnit.toMillis(leaseTime) : Long.MAX_VALUE;
                        map.put(key, nh);
                        return true;
                    }
                } finally {
                    guard.unlock();
                }
                try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
            }
            return false;
        }

        @Override
        public Optional<DistributedAcquiredLock> tryAcquire(String key, long waitTime, TimeUnit timeUnit) {
            return tryLock(key, waitTime, timeUnit)
                    ? Optional.of(new DistributedAcquiredLock(this, key))
                    : Optional.empty();
        }

        @Override
        public void unlock(String key) {
            Holder h = map.get(key);
            if (h == null) throw new LockNotFoundException("key not found: " + key);
            map.remove(key);
        }

        @Override
        public boolean isLocked(String key) {
            Holder h = map.get(key);
            if (h == null) return false;
            return h.expiresAt >= System.currentTimeMillis();
        }

        @Override
        public boolean forceUnlock(String key) {
            return map.remove(key) != null;
        }
    }

    @Test
    void tryLockSucceeds() {
        DistributedLock dl = new InMemoryDistributedLock();
        assertTrue(dl.tryLock("k1", 100, TimeUnit.MILLISECONDS));
        assertTrue(dl.isLocked("k1"));
        dl.unlock("k1");
        assertFalse(dl.isLocked("k1"));
    }

    @Test
    void tryLockTimesOut() throws Exception {
        DistributedLock dl = new InMemoryDistributedLock();
        assertTrue(dl.tryLock("k1", 100, TimeUnit.MILLISECONDS));
        assertFalse(dl.tryLock("k1", 50, TimeUnit.MILLISECONDS));
        dl.unlock("k1");
    }

    @Test
    void tryLockWithLeaseExpires() throws Exception {
        DistributedLock dl = new InMemoryDistributedLock();
        assertTrue(dl.tryLock("k1", 100, 50, TimeUnit.MILLISECONDS));
        Thread.sleep(100);
        assertFalse(dl.isLocked("k1"));
    }

    @Test
    void tryAcquireReturnsWrapper() {
        DistributedLock dl = new InMemoryDistributedLock();
        Optional<DistributedAcquiredLock> result = dl.tryAcquire("k1", 100, TimeUnit.MILLISECONDS);
        assertTrue(result.isPresent());
        try (DistributedAcquiredLock al = result.get()) {
            assertEquals("k1", al.key());
            assertTrue(al.isValid());
        }
        assertFalse(dl.isLocked("k1"));
    }

    @Test
    void unlockUnknownKeyThrows() {
        DistributedLock dl = new InMemoryDistributedLock();
        assertThrows(LockNotFoundException.class, () -> dl.unlock("missing"));
    }

    @Test
    void forceUnlockWorks() {
        DistributedLock dl = new InMemoryDistributedLock();
        assertTrue(dl.tryLock("k1", 100, TimeUnit.MILLISECONDS));
        assertTrue(dl.forceUnlock("k1"));
        assertFalse(dl.isLocked("k1"));
    }
}
