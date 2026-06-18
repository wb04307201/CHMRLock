package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

class CHMRLockDistributedTest {

    /** Test stub: simple in-memory distributed lock. */
    static class StubDistributedLock implements DistributedLock {
        final java.util.Map<String, Long> held = new java.util.concurrent.ConcurrentHashMap<>();
        final ReentrantLock guard = new ReentrantLock();

        @Override
        public boolean tryLock(String key, long waitTime, TimeUnit unit) {
            guard.lock();
            try {
                if (held.containsKey(key)) return false;
                held.put(key, System.currentTimeMillis());
                return true;
            } finally { guard.unlock(); }
        }

        @Override
        public boolean tryLock(String key, long waitTime, long leaseTime, TimeUnit unit) {
            return tryLock(key, waitTime, unit);
        }

        @Override
        public Optional<DistributedAcquiredLock> tryAcquire(String key, long waitTime, TimeUnit unit) {
            return tryLock(key, waitTime, unit)
                    ? Optional.of(new DistributedAcquiredLock(this, key))
                    : Optional.empty();
        }

        @Override
        public void unlock(String key) {
            if (held.remove(key) == null) throw new LockNotFoundException("not held: " + key);
        }

        @Override
        public boolean isLocked(String key) {
            return held.containsKey(key);
        }

        @Override
        public boolean forceUnlock(String key) {
            return held.remove(key) != null;
        }
    }

    @Test
    void registerAndGet() {
        try (CHMRLock lock = new CHMRLock()) {
            StubDistributedLock stub = new StubDistributedLock();
            lock.registerDistributedLock("tenantA", stub);
            Optional<DistributedLock> result = lock.getDistributedLock("tenantA");
            assertTrue(result.isPresent());
            assertSame(stub, result.get());
        }
    }

    @Test
    void getUnregisteredReturnsEmpty() {
        try (CHMRLock lock = new CHMRLock()) {
            assertFalse(lock.getDistributedLock("missing").isPresent());
        }
    }

    @Test
    void registerOverwritesPrevious() {
        try (CHMRLock lock = new CHMRLock()) {
            StubDistributedLock first = new StubDistributedLock();
            StubDistributedLock second = new StubDistributedLock();
            lock.registerDistributedLock("tenantA", first);
            lock.registerDistributedLock("tenantA", second);
            assertSame(second, lock.getDistributedLock("tenantA").get());
        }
    }

    @Test
    void unregisterRemoves() {
        try (CHMRLock lock = new CHMRLock()) {
            lock.registerDistributedLock("tenantA", new StubDistributedLock());
            assertTrue(lock.unregisterDistributedLock("tenantA"));
            assertFalse(lock.getDistributedLock("tenantA").isPresent());
        }
    }

    @Test
    void unregisterUnknownReturnsFalse() {
        try (CHMRLock lock = new CHMRLock()) {
            assertFalse(lock.unregisterDistributedLock("missing"));
        }
    }

    @Test
    void registerNullNameThrows() {
        try (CHMRLock lock = new CHMRLock()) {
            assertThrows(NullPointerException.class,
                    () -> lock.registerDistributedLock(null, new StubDistributedLock()));
        }
    }

    @Test
    void registerNullLockThrows() {
        try (CHMRLock lock = new CHMRLock()) {
            assertThrows(NullPointerException.class,
                    () -> lock.registerDistributedLock("name", null));
        }
    }

    @Test
    void shutdownClearsDistributedLocks() {
        CHMRLock lock = new CHMRLock();
        lock.registerDistributedLock("tenantA", new StubDistributedLock());
        lock.shutdown();
        assertFalse(lock.getDistributedLock("tenantA").isPresent());
    }

    @Test
    void distributedLockCanBeUsedDirectly() {
        try (CHMRLock lock = new CHMRLock()) {
            StubDistributedLock stub = new StubDistributedLock();
            lock.registerDistributedLock("tenantA", stub);
            DistributedLock dl = lock.getDistributedLock("tenantA").orElseThrow();
            assertTrue(dl.tryLock("resource1", 100, TimeUnit.MILLISECONDS));
            assertTrue(dl.isLocked("resource1"));
            dl.unlock("resource1");
            assertFalse(dl.isLocked("resource1"));
        }
    }
}