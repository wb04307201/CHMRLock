package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LockListenerTest {

    /** Simple recording listener. */
    static class RecordingListener implements LockListener {
        final List<String> events = new CopyOnWriteArrayList<>();
        @Override public void onLockAcquired(String key, long waitNanos) { events.add("acquired:" + key); }
        @Override public void onLockReleased(String key, long heldMillis) { events.add("released:" + key); }
        @Override public void onLockFailed(String key, long waitNanos, String reason) { events.add("failed:" + key + ":" + reason); }
        @Override public void onLockExpired(String key) { events.add("expired:" + key); }
        @Override public void onLockContended(String key) { events.add("contended:" + key); }
    }

    @Test
    void acquireAndReleaseFireEvents() {
        try (CHMRLock lock = new CHMRLock()) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            assertTrue(rec.events.contains("acquired:k1"));
            assertTrue(rec.events.contains("released:k1"));
        }
    }

    @Test
    void failedTryLockFiresFailedEvent() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);
            assertTrue(lock.tryLock("k1"));
            CountDownLatch contenderDone = new CountDownLatch(1);
            Thread contender = new Thread(() -> {
                assertFalse(lock.tryLock("k1", 50));
                contenderDone.countDown();
            });
            contender.start();
            assertTrue(contenderDone.await(2, TimeUnit.SECONDS));
            lock.unlock("k1");
            assertTrue(rec.events.stream().anyMatch(e -> e.equals("failed:k1:timeout")),
                    "expected failed:k1:timeout, got: " + rec.events);
        }
    }

    @Test
    void contendedNonBlockingFiresContendedEvent() throws Exception {
        try (CHMRLock lock = new CHMRLock()) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);
            assertTrue(lock.tryLock("k1"));
            // 用另一线程触发非阻塞 tryLock（主线程重入会成功；另一线程的 wait=0 才会触发 contended）
            AtomicReference<Boolean> result = new AtomicReference<>();
            Thread other = new Thread(() -> result.set(lock.tryLock("k1", 0, TimeUnit.MILLISECONDS)));
            other.start();
            other.join(2000);
            assertNotNull(result.get(), "other thread did not complete");
            assertFalse(result.get(), "another thread's no-wait tryLock should fail");
            assertTrue(rec.events.contains("contended:k1"),
                    "expected contended:k1, got: " + rec.events);
            lock.unlock("k1");
        }
    }

    @Test
    void leaseExpiryFiresExpiredEvent() throws Exception {
        CHMRLockConfig cfg = CHMRLockConfig.builder()
                .defaultWaitTime(Duration.ofMillis(100))
                .build();
        try (CHMRLock lock = new CHMRLock(cfg)) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);
            // Acquire with very short lease
            assertTrue(lock.tryLock("k1", 100, 50, TimeUnit.MILLISECONDS));
            // Wait for the lease to expire
            Thread.sleep(300);
            assertTrue(rec.events.contains("expired:k1"),
                    "expected expired:k1, got: " + rec.events);
        }
    }

    @Test
    void multipleListenersAllReceiveEvents() {
        try (CHMRLock lock = new CHMRLock()) {
            RecordingListener a = new RecordingListener();
            RecordingListener b = new RecordingListener();
            lock.registerListener(a);
            lock.registerListener(b);
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            assertTrue(a.events.contains("acquired:k1"));
            assertTrue(b.events.contains("acquired:k1"));
        }
    }

    @Test
    void unregisterRemovesListener() {
        try (CHMRLock lock = new CHMRLock()) {
            RecordingListener rec = new RecordingListener();
            lock.registerListener(rec);
            lock.unregisterListener(rec);
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            assertFalse(rec.events.contains("acquired:k1"));
        }
    }

    @Test
    void defaultMethodsAreNoOp() {
        // A no-op listener (relying on all 5 default methods) should not throw
        LockListener l = new LockListener() {};
        try (CHMRLock lock = new CHMRLock()) {
            lock.registerListener(l);
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            // No exception = pass
        }
    }

    @Test
    void listenerExceptionDoesNotBreakLocking() {
        try (CHMRLock lock = new CHMRLock()) {
            LockListener bad = new LockListener() {
                @Override public void onLockAcquired(String key, long waitNanos) {
                    throw new RuntimeException("listener boom");
                }
            };
            lock.registerListener(bad);
            // Lock acquisition should still succeed despite listener throwing
            assertTrue(lock.tryLock("k1"));
            lock.unlock("k1");
            // Pass = listener exception was swallowed
        }
    }
}
