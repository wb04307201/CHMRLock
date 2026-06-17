package cn.wubo.lock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * try-with-resources 友好的锁包装。
 * 关闭后自动调用 manager.unlock(key)。
 */
public final class AcquiredLock implements AutoCloseable {
    private final CHMRLock manager;
    private final String key;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    AcquiredLock(CHMRLock manager, String key) {
        this.manager = manager;
        this.key = key;
    }

    public String key() { return key; }

    public boolean isValid() { return !closed.get(); }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                manager.unlock(key);
            } catch (LockNotFoundException ignored) {
                // 已被释放或清理
            } catch (IllegalMonitorStateException ignored) {
                // 跨线程已释放
            }
        }
    }
}
