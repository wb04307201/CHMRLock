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

    /** @return 该锁包装对应的 key */
    public String key() { return key; }

    /**
     * @return 包装是否仍有效(即 {@link #close()} 尚未被调用);
     *         注意:仅表示"close 次数",不代表底层锁是否仍被持有
     */
    public boolean isValid() { return !closed.get(); }

    /**
     * 释放底层锁,幂等:多次调用仅首次生效。释放过程中抛出的
     * {@link LockNotFoundException}(已被清理)与 {@link IllegalMonitorStateException}
     * (跨线程已释放)会被静默忽略。
     */
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
