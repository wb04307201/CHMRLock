package cn.wubo.lock;

/**
 * 当对不存在的 key 调用 unlock 时抛出。
 * 继承 IllegalMonitorStateException 以保持与 ReentrantLock 一致的异常层级。
 *
 * @since 1.1.0
 */
public final class LockNotFoundException extends IllegalMonitorStateException {

    public LockNotFoundException(String key) {
        super("锁不存在或已被清理: " + key);
    }
}
