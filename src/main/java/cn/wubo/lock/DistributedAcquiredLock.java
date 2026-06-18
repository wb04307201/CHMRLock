package cn.wubo.lock;

/**
 * {@link DistributedLock#tryAcquire} 返回的包装对象,支持 try-with-resources。
 *
 * <p>关闭时自动调用 {@link DistributedLock#unlock(String)}。
 * 释放过程中抛出的异常会被静默忽略(best-effort 释放)。</p>
 *
 * <p>典型用法:</p>
 * <pre>{@code
 * Optional<DistributedAcquiredLock> result = distLock.tryAcquire("resource", 5, TimeUnit.SECONDS);
 * if (result.isPresent()) {
 *     try (DistributedAcquiredLock al = result.get()) {
 *         doWork();
 *     } // 自动调用 distLock.unlock("resource")
 * }
 * }</pre>
 *
 * @since 2.0.0
 */
public class DistributedAcquiredLock implements AutoCloseable {
    private final DistributedLock lock;
    private final String key;
    private volatile boolean valid;

    /**
     * 构造锁包装实例。
     * @param lock 分布式锁实现
     * @param key 锁标识
     */
    public DistributedAcquiredLock(DistributedLock lock, String key) {
        this.lock = lock;
        this.key = key;
        this.valid = true;
    }

    /**
     * @return 该锁包装对应的 key
     */
    public String key() { return key; }

    /**
     * @return 包装是否仍有效(即 {@link #close()} 尚未被调用);
     *         注意:仅表示"close 次数",不代表底层分布式锁是否仍被持有
     */
    public boolean isValid() { return valid; }

    /**
     * 关闭包装,释放底层分布式锁。幂等:多次调用仅首次生效。
     * 释放过程中抛出的异常会被静默忽略(已通过 listener 等机制记录)。
     */
    @Override
    public void close() {
        if (valid) {
            valid = false;
            try {
                lock.unlock(key);
            } catch (Exception ignored) {
                // 释放失败不传播(已通过 listener 等机制记录)
            }
        }
    }
}
