package cn.wubo.lock;

/**
 * 锁生命周期监听器。所有方法默认 no-op，实现类按需覆盖感兴趣的事件。
 *
 * <p>事件触发时机：</p>
 * <ul>
 *   <li>{@link #onLockAcquired} - tryLock 成功获取后</li>
 *   <li>{@link #onLockReleased} - unlock 成功后</li>
 *   <li>{@link #onLockFailed} - tryLock 失败（reason: timeout / maxKeys / interrupted）</li>
 *   <li>{@link #onLockExpired} - 租约到期强制释放后</li>
 *   <li>{@link #onLockContended} - tryLock(无等待) 发现 key 已被持有</li>
 * </ul>
 *
 * <p>监听器抛出的异常会被 CHMRLock 捕获并忽略，不会影响锁功能。</p>
 */
public interface LockListener {

    /** 锁被成功获取后触发。{@code waitNanos} 是实际等待时长(纳秒)。 */
    default void onLockAcquired(String key, long waitNanos) {}

    /** 锁被释放后触发。{@code heldMillis} 是持有时长(毫秒)。 */
    default void onLockReleased(String key, long heldMillis) {}

    /**
     * 获取锁失败时触发。{@code reason} 取值:
     * <ul>
     *   <li>{@code "timeout"} - 等待超时</li>
     *   <li>{@code "maxKeys"} - 超过 maxKeys 限制</li>
     *   <li>{@code "interrupted"} - 等待中被中断</li>
     * </ul>
     */
    default void onLockFailed(String key, long waitNanos, String reason) {}

    /** 租约到期强制释放后触发。 */
    default void onLockExpired(String key) {}

    /**
     * tryLock(无等待) 发现 key 已被持有时触发。
     * 注意:仅在非阻塞 tryLock 中触发，阻塞 tryLock 不会触发(无法在
     * 锁内便宜地检测"曾经被争用")。
     */
    default void onLockContended(String key) {}
}
