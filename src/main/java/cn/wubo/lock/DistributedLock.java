package cn.wubo.lock;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁 SPI 接口。允许用户将自己的分布式锁实现(Redisson / ZooKeeper / etcd 等)
 * 接入 CHMRLock,获得与本地锁一致的 API 体验。
 *
 * <p>所有方法与 {@link CHMRLock} 的方法语义保持一致:</p>
 * <ul>
 *   <li>{@link #tryLock(String, long, TimeUnit)} — 非阻塞获取,返回是否成功</li>
 *   <li>{@link #tryAcquire(String, long, TimeUnit)} — 获取并返回包装</li>
 *   <li>{@link #unlock(String)} — 释放锁</li>
 *   <li>{@link #isLocked(String)} — 查询是否锁定</li>
 *   <li>{@link #forceUnlock(String)} — 跨线程强制释放(适用于租约已过期等异常场景)</li>
 * </ul>
 *
 * <p>实现者需要保证:</p>
 * <ul>
 *   <li>线程安全:多线程并发调用任一方法都必须安全</li>
 *   <li>超时精确:waitTime 到期时应当放弃,即使网络延迟也应快速返回</li>
 *   <li>租约语义(可选):如果实现支持租约,应通过 leaseTime 参数传递</li>
 *   <li>异常隔离:底层分布式协调服务的异常应包装为 {@link RuntimeException} 抛出</li>
 * </ul>
 *
 * <p>典型用法(以 Redisson 为例):</p>
 * <pre>{@code
 * DistributedLock distLock = new RedissonAdapter(redissonClient);
 * try (CHMRLock lock = new CHMRLock()) {
 *     lock.registerDistributedLock(distLock);
 *     if (lock.tryLock("resource", 5, TimeUnit.SECONDS)) {
 *         try {
 *             doWork();
 *         } finally {
 *             lock.unlock("resource");
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see CHMRLock#registerDistributedLock(String, DistributedLock)
 * @see CHMRLock#getDistributedLock(String)
 * @since 2.0.0
 */
public interface DistributedLock {

    /**
     * 尝试获取分布式锁。
     * @param key 锁标识(在分布式系统中应作为资源 ID)
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return 是否成功获取
     */
    boolean tryLock(String key, long waitTime, TimeUnit timeUnit);

    /**
     * 尝试获取分布式锁,带租约。
     * @param key 锁标识
     * @param waitTime 等待时间
     * @param leaseTime 租约时间(0 表示无租约,实现可忽略)
     * @param timeUnit 时间单位
     * @return 是否成功获取
     */
    boolean tryLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit);

    /**
     * 尝试获取分布式锁并返回包装(支持 try-with-resources)。
     * @param key 锁标识
     * @param waitTime 等待时间
     * @param timeUnit 时间单位
     * @return 包装对象,获取失败时为 {@link Optional#empty()}
     */
    Optional<DistributedAcquiredLock> tryAcquire(String key, long waitTime, TimeUnit timeUnit);

    /**
     * 释放分布式锁。调用方应保证锁由当前线程(或当前上下文)持有。
     * @param key 锁标识
     * @throws LockNotFoundException 锁不存在或已过期
     */
    void unlock(String key);

    /**
     * 查询分布式锁是否仍处于锁定状态。
     * @param key 锁标识
     * @return 是否锁定
     */
    boolean isLocked(String key);

    /**
     * 强制释放分布式锁(跨线程/跨上下文)。仅在租约已过期或分布式锁状态异常时使用。
     * @param key 锁标识
     * @return 是否成功释放
     */
    boolean forceUnlock(String key);
}
