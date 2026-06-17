package cn.wubo.lock;

/**
 * 按 key 抽象的读写锁接口。底层由 {@link java.util.concurrent.locks.StampedLock} 实现。
 *
 * <p><b>非可重入:</b>StampedLock 不支持重入 — 同一线程连续调用 {@link #readLock} 或
 * {@link #writeLock} 会死锁。</p>
 *
 * <p>乐观读模式:先调用 {@link #tryOptimisticRead} 拿到戳记,读取共享状态后调用
 * {@link #validate(long)} 验证是否被写过。验证失败则需要重读(可能需要升级为悲观读)。</p>
 *
 * @see StampedKeyedReadWriteLock
 * @see CHMRLock#readWriteLock(String)
 * @since 2.0.0
 */
public interface KeyedReadWriteLock {

    /**
     * 获取读锁(共享),等待直到可用。
     * @return 持锁令牌(非 0),调用 {@link #unlockRead(long)} 释放
     * @throws java.lang.InterruptedException 等待中被中断
     */
    long readLock() throws InterruptedException;

    /**
     * 尝试获取读锁,立即返回。
     * @return 0 表示失败;非 0 表示持锁令牌
     */
    long tryReadLock();

    /**
     * 获取写锁(独占),等待直到可用。
     * @return 持锁令牌(非 0),调用 {@link #unlockWrite(long)} 释放
     * @throws java.lang.InterruptedException 等待中被中断
     */
    long writeLock() throws InterruptedException;

    /**
     * 尝试获取写锁,立即返回。
     * @return 0 表示失败;非 0 表示持锁令牌
     */
    long tryWriteLock();

    /**
     * 乐观读(无锁读取)。读取后必须用 {@link #validate(long)} 验证。
     * @return 乐观读戳记(非 0)
     */
    long tryOptimisticRead();

    /**
     * 验证乐观读戳记是否仍然有效(自戳记以来无写操作)。
     * @param stamp {@link #tryOptimisticRead} 返回的戳记
     * @return true 表示有效
     */
    boolean validate(long stamp);

    /** 释放读锁。{@code stamp} 必须来自 {@link #readLock()} 或 {@link #tryReadLock()}。 */
    void unlockRead(long stamp);

    /** 释放写锁。{@code stamp} 必须来自 {@link #writeLock()} 或 {@link #tryWriteLock()}。 */
    void unlockWrite(long stamp);
}
