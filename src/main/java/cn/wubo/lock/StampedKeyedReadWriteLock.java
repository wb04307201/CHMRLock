package cn.wubo.lock;

import java.util.concurrent.locks.StampedLock;

/**
 * {@link KeyedReadWriteLock} 的 {@link StampedLock} 实现。
 *
 * <p>非可重入(StampedLock 限制)。写锁释放戳记通过 {@link StampedLock#isWriteLockStamp}
 * 在释放时区分,以便调用方使用对应的 {@code unlockRead}/{@code unlockWrite} 方法。</p>
 *
 * @see KeyedReadWriteLock
 * @since 2.0.0
 */
final class StampedKeyedReadWriteLock implements KeyedReadWriteLock {

    private final StampedLock stamped = new StampedLock();

    @Override
    public long readLock() throws InterruptedException {
        return stamped.readLockInterruptibly();
    }

    @Override
    public long tryReadLock() {
        return stamped.tryReadLock();
    }

    @Override
    public long writeLock() throws InterruptedException {
        return stamped.writeLockInterruptibly();
    }

    @Override
    public long tryWriteLock() {
        return stamped.tryWriteLock();
    }

    @Override
    public long tryOptimisticRead() {
        return stamped.tryOptimisticRead();
    }

    @Override
    public boolean validate(long stamp) {
        return stamped.validate(stamp);
    }

    @Override
    public void unlockRead(long stamp) {
        // 底层 StampedLock: 若 stamp 不是读锁戳记,抛 IllegalMonitorStateException
        // (实际类型为 StampedLock.WrongThreadStampException,继承 IllegalMonitorStateException)。
        stamped.unlockRead(stamp);
    }

    @Override
    public void unlockWrite(long stamp) {
        // 底层 StampedLock: 若 stamp 不是写锁戳记,抛 IllegalMonitorStateException
        // (实际类型为 StampedLock.WrongThreadStampException,继承 IllegalMonitorStateException)。
        stamped.unlockWrite(stamp);
    }
}
