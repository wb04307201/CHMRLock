package cn.wubo.lock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class LockEntry {
    final ReentrantLock lock;
    private final AtomicLong lastAcquireTime = new AtomicLong(0);
    private final AtomicLong ownerThreadId = new AtomicLong(-1);

    LockEntry() {
        this(false);
    }

    LockEntry(boolean fair) {
        this.lock = new ReentrantLock(fair);
    }

    long getLastAcquireTime() {
        return lastAcquireTime.get();
    }

    void touchLastAcquireTime() {
        lastAcquireTime.set(System.currentTimeMillis());
    }

    Long getOwnerThreadId() {
        long id = ownerThreadId.get();
        return id < 0 ? null : id;
    }

    void setOwnerThreadId(long id) {
        ownerThreadId.set(id);
    }

    void clearOwner() {
        ownerThreadId.set(-1);
    }
}
