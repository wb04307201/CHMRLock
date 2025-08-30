package cn.wubo.lock;

import java.util.concurrent.locks.ReentrantLock;

public class LockEntry {
    final ReentrantLock lock = new ReentrantLock();
    volatile long lastAcquireTime;

    public long getLastAcquireTime() {
        return lastAcquireTime;
    }


}
