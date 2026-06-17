package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LockEntryTest {

    @Test
    void ownerIsNullInitially() {
        LockEntry e = new LockEntry();
        assertNull(e.getOwnerThreadId());
    }

    @Test
    void setOwnerAndClear() {
        LockEntry e = new LockEntry();
        e.setOwnerThreadId(42L);
        assertEquals(42L, e.getOwnerThreadId());
        e.clearOwner();
        assertNull(e.getOwnerThreadId());
    }

    @Test
    void lastAcquireTimeUpdated() {
        LockEntry e = new LockEntry();
        long before = System.currentTimeMillis();
        e.touchLastAcquireTime();
        long after = System.currentTimeMillis();
        long t = e.getLastAcquireTime();
        assertTrue(t >= before && t <= after);
    }
}
