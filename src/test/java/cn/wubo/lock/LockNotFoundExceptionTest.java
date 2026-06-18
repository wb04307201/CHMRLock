package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LockNotFoundExceptionTest {

    @Test
    void extendsIllegalMonitorStateException() {
        LockNotFoundException ex = new LockNotFoundException("k1");
        assertTrue(ex instanceof IllegalMonitorStateException,
                "LockNotFoundException 必须继承 IllegalMonitorStateException 以保持向后兼容");
    }

    @Test
    void messageContainsKey() {
        LockNotFoundException ex = new LockNotFoundException("my_lock");
        assertTrue(ex.getMessage().contains("my_lock"),
                "异常信息应包含 key 便于诊断");
    }
}
