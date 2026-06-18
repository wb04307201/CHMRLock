package cn.wubo.lock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MonitorMetricsTest {

    @Test
    void avgWaitTimeReturnsDouble() {
        MonitorMetrics m = new MonitorMetrics(4, 4, 0, 3);  // 3ms / 4 次 = 0.75ms
        double avg = m.getAvgWaitTime();
        assertEquals(0.75, avg, 0.001, "平均等待时间应返回 double 保留小数");
    }

    @Test
    void avgWaitTimeZeroWhenNoRequests() {
        MonitorMetrics m = new MonitorMetrics(0, 0, 0, 0);
        assertEquals(0.0, m.getAvgWaitTime());
    }

    @Test
    void avgWaitTimeLegacyMillisReturnsDouble() {
        MonitorMetrics m = new MonitorMetrics(2, 1, 1, 1);
        assertEquals(0.5, m.getAvgWaitTime(), 0.001);
    }
}
