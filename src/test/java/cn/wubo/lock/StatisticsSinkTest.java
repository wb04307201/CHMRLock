package cn.wubo.lock;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StatisticsSinkTest {

    @Test
    void recordIsInvoked() {
        StatisticsSink sink = new StatisticsSink() {
            @Override
            public void record(MonitorMetrics global, Map<String, KeyStatistics> perKey) {
                // no-op
            }
        };
        MonitorMetrics global = new MonitorMetrics(10, 8, 2, 1000);
        sink.record(global, Map.of());
        // Just verify it doesn't throw
    }

    @Test
    void recordReceivesSnapshot() {
        AtomicInteger callCount = new AtomicInteger();
        List<Long> receivedTotals = new ArrayList<>();
        StatisticsSink sink = (global, perKey) -> {
            callCount.incrementAndGet();
            receivedTotals.add(global.getTotalLocks());
        };

        sink.record(new MonitorMetrics(5, 4, 1, 100), Map.of());
        sink.record(new MonitorMetrics(10, 8, 2, 200), Map.of());

        assertEquals(2, callCount.get());
        assertEquals(List.of(5L, 10L), receivedTotals);
    }

    @Test
    void recordWithPerKey() {
        List<Map<String, KeyStatistics>> received = new ArrayList<>();
        StatisticsSink sink = (global, perKey) -> received.add(perKey);

        KeyStatistics ks = new KeyStatistics("k1", 5, 4, 1, 0L, 0L, 0L, 1L, 1L);
        sink.record(new MonitorMetrics(5, 4, 1, 0), Map.of("k1", ks));

        assertEquals(1, received.size());
        assertEquals(1, received.get(0).size());
        assertEquals("k1", received.get(0).get("k1").key());
    }

    @Test
    void recordWithEmptyPerKey() {
        AtomicInteger callCount = new AtomicInteger();
        StatisticsSink sink = (global, perKey) -> {
            callCount.incrementAndGet();
            assertTrue(perKey.isEmpty());
        };
        sink.record(new MonitorMetrics(0, 0, 0, 0), Map.of());
        assertEquals(1, callCount.get());
    }

    @Test
    void multipleSinksCanBeUsedIndependently() {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        StatisticsSink sinkA = (g, p) -> a.incrementAndGet();
        StatisticsSink sinkB = (g, p) -> b.incrementAndGet();

        MonitorMetrics global = new MonitorMetrics(0, 0, 0, 0);
        sinkA.record(global, Map.of());
        sinkB.record(global, Map.of());
        sinkA.record(global, Map.of());

        assertEquals(2, a.get());
        assertEquals(1, b.get());
    }
}
