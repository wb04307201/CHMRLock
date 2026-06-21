package cn.wubo.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 延迟分桶直方图。7 桶，按纳秒边界划分：
 * <ol>
 *   <li>≤ 1µs</li>
 *   <li>≤ 10µs</li>
 *   <li>≤ 100µs</li>
 *   <li>≤ 1ms</li>
 *   <li>≤ 10ms</li>
 *   <li>≤ 100ms</li>
 *   <li>&gt; 100ms</li>
 * </ol>
 *
 * <p>使用 {@link LongAdder} 计数，高并发下竞争开销低。无外部依赖。
 *
 * @since 2.1.0
 */
public final class LatencyHistogram {

    private static final long[] UPPER_BOUNDS_NANOS = {
            TimeUnit.MICROSECONDS.toNanos(1),
            TimeUnit.MICROSECONDS.toNanos(10),
            TimeUnit.MICROSECONDS.toNanos(100),
            TimeUnit.MILLISECONDS.toNanos(1),
            TimeUnit.MILLISECONDS.toNanos(10),
            TimeUnit.MILLISECONDS.toNanos(100),
            Long.MAX_VALUE
    };

    private final LongAdder[] buckets;
    private final AtomicLong minNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxNanos = new AtomicLong(Long.MIN_VALUE);

    public LatencyHistogram() {
        this.buckets = new LongAdder[UPPER_BOUNDS_NANOS.length];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /**
     * 记录一次耗时观测（单位：纳秒）。
     */
    public void record(long elapsedNanos) {
        int idx = bucketIndex(elapsedNanos);
        buckets[idx].increment();
        // 维护 min（CAS 直到更小）
        long curMin;
        do {
            curMin = minNanos.get();
            if (elapsedNanos >= curMin) break;
        } while (!minNanos.compareAndSet(curMin, elapsedNanos));
        // 维护 max（CAS 直到更大）
        long curMax;
        do {
            curMax = maxNanos.get();
            if (elapsedNanos <= curMax) break;
        } while (!maxNanos.compareAndSet(curMax, elapsedNanos));
    }

    /**
     * 最小观测值（纳秒）。无观测时返回 Long.MAX_VALUE。
     */
    public long minNanos() {
        return minNanos.get();
    }

    /**
     * 最大观测值（纳秒）。无观测时返回 Long.MIN_VALUE。
     */
    public long maxNanos() {
        return maxNanos.get();
    }

    private static int bucketIndex(long nanos) {
        for (int i = 0; i < UPPER_BOUNDS_NANOS.length; i++) {
            if (nanos <= UPPER_BOUNDS_NANOS[i]) {
                return i;
            }
        }
        return UPPER_BOUNDS_NANOS.length - 1;
    }

    /**
     * 总观测次数。
     */
    public long totalCount() {
        long total = 0;
        for (LongAdder b : buckets) total += b.sum();
        return total;
    }

    public long countAtMostOneMicro()   { return buckets[0].sum(); }
    public long countAtMostTenMicro()   { return buckets[1].sum(); }
    public long countAtMostHundredMicro(){ return buckets[2].sum(); }
    public long countAtMostOneMilli()   { return buckets[3].sum(); }
    public long countAtMostTenMilli()   { return buckets[4].sum(); }
    public long countAtMostHundredMilli(){ return buckets[5].sum(); }
    public long countOverHundredMilli() { return buckets[6].sum(); }

    /**
     * 估算平均耗时（纳秒，使用各桶中点）。
     */
    public double averageNanos() {
        long total = totalCount();
        if (total == 0) return 0.0;
        // 中点近似：第一桶 500ns、最后一桶取 100ms
        long[] midpoints = {
                TimeUnit.MICROSECONDS.toNanos(1) / 2,
                (TimeUnit.MICROSECONDS.toNanos(1) + TimeUnit.MICROSECONDS.toNanos(10)) / 2,
                (TimeUnit.MICROSECONDS.toNanos(10) + TimeUnit.MICROSECONDS.toNanos(100)) / 2,
                (TimeUnit.MICROSECONDS.toNanos(100) + TimeUnit.MILLISECONDS.toNanos(1)) / 2,
                (TimeUnit.MILLISECONDS.toNanos(1) + TimeUnit.MILLISECONDS.toNanos(10)) / 2,
                (TimeUnit.MILLISECONDS.toNanos(10) + TimeUnit.MILLISECONDS.toNanos(100)) / 2,
                TimeUnit.MILLISECONDS.toNanos(100)
        };
        double weighted = 0;
        for (int i = 0; i < buckets.length; i++) {
            weighted += buckets[i].sum() * midpoints[i];
        }
        return weighted / total;
    }

    public void clear() {
        for (LongAdder b : buckets) b.reset();
        minNanos.set(Long.MAX_VALUE);
        maxNanos.set(Long.MIN_VALUE);
    }
}
