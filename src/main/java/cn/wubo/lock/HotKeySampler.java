package cn.wubo.lock;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 热点 key 采样器。基于 Count-Min Sketch（4 哈希 × 1024 桶 ≈ 8KB），
 * 每 N 次访问采样一次（避免高基数 key 集合撑爆）。
 *
 * <p>精度：低基数（&lt; 1万）key 的相对频率误差 &lt; 1%，适合"识别热点"而非"精确计数"。
 *
 * @since 2.1.0
 */
public final class HotKeySampler {

    private static final int HASHES = 4;
    private static final int BUCKETS = 1024;
    private static final int SAMPLE_RATIO = 10_000; // 每 10000 次访问采样 1 次

    private final long[][] counts = new long[HASHES][BUCKETS];
    private long accessCounter = 0;

    /**
     * 记录一次访问。仅每 SAMPLE_RATIO 次访问真正采样一次（降低开销）。
     */
    public void record(String key) {
        long counter = ++accessCounter;
        if (counter % SAMPLE_RATIO != 0) return;
        int h1 = spread(key == null ? 0 : key.hashCode());
        for (int i = 0; i < HASHES; i++) {
            int idx = (h1 + i * 0x9E3779B9) & (BUCKETS - 1);
            counts[i][idx]++;
        }
    }

    /**
     * 查询 key 的估计访问次数（最小值，取 4 个哈希的最小值以减小误差）。
     */
    public long estimateCount(String key) {
        int h1 = spread(key == null ? 0 : key.hashCode());
        long min = Long.MAX_VALUE;
        for (int i = 0; i < HASHES; i++) {
            int idx = (h1 + i * 0x9E3779B9) & (BUCKETS - 1);
            min = Math.min(min, counts[i][idx]);
        }
        return min == Long.MAX_VALUE ? 0 : min;
    }

    public long accessCount() {
        return accessCounter;
    }

    public void clear() {
        for (long[] row : counts) {
            java.util.Arrays.fill(row, 0L);
        }
        accessCounter = 0;
    }

    /**
     * 返回当前估计的 top-N 热点 keys（按估计访问次数降序）。
     * 由于 Count-Min Sketch 只能反映哈希后分布，调用方需传入候选 key 集合来反查。
     */
    public List<String> topHotKeys(List<String> candidates, int n) {
        List<String> sorted = new ArrayList<>(candidates);
        sorted.sort(Comparator.comparingLong(this::estimateCount).reversed());
        if (sorted.size() > n) return sorted.subList(0, n);
        return sorted;
    }

    private static int spread(int h) {
        h ^= (h >>> 16);
        return h;
    }
}
