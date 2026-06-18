package cn.wubo.lock;

import java.util.Map;

/**
 * 默认的 JSON 指标导出器,使用 {@link Appendable} 作为输出目的地。
 * 零三方依赖 — 不使用 Jackson/Gson,手写序列化以保持库的零依赖特性。
 *
 * @see MetricsExporter
 * @since 1.2.0
 */
public class JsonMetricsExporter implements MetricsExporter {

    private final Appendable out;

    /** 默认输出到 {@link System#out}。 */
    public JsonMetricsExporter() {
        this(System.out);
    }

    /** 指定输出目的地,通常为 {@link StringBuilder}(测试)或 {@link java.io.PrintWriter}(文件)。 */
    public JsonMetricsExporter(Appendable out) {
        this.out = out;
    }

    @Override
    public void export(MonitorMetrics global, Map<String, KeyStatistics> perKey) {
        try {
            out.append("{\n");
            appendGlobal(out, global);
            out.append(",\n");
            appendPerKey(out, perKey != null ? perKey : Map.of());
            out.append("\n}");
        } catch (java.io.IOException e) {
            throw new RuntimeException("JsonMetricsExporter failed", e);
        }
    }

    private void appendGlobal(Appendable o, MonitorMetrics m) throws java.io.IOException {
        o.append("  \"global\": {\n");
        appendField(o, "totalLocks", m.getTotalLocks(), true);
        appendField(o, "successLocks", m.getSuccessLocks(), false);
        appendField(o, "failedLocks", m.getFailedLocks(), false);
        appendField(o, "totalWaitTime", m.getTotalWaitTime(), false);
        appendField(o, "successRate", m.getSuccessRate(), false);
        appendField(o, "avgWaitTime", m.getAvgWaitTime(), false);
        o.append("  }");
    }

    private void appendPerKey(Appendable o, Map<String, KeyStatistics> perKey) throws java.io.IOException {
        o.append("  \"perKey\": {\n");
        boolean first = true;
        for (Map.Entry<String, KeyStatistics> e : perKey.entrySet()) {
            if (e.getValue() == null) continue;  // skip nulls
            if (!first) o.append(",\n");
            first = false;
            o.append("    \"").append(escape(e.getKey())).append("\": {\n");
            KeyStatistics ks = e.getValue();
            appendField(o, "acquireCount", ks.acquireCount(), true);
            appendField(o, "successCount", ks.successCount(), false);
            appendField(o, "failedCount", ks.failedCount(), false);
            appendField(o, "totalWaitNanos", ks.totalWaitNanos(), false);
            appendField(o, "lastAcquireEpochMs", ks.lastAcquireEpochMs(), false);
            appendField(o, "lastReleaseEpochMs", ks.lastReleaseEpochMs(), false);
            appendField(o, "currentHoldCount", ks.currentHoldCount(), false);
            appendField(o, "currentHolderThreadId", ks.currentHolderThreadId(), false);
            appendField(o, "successRate", ks.getSuccessRate(), false);
            appendField(o, "avgWaitTimeMillis", ks.getAvgWaitTimeMillis(), false);
            o.append("    }");
        }
        if (!first) o.append("\n");
        o.append("  }");
    }

    private void appendField(Appendable o, String name, long value, boolean first) throws java.io.IOException {
        if (!first) o.append(",\n");
        o.append("    \"").append(name).append("\":").append(String.valueOf(value));
    }

    private void appendField(Appendable o, String name, double value, boolean first) throws java.io.IOException {
        if (!first) o.append(",\n");
        o.append("    \"").append(name).append("\":").append(String.valueOf(value));
    }

    private void appendField(Appendable o, String name, int value, boolean first) throws java.io.IOException {
        if (!first) o.append(",\n");
        o.append("    \"").append(name).append("\":").append(String.valueOf(value));
    }

    private String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '/':  sb.append("\\/"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        // RFC 8259 §7: control chars U+0000..U+001F must be escaped
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * 辅助方法,用于测试。返回序列化后的 JSON 字符串。
     * 行为等价于 {@code new JsonMetricsExporter(sb).export(global, perKey)} 然后
     * {@code sb.toString()}。仅作测试便利,不建议在生产代码中使用(生产请直接
     * 通过 {@link #export(MonitorMetrics, Map)} 写入 Appendable)。
     */
    public String exportToString(MonitorMetrics global, Map<String, KeyStatistics> perKey) {
        StringBuilder sb = new StringBuilder();
        new JsonMetricsExporter(sb).export(global, perKey);
        return sb.toString();
    }
}
