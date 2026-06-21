package cn.wubo.lock;

/**
 * 锁获取失败的原因。由 {@link LockListener#onLockFailed(String, long, LockFailureReason)}
 * 传递给监听器,替代原先的字符串 {@code reason},提供编译期类型安全。
 *
 * @since 2.1.0
 */
public enum LockFailureReason {

    /** 等待超时仍未获取到锁。对应 {@code tryLock(waitTime, unit)} 返回 {@code false}。 */
    TIMEOUT("timeout"),

    /** 超过 {@link CHMRLockConfig#maxKeys()} 限制,新 key 被拒绝。 */
    MAX_KEYS("maxKeys"),

    /** 等待过程中线程被中断。 */
    INTERRUPTED("interrupted");

    private final String code;

    LockFailureReason(String code) {
        this.code = code;
    }

    /**
     * 兼容原先字符串 reason 的简短标识(如 {@code "timeout"}、{@code "maxKeys"}、
     * {@code "interrupted"})。便于日志输出和迁移过渡。
     */
    public String code() {
        return code;
    }

    @Override
    public String toString() {
        return code;
    }
}
