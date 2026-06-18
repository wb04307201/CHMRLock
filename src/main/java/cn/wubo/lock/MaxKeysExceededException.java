package cn.wubo.lock;

/**
 * 当 {@link CHMRLockConfig#maxKeys()} 限制被触发,且调用方使用的是阻塞获取
 * ({@link CHMRLock#lock(String)} / {@link CHMRLock#lock(String, long, TimeUnit)}
 * / {@link CHMRLock#lockInterruptibly(String, long, TimeUnit)}) 时抛出。
 *
 * <p>继承自 {@link IllegalStateException} —— <b>向后兼容</b>,已有的 catch
 * {@code IllegalStateException} 代码仍可捕获本异常。但业务方若想精确区分,
 * 可单独 catch {@code MaxKeysExceededException}。</p>
 *
 * <p>R6 修复(A-5):之前直接抛 {@link IllegalStateException},业务方难以精确处理。
 * 现在提供更具语义的异常类型,且不破坏现有 API。</p>
 *
 * @since 2.0.0
 */
public class MaxKeysExceededException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message 异常描述(通常包含当前 maxKeys 值)
     */
    public MaxKeysExceededException(String message) {
        super(message);
    }

    /**
     * @param message 异常描述
     * @param cause   触发此异常的原始异常(可选)
     */
    public MaxKeysExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}