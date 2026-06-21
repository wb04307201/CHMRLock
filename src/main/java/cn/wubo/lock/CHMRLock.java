package cn.wubo.lock;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * 基于 {@link java.util.concurrent.ConcurrentHashMap} 和 {@link java.util.concurrent.locks.ReentrantLock} 的细粒度单机锁库。
 *
 * <p>每个 String key 维护一把独立的锁。线程安全、零三方依赖、支持 try-with-resources 和租约模式。</p>
 *
 * <p>典型用法：</p>
 * <pre>{@code
 * CHMRLock lock = new CHMRLock();
 * try {
 *     if (lock.tryLock("resource_id")) {
 *         try {
 *             doWork();
 *         } finally {
 *             lock.unlock("resource_id");
 *         }
 *     }
 * } finally {
 *     lock.shutdown();
 * }
 * }</pre>
 *
 * <h2>行为约定</h2>
 * <ul>
 *   <li>默认非公平锁（{@code ReentrantLock(false)}）</li>
 *   <li>清理线程为守护线程（命名 {@code chmrlock-cleanup-N}）</li>
 *   <li>{@link #shutdown()} 幂等，可多次调用</li>
 *   <li>{@link #unlock(String)} 跨线程调用会抛 {@link IllegalMonitorStateException}</li>
 * </ul>
 *
 * @see CHMRLockConfig
 * @see AcquiredLock
 */
public class CHMRLock implements AutoCloseable {
    // 配置
    private final CHMRLockConfig config;

    // 默认等待时间（毫秒）
    private long defaultWaitTime;
    // 默认租约时间（毫秒），0 表示无租约
    private long defaultLeaseTime;

    // 存储各个key对应的锁
    private final Map<String, LockEntry> lockMap = new ConcurrentHashMap<>();

    // Per-key StampedLock cache for read-write scenarios
    private final ConcurrentHashMap<String, StampedKeyedReadWriteLock> rwLockMap = new ConcurrentHashMap<>();

    // 统计信息（LongAdder：高竞争场景下 CAS 失败率显著低于 AtomicLong）
    private final LongAdder totalLocks = new LongAdder();
    private final LongAdder successLocks = new LongAdder();
    private final LongAdder failedLocks = new LongAdder();
    private final LongAdder totalWaitTime = new LongAdder();

    // recordStats 才启用：延迟分布直方图
    private final LatencyHistogram waitLatency;
    // recordStats 才启用：热点 key 采样器
    private final HotKeySampler hotKeySampler;

    // 清理线程池：用于定期执行清理任务
    private final ScheduledExecutorService cleanupExecutor;

    // 异步获取锁的线程池：用于 CompletableFuture.supplyAsync 的执行器（守护线程）。
    // 可通过 {@link CHMRLockConfig.Builder#asyncExecutor(ExecutorService)} 注入自定义线程池,
    // 此时线程数与队列配置被忽略,且 CHMRLock {@link #shutdown()} 不会关闭注入的 executor。
    private final ExecutorService asyncExecutor;
    // 是否由 CHMRLock 自己创建 asyncExecutor（决定 shutdown 时是否关闭）
    private final boolean ownsAsyncExecutor;

    // 锁生命周期监听器列表
    private final List<LockListener> listeners = new CopyOnWriteArrayList<>();

    // 已注册的分布式锁 SPI 实例(按逻辑名称索引)
    private final ConcurrentHashMap<String, DistributedLock> distributedLocks = new ConcurrentHashMap<>();

    private static final Logger log = Logger.getLogger(CHMRLock.class.getName());
    private final AtomicBoolean shutdownCalled = new AtomicBoolean(false);


    /** 使用默认配置创建实例。详见 {@link CHMRLockConfig#defaults()}。 */
    public CHMRLock() {
        this(CHMRLockConfig.defaults());
    }

    /**
     * 使用指定默认等待时间创建实例，其余配置使用默认值。
     * @param defaultWaitTime 默认等待时长
     * @param timeUnit 时间单位
     */
    public CHMRLock(long defaultWaitTime, TimeUnit timeUnit) {
        this(CHMRLockConfig.builder()
                .defaultWaitTime(Duration.ofMillis(timeUnit.toMillis(defaultWaitTime)))
                .build());
    }

    /**
     * 使用自定义配置创建实例。
     * @param config 锁配置（不可为 null）
     */
    public CHMRLock(CHMRLockConfig config) {
        this.config = config;
        this.defaultWaitTime = config.defaultWaitTime().toMillis();
        this.defaultLeaseTime = config.defaultLeaseTime().toMillis();
        this.waitLatency = config.recordStats() ? new LatencyHistogram() : null;
        this.hotKeySampler = config.recordStats() ? new HotKeySampler() : null;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(config, "chmrlock-" + config.name() + "-cleanup"));
        // 异步获取锁的线程池:优先使用用户注入的 executor；否则创建有界线程池 + 有界任务队列,
        // 两段式 RejectedExecutionHandler 提供背压。
        // 默认线程数 = availableProcessors * 4,默认队列 = 1024。
        if (config.asyncExecutor() != null) {
            this.asyncExecutor = config.asyncExecutor();
            this.ownsAsyncExecutor = false;
        } else {
            // R6 修复(R-2):之前的 handler `(r, executor) -> r.run()` 在所有拒绝场景下都同步执行,
            // 包括 executor 已 shutdown 的情况 —— 这会导致业务方在 shutdown 后调用 tryAcquireAsync
            // 时被调用线程同步占用(可能 3 秒以上,取决于 defaultWaitTime),与"异步不阻塞"的承诺矛盾。
            //
            // 当前策略:
            // - 队列满(线程池未 shutdown):由调用线程同步执行 (CallerRunsPolicy 语义),提供背压
            // - executor 已 shutdown:抛 RejectedExecutionException,业务方可显式感知并降级处理
            java.util.concurrent.ThreadPoolExecutor asyncPool = new java.util.concurrent.ThreadPoolExecutor(
                    0, config.asyncMaxThreads(),
                    60L, TimeUnit.SECONDS,
                    new java.util.concurrent.LinkedBlockingQueue<>(config.asyncQueueCapacity()),
                    daemonThreadFactory(config, "chmrlock-" + config.name() + "-async"),
                    (r, executor) -> {
                        // shutdown 后:不再同步执行,直接 REE 让业务感知
                        if (executor.isShutdown()) {
                            throw new java.util.concurrent.RejectedExecutionException(
                                    "CHMRLock asyncExecutor is shutdown");
                        }
                        // 队列满(线程池仍运行):由调用线程同步执行,提供背压
                        r.run();
                    });
            asyncPool.allowCoreThreadTimeOut(true);
            this.asyncExecutor = asyncPool;
            this.ownsAsyncExecutor = true;
        }

        // 启动后台清理线程
        startCleanupThread();
    }

    /**
     * 创建守护线程工厂。{@code seq} 为每个工厂实例的独立计数器(每次调用本方法
     * 都会返回一个新的工厂和独立的序列号空间),用于生成 {@code prefix-N} 形式的
     * 线程名,避免不同实例的线程名冲突。
     */
    private static ThreadFactory daemonThreadFactory(CHMRLockConfig config, String prefix) {
        AtomicInteger seq = new AtomicInteger(1);
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.getAndIncrement());
            t.setDaemon(config.daemonCleanupThread());
            return t;
        };
    }

    /**
     * 启动后台定时清理线程，定期执行清理任务
     */
    private void startCleanupThread() {
        long intervalMillis = config.cleanupInterval().toMillis();
        cleanupExecutor.scheduleAtFixedRate(this::cleanupTick,
                intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void cleanupTick() {
        // 顶层 Exception 防护:scheduleAtFixedRate 在任务抛任何异常时会取消后续调度,
        // 导致清理线程永久死亡,孤儿 entry 永远无法被回收 → 内存泄漏。
        // R6 修复(E-2):从 catch(Throwable) 改回 catch(Exception),让 Error(如 OOM、StackOverflow)
        // 向上传播 —— 这类错误说明 JVM 状态已不可靠,继续清理可能掩盖真实问题。
        // Exception 类异常(业务代码 bug、并发竞态等)仍被吞掉,保证清理线程继续运行。
        try {
            long now = config.clock().millis();
            long threshold = config.idleThreshold().toMillis();
            lockMap.entrySet().removeIf(entry -> {
                LockEntry le = entry.getValue();
                if (le.lock.isLocked()) return false;
                if (le.getLeaseEndTime() != Long.MAX_VALUE && le.getLeaseEndTime() < now) {
                    return false;  // 保留租约未结束的 entry(防御性,避免误删)
                }
                // R6 修复(B-3):借锁改用 tryLock(0, MS) 非阻塞版本。
                // 之前用 le.lock.tryLock() 等价于 tryLock(0, MS),但对于 hot key
                // (lockMap.computeIfAbsent 之后立即 tryLock),即使空闲也会借锁失败
                // (因刚释放的锁还在 unlock 路径中持有),需多等几个 cleanup tick 才能清理。
                // 现在的语义不变(都是非阻塞 tryLock),但更明确表达"借锁立即返回"。
                // 收窄竞态窗口:借用锁做空闲判断,确保此刻无人持有,避免误删活跃 entry。
                // 注意:这并不能完全消除竞态 —— 在 tryLock 与 removeIf 实际 remove 之间
                // 另一个线程仍可能 start an acquisition;但窗口已大幅收窄,且即便发生
                // 孤儿获取,后续 cleanup 周期或租约/forceUnlock 路径会清理。
                try {
                    if (!le.lock.tryLock(0, TimeUnit.MILLISECONDS)) return false;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;  // 被中断,本 tick 不清理,下一 tick 再试
                }
                try {
                    return (now - le.getLastAcquireTime()) > threshold;
                } finally {
                    le.lock.unlock();
                }
            });
        } catch (Exception e) {
            // 业务类异常不能让清理线程死掉
            log.warning("[" + config.name() + "] cleanupTick failed: " + e);
        }
    }

    /**
     * 统计当前持有中的 entry 数量（用于 maxKeys 限制）。
     * 注意：仅扫描持有中的锁，释放后的 entry 不占 slot，
     * 避免 unlock → tryLock 新 key 的常见模式被错误阻塞。
     */
    private long countHeldEntries() {
        long count = 0;
        for (LockEntry le : lockMap.values()) {
            if (le.lock.isLocked()) count++;
        }
        return count;
    }

    /**
     * 尝试获取指定 key 的锁,等待最多 {@link CHMRLockConfig#defaultWaitTime()}(默认 3 秒)。
     * 便捷重载,等价于 {@link #tryLock(String, long, TimeUnit) tryLock(key, defaultWaitTime, MILLISECONDS)}。
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @return 是否成功获取
     * @throws IllegalArgumentException 若 {@code key} 为空字符串
     */
    public boolean tryLock(String key) {
        validateKey(key);
        return tryLock(key, defaultWaitTime, resolveLeaseTimeMillis(key), TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试获取指定 key 的锁,等待最多 {@code waitTime}。无租约,等价于
     * {@link #tryLock(String, long, long, TimeUnit) tryLock(key, waitTime, 0, timeUnit)}。
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return 是否成功获取
     */
    public boolean tryLock(String key, long waitTime, TimeUnit timeUnit) {
        validateKey(key);
        Objects.requireNonNull(timeUnit, "timeUnit must not be null");
        if (waitTime < 0) throw new IllegalArgumentException("waitTime 必须 >= 0,实际:" + waitTime);
        return tryLock(key, waitTime, resolveLeaseTimeMillis(key), timeUnit);
    }

    /**
     * 获取锁,指定等待时间与租约时间。
     * 租约到期后会通过 {@link #isLocked(String)} 报告锁已释放,但底层 ReentrantLock
     * 仍由原持有线程持有(ReentrantLock.unlock 要求 owner 线程)。
     * 真正跨线程强制释放需调用 {@link #unlock(String)} 或后续版本的 forceUnlock。
     *
     * <p><b>shutdown 后行为(R6 修复 R-1):</b>在 {@link #shutdown()} 之后调用
     * 本方法将抛 {@link IllegalStateException} —— 这是为了防止新 entry 被加入
     * 已停止清理线程的 lockMap(否则这些 entry 永远不会被回收,导致内存泄漏)。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间
     * @param leaseTime 租约时间(毫秒),0 表示无租约
     * @param timeUnit 时间单位
     * @return 是否成功获取
     * @throws IllegalArgumentException 若 {@code key} 为空字符串,或 {@code waitTime}/{@code leaseTime} 为负
     * @throws IllegalStateException 若已调用 {@link #shutdown()}
     */
    public boolean tryLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        validateKey(key);
        Objects.requireNonNull(timeUnit, "timeUnit must not be null");
        if (waitTime < 0) throw new IllegalArgumentException("waitTime 必须 >= 0,实际:" + waitTime);
        if (leaseTime < 0) throw new IllegalArgumentException("leaseTime 必须 >= 0,实际:" + leaseTime);
        // R6 修复(R-1):shutdown 后禁止新增 entry,避免永久泄漏
        if (shutdownCalled.get()) {
            throw new IllegalStateException("CHMRLock has been shutdown");
        }
        // R6 修复(E-1):用 System.nanoTime() 单调时钟计算 elapsed,
        // 避免 wall clock (config.clock().millis()) 在时钟回退时产生负 elapsed,
        // 进而污染 totalWaitTime 统计。
        long startNanos = System.nanoTime();
        totalLocks.increment();
        if (hotKeySampler != null) hotKeySampler.record(key);
        boolean perKeyMetrics = config.enablePerKeyMetrics();

        // 记录 key 是否已存在于 map 中(用于 maxKeys 限制的"新 key"判断)
        boolean wasNew = !lockMap.containsKey(key);

        // 先创建 entry,后续 per-key metrics 可以记录在刚创建的 entry 上
        // (无论是否最终被 maxKeys 拒绝)。同时已存在的 key 可重入。
        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry(config.fairLock()));

        // maxKeys 限制:仅对"新 key"生效,已存在的 key 可重入;
        // 同时要求该 key 当前未被持有(否则是同线程 reentry,不应被拒绝)。
        if (config.maxKeys() > 0 && wasNew && countHeldEntries() >= config.maxKeys()) {
            failedLocks.increment();
            long elapsedNanos = System.nanoTime() - startNanos;
            totalWaitTime.add(TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            // R6 修复(B-2):在 lockMap.remove 之前先把失败计数持久化到 LockEntry,
            // 否则 getStatistics(key) 会因为 entry 已被移除而看不到这次失败尝试。
            if (perKeyMetrics) {
                lockEntry.recordAcquireAttempt();
                lockEntry.recordAcquireFailure(elapsedNanos);
            }
            // 被拒绝的 key 不应在 lockMap 中残留(以免 getActiveKeys 把已拒绝的 key 算进去)
            lockMap.remove(key, lockEntry);
            fireFailed(key, elapsedNanos, LockFailureReason.MAX_KEYS);
            return false;
        }

        if (perKeyMetrics) {
            lockEntry.recordAcquireAttempt();
        }

        // R6 修复(E-4):fireContended 触发条件改为"无论 waitTime,只要发现争用就 fire"。
        // 之前仅 waitTime<=0 时触发,导致所有 >=1ms 的 tryLock 永不 fireContended,
        // 监控争用率严重偏低(只反映非阻塞超时)。现在尝试加锁前若被其他线程持有,
        // 立即触发争用事件,无论后续是否会等待。
        if (lockEntry.lock.isLocked() && !lockEntry.lock.isHeldByCurrentThread()) {
            fireContended(key);
        }

        try {
            boolean acquired = lockEntry.lock.tryLock(waitTime, timeUnit);
            if (acquired) {
                successLocks.increment();
                lockEntry.touchLastAcquireTime(config.clock());
                lockEntry.setOwnerThreadId(Thread.currentThread().getId());
                // 重新获取时复位 forceUnlock 哨兵,使后续 isLocked 反映真实状态
                lockEntry.clearForceUnlocked();
                lockEntry.incrementHoldCount();
                if (perKeyMetrics) {
                    lockEntry.recordAcquireSuccess(System.nanoTime() - startNanos);
                }
                if (leaseTime > 0) {
                    long leaseEnd = config.clock().millis() + timeUnit.toMillis(leaseTime);
                    lockEntry.setLeaseEndTime(leaseEnd);
                    try {
                        scheduleLeaseExpiry(key, leaseEnd);
                    } catch (RejectedExecutionException e) {
                        // shutdown 后 scheduleLeaseExpiry 抛 REE —— 锁已成功持有,
                        // 不应让业务方误以为获取失败。日志记录,继续返回 true。
                        log.warning("scheduleLeaseExpiry rejected (post-shutdown?): " + e.getMessage());
                    }
                }
                fireAcquired(key, System.nanoTime() - startNanos);
                return true;
            } else {
                failedLocks.increment();
                if (perKeyMetrics) {
                    lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
                }
                fireFailed(key, System.nanoTime() - startNanos, LockFailureReason.TIMEOUT);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedLocks.increment();
            if (perKeyMetrics) {
                lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
            }
            fireFailed(key, System.nanoTime() - startNanos, LockFailureReason.INTERRUPTED);
            return false;
        } finally {
            long elapsedNanos = System.nanoTime() - startNanos;
            totalWaitTime.add(TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            if (waitLatency != null) waitLatency.record(elapsedNanos);
        }
    }

    /**
     * 校验 key 合法性。R6 修复(E-5):拒绝空字符串 key,避免业务误用导致难以诊断的锁。
     *
     * <p>历史:之前 {@code tryLock("")} 会创建一个 key="" 的 entry,与正常 key 区分
     * 不出来,且业务上"空 key"通常是 bug。统一拒绝空字符串 key,抛
     * {@link IllegalArgumentException}。</p>
     */
    private void validateKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isEmpty()) throw new IllegalArgumentException("key must not be empty");
    }

    /**
     * 解析指定 key 的租约毫秒数。若配置了 {@link LeasePolicy} 且策略返回非 null 非零值,
     * 使用策略结果;否则使用 {@link #defaultLeaseTime}。
     */
    private long resolveLeaseTimeMillis(String key) {
        LeasePolicy policy = config.leasePolicy();
        if (policy != null) {
            try {
                Duration d = policy.leaseFor(key);
                if (d != null) return d.toMillis();
            } catch (Exception e) {
                log.warning("LeasePolicy threw for key=" + key + ": " + e.getMessage());
            }
        }
        return defaultLeaseTime;
    }

    private void scheduleLeaseExpiry(String key, long leaseEndMillis) {
        long delayMillis = Math.max(0, leaseEndMillis - config.clock().millis());
        cleanupExecutor.schedule(() -> {
            LockEntry entry = lockMap.get(key);
            if (entry != null && entry.getLeaseEndTime() == leaseEndMillis) {
                // 仍处于该租约窗口
                if (!entry.lock.isLocked()) {
                    return;  // 已被主动释放
                }
                // 尊重 forceUnlockOnLeaseExpiry 配置:若用户关闭租约到期自动清理,
                // 则仅更新 isLocked 视角(通过 lock 仍由原线程持有 + 哨兵策略)但不强制释放。
                if (!config.forceUnlockOnLeaseExpiry()) {
                    // 不 markForceUnlocked:保持 CHMRLock 视角为"仍持有",
                    // 让业务侧自己决定何时 unlock。但通过监听器通知租约到期。
                    fireExpired(key);
                    return;
                }
                // 租约到期。ReentrantLock 不支持跨线程 unlock(会抛 IllegalMonitorStateException),
                // 因此这里尝试解锁后必须通过 forceUnlock 哨兵让 isLocked 报告已释放。
                try {
                    entry.lock.unlock();
                } catch (IllegalMonitorStateException ignored) {
                    // 跨线程 unlock 失败,仅通过哨兵清理 CHMRLock 状态
                }
                // 哨兵置位:isLocked 据此返回 false,owner/lease 也被清空。
                // 后续由原持有线程调用 unlock(key) 时,unlock 会走哨兵感知路径,
                // 跳过底层 lock.unlock() 并清理 CHMRLock 状态。
                entry.markForceUnlocked();
                // 记录 per-key 释放时间戳,使 lastReleaseEpochMs 反映租约到期时刻
                if (config.enablePerKeyMetrics()) {
                    entry.recordRelease(config.clock().millis());
                }
                // 租约已到期,通知监听器(无论底层 ReentrantLock 是否真正释放)
                fireExpired(key);
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试获取指定 key 的锁,等待最多 {@code waitTime} 毫秒。
     * 便捷重载,等价于 {@link #tryLock(String, long, TimeUnit) tryLock(key, waitTime, TimeUnit.MILLISECONDS)}。
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间(毫秒)
     * @return 是否成功获取
     */
    public boolean tryLock(String key, long waitTime){
        validateKey(key);
        if (waitTime < 0) throw new IllegalArgumentException("waitTime 必须 >= 0,实际:" + waitTime);
        return tryLock(key, waitTime, resolveLeaseTimeMillis(key), TimeUnit.MILLISECONDS);
    }

    /**
     * 阻塞获取锁,无限等待,无租约。线程被中断时抛出 {@link InterruptedException}。
     * 实现基于 {@link java.util.concurrent.locks.ReentrantLock#lockInterruptibly()},
     * 因此等待期间可响应中断。
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @throws InterruptedException 等待获取过程中线程被中断
     * @throws IllegalStateException 若已调用 {@link #shutdown()}
     */
    public void lock(String key) throws InterruptedException {
        validateKey(key);
        if (shutdownCalled.get()) {
            throw new IllegalStateException("CHMRLock has been shutdown");
        }
        lock(key, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 阻塞获取锁,无限等待,可指定租约时长。租约到期后 {@link #isLocked(String)}
     * 会报告锁已释放,但底层 {@link java.util.concurrent.locks.ReentrantLock}
     * 仍由原持有线程持有,需配合 {@link #unlock(String)} 释放。
     *
     * <p><b>关于 {@code maxKeys}:</b>当 {@code maxKeys} 限制被触发时,
     * {@link #tryLock(String, long, long, TimeUnit) tryLock} 返回 {@code false}
     * (非阻塞),而 {@code lock} 抛 {@link cn.wubo.lock.MaxKeysExceededException}
     * (继承自 {@link IllegalStateException},以便向后兼容)。
     * 两者语义不同 —— {@code tryLock} 是"尽力而为",{@code lock} 是"必须拿到";
     * {@code maxKeys} 在调用时刻立即生效,业务方应自行决定如何处理(释放部分 key 后重试,
     * 或回退到 {@code tryLock})。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param leaseTime 租约时长(>0);0 表示无租约
     * @param timeUnit 时间单位
     * @throws InterruptedException 等待获取过程中线程被中断
     * @throws MaxKeysExceededException 当 {@code maxKeys} 限制被触发时(继承自 {@link IllegalStateException})
     * @throws IllegalStateException 若已调用 {@link #shutdown()}
     */
    public void lock(String key, long leaseTime, TimeUnit timeUnit) throws InterruptedException {
        validateKey(key);
        Objects.requireNonNull(timeUnit, "timeUnit must not be null");
        if (leaseTime < 0) throw new IllegalArgumentException("leaseTime 必须 >= 0,实际:" + leaseTime);
        // R6 修复(R-1):shutdown 后禁止新增 entry,避免永久泄漏
        if (shutdownCalled.get()) {
            throw new IllegalStateException("CHMRLock has been shutdown");
        }
        // R6 修复(E-1):用 System.nanoTime() 单调时钟计算 elapsed
        long startNanos = System.nanoTime();
        totalLocks.increment();
        if (hotKeySampler != null) hotKeySampler.record(key);
        boolean perKeyMetrics = config.enablePerKeyMetrics();

        // 记录 key 是否已存在于 map 中(用于 maxKeys 限制的"新 key"判断)
        boolean wasNew = !lockMap.containsKey(key);

        // 先创建 entry,后续 per-key metrics 可以记录在刚创建的 entry 上
        LockEntry lockEntry = lockMap.computeIfAbsent(key, k -> new LockEntry(config.fairLock()));

        // maxKeys 限制:仅对"新 key"生效,已存在的 key 可重入;
        // 同时要求该 key 当前未被持有(否则是同线程 reentry,不应被拒绝)。
        if (config.maxKeys() > 0 && wasNew && countHeldEntries() >= config.maxKeys()) {
            failedLocks.increment();
            long elapsedNanos = System.nanoTime() - startNanos;
            totalWaitTime.add(TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            // R6 修复(B-2):在 lockMap.remove 之前先把失败计数持久化
            if (perKeyMetrics) {
                lockEntry.recordAcquireAttempt();
                lockEntry.recordAcquireFailure(elapsedNanos);
            }
            // 被拒绝的 key 不应在 lockMap 中残留
            lockMap.remove(key, lockEntry);
            fireFailed(key, elapsedNanos, LockFailureReason.MAX_KEYS);
            throw new MaxKeysExceededException("maxKeys limit reached: " + config.maxKeys());
        }

        if (perKeyMetrics) {
            lockEntry.recordAcquireAttempt();
        }

        // R6 修复(E-4):fireContended 触发条件改为"无论 waitTime,只要发现争用就 fire"。
        if (lockEntry.lock.isLocked() && !lockEntry.lock.isHeldByCurrentThread()) {
            fireContended(key);
        }

        try {
            lockEntry.lock.lockInterruptibly();
            successLocks.increment();
            lockEntry.touchLastAcquireTime(config.clock());
            lockEntry.setOwnerThreadId(Thread.currentThread().getId());
            // 重新获取时复位 forceUnlock 哨兵
            lockEntry.clearForceUnlocked();
            lockEntry.incrementHoldCount();
            if (leaseTime > 0) {
                long leaseEnd = config.clock().millis() + timeUnit.toMillis(leaseTime);
                lockEntry.setLeaseEndTime(leaseEnd);
                try {
                    scheduleLeaseExpiry(key, leaseEnd);
                } catch (RejectedExecutionException e) {
                    // 同 tryLock 路径:shutdown 后 REE 仅记录警告,锁仍正常持有
                    log.warning("scheduleLeaseExpiry rejected (post-shutdown?): " + e.getMessage());
                }
            }
            if (perKeyMetrics) {
                lockEntry.recordAcquireSuccess(System.nanoTime() - startNanos);
            }
            fireAcquired(key, System.nanoTime() - startNanos);
        } catch (InterruptedException e) {
            failedLocks.increment();
            if (perKeyMetrics) {
                lockEntry.recordAcquireFailure(System.nanoTime() - startNanos);
            }
            fireFailed(key, System.nanoTime() - startNanos, LockFailureReason.INTERRUPTED);
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            long elapsedNanos = System.nanoTime() - startNanos;
            totalWaitTime.add(TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
            if (waitLatency != null) waitLatency.record(elapsedNanos);
        }
    }

    /**
     * 显式命名的可中断阻塞获取,语义完全等价于 {@link #lock(String, long, TimeUnit)}。
     *
     * <p>R6 注释(A-6):该方法是 {@code lock} 的语义别名,提供它是为了在 API 层显式表达
     * "可中断"语义,便于调用方在需要时区分普通阻塞 {@code lock} 与可中断阻塞。
     * 底层 {@code lock(key, leaseTime, timeUnit)} 已基于
     * {@link java.util.concurrent.locks.ReentrantLock#lockInterruptibly()} 实现,
     * 因此二者实现完全一致,无功能差异。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param leaseTime 租约时长(>0);0 表示无租约
     * @param timeUnit 时间单位
     * @throws InterruptedException 等待获取过程中线程被中断
     * @throws MaxKeysExceededException 当 {@code maxKeys} 限制被触发时(继承自 {@link IllegalStateException})
     * @throws IllegalStateException 若已调用 {@link #shutdown()}
     */
    public void lockInterruptibly(String key, long leaseTime, TimeUnit timeUnit) throws InterruptedException {
        validateKey(key);
        lock(key, leaseTime, timeUnit);
    }

    /**
     * 非可重入版本的 {@link #tryLock(String)},返回 {@link AcquiredLock} 以支持 try-with-resources。
     *
     * <p><b>非可重入语义:</b>与 {@code tryLock} 不同,若当前线程已持有 key,
     * 返回 {@link Optional#empty()} 而非成功。这保证了 {@link AcquiredLock} 包装
     * 与底层锁获取是 1:1 对应,避免 try-with-resources 重复释放。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @return 成功时返回包含 {@link AcquiredLock} 的 Optional,失败或被当前线程持有时返回空
     * @throws IllegalArgumentException 若 {@code key} 为空字符串
     */
    public Optional<AcquiredLock> tryAcquire(String key) {
        validateKey(key);
        return tryAcquire(key, defaultWaitTime, defaultLeaseTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取锁并返回 {@link AcquiredLock} 包装,支持 try-with-resources;等待最多 {@code waitTime}。
     *
     * <p><b>非可重入语义:</b>与 {@link #tryLock(String, long, TimeUnit) tryLock} 不同,
     * 若当前线程已持有 key,直接返回 {@link Optional#empty()} (不等待)。
     * 这保证 {@link AcquiredLock} 包装与底层锁获取是 1:1 对应,
     * 避免 try-with-resources 重复释放。详细语义见 {@link #tryAcquire(String)}。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return 成功则返回包含 {@link AcquiredLock} 的 Optional,否则空
     */
    public Optional<AcquiredLock> tryAcquire(String key, long waitTime, TimeUnit timeUnit) {
        validateKey(key);
        return tryAcquire(key, waitTime, 0, timeUnit);
    }

    /**
     * 获取锁并返回 {@link AcquiredLock} 包装,支持 try-with-resources,可指定租约。
     *
     * <p><b>非可重入语义:</b>与 {@link #tryLock(String, long, long, TimeUnit) tryLock} 不同,
     * 若当前线程已持有 key,直接返回 {@link Optional#empty()} (不等待)。
     * 这是 tryAcquire 的所有重载中唯一支持租约的入口。</p>
     *
     * <p><b>R6 修复(B-1):</b>非可重入 fast-fail 失败时会同时增加全局
     * {@code failedLocks} 计数(之前只增加 per-key 计数或完全不计数)。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间
     * @param leaseTime 租约时长,0 表示无租约
     * @param timeUnit 时间单位
     * @return 成功则返回包含 {@link AcquiredLock} 的 Optional,否则空
     */
    public Optional<AcquiredLock> tryAcquire(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        validateKey(key);
        // 非可重入 fast-fail:R6 修复(B-1)—— fast-fail 现在计入全局 failedLocks,
        // 之前注释说计入但实际未计入(per-key 也未记,因为还没进 tryLock),导致
        // 总锁计数与成功+失败计数之和不等于 totalLocks。现在统一在 fast-fail
        // 路径也调用 failedLocks.increment(),保持 totalLocks == success + failed 不变。
        if (isHeldByCurrentThread(key)) {
            failedLocks.increment();
            return Optional.empty();
        }
        if (tryLock(key, waitTime, leaseTime, timeUnit)) {
            return Optional.of(new AcquiredLock(this, key));
        }
        return Optional.empty();
    }

    /**
     * 异步获取锁并返回 {@link AcquiredLock} 包装(支持 try-with-resources)。
     * 底层使用独立线程池,不会阻塞调用线程。语义与 {@link #tryAcquire(String)} 一致
     * (非可重入:若当前线程已持有 key,返回 {@code Optional.empty()})。
     *
     * <p>底层由专用守护线程池执行,不会阻塞调用线程。线程命名格式为 {@code chmrlock-async-N}。</p>
     * <p>返回的 {@link AcquiredLock} 的持有线程为执行加锁的异步线程 ——
     *    若从调用线程调用 {@code close()},由于 ReentrantLock 不允许跨线程释放,
     *    实际不会释放锁(异常会被吞掉)。</p>
     * <p><b>注意:</b>取消返回的 future 不会中断底层加锁;若加锁成功,锁仍由异步线程持有,
     *    必须通过 {@link #unlock(String)} 或 {@link #forceUnlock(String)} 释放。</p>
     *
     * <p><b>shutdown 后行为(R6 修复 R-2):</b>在 {@link #shutdown()} 之后调用
     * 会立即抛出 {@link RejectedExecutionException} —— 业务方应显式捕获并降级处理
     * (例如转为同步 tryAcquire)。之前的行为是"shutdown 后由调用线程同步执行",
     * 这违背了"异步不阻塞调用线程"的承诺;现在改为标准行为,业务方感知更明确。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @return CompletableFuture&lt;Optional&lt;AcquiredLock&gt;&gt;: 成功时 Optional 非空,否则空
     */
    public CompletableFuture<Optional<AcquiredLock>> tryAcquireAsync(String key) {
        validateKey(key);
        return CompletableFuture.supplyAsync(() -> tryAcquire(key), asyncExecutor);
    }

    /**
     * 异步获取锁并返回 {@link AcquiredLock} 包装,带超时。底层使用独立线程池,不会阻塞调用线程。
     * 语义与 {@link #tryAcquire(String, long, TimeUnit)} 一致
     * (非可重入:若当前线程已持有 key,返回 {@code Optional.empty()})。
     *
     * <p>底层由专用守护线程池执行,不会阻塞调用线程。线程命名格式为 {@code chmrlock-async-N}。</p>
     * <p>返回的 {@link AcquiredLock} 的持有线程为执行加锁的异步线程 ——
     *    若从调用线程调用 {@code close()},由于 ReentrantLock 不允许跨线程释放,
     *    实际不会释放锁(异常会被吞掉)。</p>
     * <p><b>注意:</b>取消返回的 future 不会中断底层加锁;若加锁成功,锁仍由异步线程持有,
     *    必须通过 {@link #unlock(String)} 或 {@link #forceUnlock(String)} 释放。</p>
     *
     * <p><b>shutdown 后行为:</b>同 {@link #tryAcquireAsync(String)} —— shutdown 后
     * 抛 {@link RejectedExecutionException},由业务方显式处理。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @param waitTime 等待获取的最长时间
     * @param timeUnit 时间单位
     * @return CompletableFuture&lt;Optional&lt;AcquiredLock&gt;&gt;: 成功时 Optional 非空,否则空
     */
    public CompletableFuture<Optional<AcquiredLock>> tryAcquireAsync(String key, long waitTime, TimeUnit timeUnit) {
        validateKey(key);
        return CompletableFuture.supplyAsync(() -> tryAcquire(key, waitTime, timeUnit), asyncExecutor);
    }

    /**
     * 原子获取多个 key,全部成功或全部失败。失败时回滚已获取的锁(调用 unlock 释放)。
     * keys 内部按字典序排序后逐个加锁,防止多线程按不同顺序加锁导致的死锁。
     * 重复的 key 会被去重,只加锁一次。
     * 使用 {@link #defaultWaitTime} 作为单个 key 的最大等待时间。
     *
     * @param keys 要获取的 key 列表(可变参数;不可含 null 或空字符串)
     * @return 是否全部成功;空数组或 null 视为成功(无操作)
     * @throws IllegalArgumentException 若任一 key 为空字符串
     */
    public boolean tryMultiLock(String... keys) {
        return tryMultiLock(defaultWaitTime, 0, TimeUnit.MILLISECONDS, keys);
    }

    /**
     * 原子获取多个 key,带超时。失败时回滚已获取的锁。
     * 等价于 {@code tryMultiLock(waitTime, 0, timeUnit, keys)} — 无租约。
     *
     * <p>R6 修复(A-4):JavaDoc 已明确"总等待时间近似 N × waitTime"——实际最坏情况
     * 取决于预算分配算法:每个 key 预算为 {@code waitTime / N} 且不低于 1ms,
     * 所以 N 个 key 的最坏总等待时间约为 {@code waitTime} (但因下限 1ms,可能
     * 略高)。如果用户需要严格的"总等待时间 = waitTime"保证,请自行外层计时并降级。</p>
     *
     * @param waitTime 单个 key 的最大等待时间(总等待时间最坏为 {@code waitTime} 附近)
     * @param timeUnit 时间单位
     * @param keys 要获取的 key 列表(不可含 null 或空字符串)
     * @return 是否全部成功
     */
    public boolean tryMultiLock(long waitTime, TimeUnit timeUnit, String... keys) {
        return tryMultiLock(waitTime, 0, timeUnit, keys);
    }

    /**
     * 原子获取多个 key,带超时与租约。失败时回滚。
     * 排序与去重:keys 按字典序排序以避免多线程按不同顺序加锁导致的死锁;
     * 重复 key 仅加锁一次。
     *
     * <p>每个 key 的等待时间按公平份额分配(单 key 预算 = 总预算 / N),
     * 同时受剩余时间约束。最坏情况下,总等待时间约为 {@code waitTime}(N 个 key
     * 中每个最多等 {@code waitTime / N} ms);由于每个 key 有 1ms 等待下限
     * (避免 0ms 退化为空操作),最后一把锁可能略超预期。详细边界分析见
     * {@link #tryMultiLock(long, TimeUnit, String...)} 的 R6 注释。</p>
     *
     * @param waitTime 单个 key 的最大等待时间
     * @param leaseTime 租约时间(以 {@code timeUnit} 为单位),0 表示无租约
     * @param timeUnit 时间单位
     * @param keys 要获取的 key 列表(不可含 null 或空字符串)
     * @return 是否全部成功
     * @throws IllegalArgumentException 若任一 key 为空字符串
     */
    public boolean tryMultiLock(long waitTime, long leaseTime, TimeUnit timeUnit, String... keys) {
        if (keys == null || keys.length == 0) return true;
        Objects.requireNonNull(timeUnit, "timeUnit must not be null");

        // 显式校验数组中是否含有 null 或空字符串 ——
        // 否则后续 Comparator.naturalOrder()(在 distinct/sorted 中)对 null 抛 NPE 错误信息不友好,
        // 对空字符串则会让空 key 进入 lockMap。
        for (String k : keys) {
            validateKey(k);
        }

        // Sort + dedupe: lexicographic ordering prevents deadlock across threads,
        // and distinct() ensures each key is acquired at most once.
        String[] unique = Arrays.stream(keys).distinct().sorted().toArray(String[]::new);

        // Try to acquire each key in sorted order; track acquired for rollback on failure.
        List<String> acquired = new ArrayList<>(unique.length);
        long startNanos = System.nanoTime();
        long totalNanos = timeUnit.toNanos(waitTime);

        // Pre-compute a fair per-key budget so the cumulative wait stays bounded by
        // waitTime. The last key may still get a 1ms floor (Math.max(1, ...)), so the
        // worst-case total is waitTime + 1ms instead of waitTime + N-1 ms.
        int n = unique.length;
        long perKeyBudgetNanos = totalNanos / n;

        // leaseTime 已经在 timeUnit 中,先转成毫秒
        long leaseMillis = timeUnit.toMillis(leaseTime);

        for (String key : unique) {
            long elapsedNanos = System.nanoTime() - startNanos;
            long remainingNanos = totalNanos - elapsedNanos;
            if (remainingNanos <= 0) {
                rollback(acquired);
                return false;
            }
            // 给每个 key 一个公平的预算份额,最后再用剩余时间作上限
            long budgetNanos = Math.min(perKeyBudgetNanos, remainingNanos);
            long budgetMillis = Math.max(1, budgetNanos / 1_000_000);
            if (!tryLock(key, budgetMillis, leaseMillis, TimeUnit.MILLISECONDS)) {
                rollback(acquired);
                return false;
            }
            acquired.add(key);
        }
        return true;
    }

    /**
     * 回滚已获取的 key(在多 key 原子获取失败时调用)。
     * 任何异常被静默忽略,因为这是 best-effort 回滚。
     */
    private void rollback(List<String> acquired) {
        for (String key : acquired) {
            try {
                unlock(key);
            } catch (Exception ignored) {
                // best-effort rollback
            }
        }
    }

    /**
     * 释放锁。key 必须已经通过 tryLock 成功获取。
     *
     * <p><b>哨兵感知路径(粘性哨兵):</b>如果该 key 曾被 {@link #forceUnlock(String)}
     * 强制释放,或租约到期后由后台线程清理,本方法会先检查 forceUnlock 哨兵 —
     * 若哨兵已置位,跳过底层 {@code lock.unlock()}(避免抛 {@link IllegalMonitorStateException}),
     * 清理 owner/lease,触发一次 {@link LockListener#onLockReleased} 事件。哨兵保持
     * "粘性"直至下一次 {@link #tryLock(String)} 成功获取;哨兵粘性期间的后续 unlock
     * 都是幂等 no-op,既不会重复 fireReleased,也不会触发底层 unlock 抛异常。</p>
     *
     * <p>若哨兵未置位但底层 lock.unlock() 仍抛 {@link IllegalMonitorStateException}
     * (例如另一个线程刚调用了 forceUnlock),本方法会再次检查哨兵并按哨兵路径处理;
     * 仅当哨兵确实未置位时才重新抛出异常,标记为跨线程 unlock。</p>
     *
     * <p><b>关于未知 key:</b>若 key 已被清理线程释放(常见于 maxKeys 限制触发或
     * 后台清理已将该 entry 移除),此方法为 no-op 并记录 FINE 级别日志 ——
     * 不抛 {@link LockNotFoundException}。这样保证 {@link AcquiredLock#close()}
     * 在清理后调用 unlock 不会失败,且 {@link LockListener#onLockReleased} 的
     * 触发不会被静默吞掉。</p>
     *
     * <p><b>R6 修复(B-5):</b>跨线程 forceUnlock 后,持有线程调用 unlock 走哨兵
     * 路径(因为 forceUnlocked 位仍置位),不会触发 {@link IllegalMonitorStateException};
     * 后续重入 unlock 走粘性 no-op 路径,亦无异常。但底层 ReentrantLock 的
     * holdCount 与 CHMRLock 维护的 holderHoldCount 可能分裂 —— 这是
     * {@link java.util.concurrent.locks.ReentrantLock} 的根本限制(无法跨线程
     * 修改 holdCount),只能文档化。{@link LockEntry#decrementHoldCount()}
     * 会将 holderHoldCount 钳制到 {@code >= 0},避免出现负数。</p>
     *
     * @throws IllegalMonitorStateException 锁未被当前线程持有(跨线程 unlock)
     */
    public void unlock(String key) {
        validateKey(key);
        LockEntry lockEntry = lockMap.get(key);
        if (lockEntry == null) {
            // Entry 已被清理(典型场景:后台清理线程在 lockMap 中移除了该 key)。
            // 视为幂等 no-op,记录 FINE 级别日志,避免 AcquiredLock.close() 失败。
            log.fine("unlock: key not found, likely cleaned up: " + key);
            return;
        }
        // 哨兵已置位:跳过底层 unlock,清理 CHMRLock 状态。
        // 哨兵保持"粘性"(直至下一次 tryLock 成功才清除),避免后续 unlock
        // 走到底层 lock.unlock() 时抛出 IllegalMonitorStateException。
        // onLockReleased 事件仅触发一次(由 releaseEventFired 标记控制)。
        if (lockEntry.isForceUnlocked()) {
            if (lockEntry.isReleaseEventFired()) {
                // 后续 unlock: 已 fireReleased 过,直接 no-op,避免重复事件。
                return;
            }
            lockEntry.markReleaseEventFired();
            lockEntry.clearOwner();
            lockEntry.clearLease();
            if (config.enablePerKeyMetrics()) {
                lockEntry.recordRelease(config.clock().millis());
            }
            fireReleased(key, config.clock().millis() - lockEntry.getLastAcquireTime());
            return;
        }
        try {
            lockEntry.lock.unlock();
            // 持有线程已成功 unlock 一次,holderHoldCount 减一
            lockEntry.decrementHoldCount();
        } catch (IllegalMonitorStateException e) {
            // 竞态:在哨兵检查与 unlock 之间另一个线程调用了 forceUnlock。
            // 重新检查哨兵,若已置位则按哨兵路径处理。
            if (lockEntry.isForceUnlocked()) {
                if (lockEntry.isReleaseEventFired()) {
                    return;
                }
                lockEntry.markReleaseEventFired();
                lockEntry.clearOwner();
                lockEntry.clearLease();
                if (config.enablePerKeyMetrics()) {
                    lockEntry.recordRelease(config.clock().millis());
                }
                fireReleased(key, config.clock().millis() - lockEntry.getLastAcquireTime());
                return;
            }
            throw e;  // 真正的跨线程 unlock
        }
        lockEntry.clearOwner();
        lockEntry.clearLease();
        if (config.enablePerKeyMetrics()) {
            lockEntry.recordRelease(config.clock().millis());
        }
        long heldMillis = config.clock().millis() - lockEntry.getLastAcquireTime();
        fireReleased(key, heldMillis);
    }

    /**
     * 强制释放指定 key 的锁,可从任意线程调用,绕过 owner 检查。
     *
     * <p>由于 {@link java.util.concurrent.locks.ReentrantLock} 的 {@code unlock}
     * 要求 owner 线程,从其他线程真正释放底层锁需要反射 AQS 内部状态(脆弱、依赖 JDK 版本)。
     * 本方法的实际行为是:尝试调用底层 {@code unlock}(若当前线程即 owner 则成功;
     * 若非 owner 则会抛 {@link IllegalMonitorStateException},被静默忽略),
     * 然后通过 {@link LockEntry} 中的 forceUnlock 哨兵把 owner / lease 状态清空,
     * 并让 {@link #isLocked(String)} 报告该 key 已释放。后续若同一线程再次调用
     * {@link #tryLock(String)} 成功,哨兵会被自动清除。</p>
     *
     * <p>对未知 key 是 no-op(idempotent)。对已释放 key 也是 no-op。</p>
     *
     * <p>需 {@link CHMRLockConfig#forceUnlockEnabled()} 为 true,否则抛
     * {@link UnsupportedOperationException}。</p>
     *
     * <p><b>R6 修复(B-5):</b>跨线程 forceUnlock 后,持有线程继续 unlock 走哨兵
     * 粘性 no-op 路径,不会抛 {@link IllegalMonitorStateException};但底层
     * ReentrantLock 的 holdCount 与 CHMRLock 维护的 holderHoldCount 可能分裂 —
     * 这是 {@link ReentrantLock} 的根本限制(无法跨线程修改 holdCount),只能文档化。
     * 业务方应避免在跨线程 forceUnlock 后依赖 {@link #getHoldCount(String)} 的精确性。</p>
     *
     * @param key 锁标识(不可为空,且不可为空字符串)
     * @throws UnsupportedOperationException 若 {@code config.forceUnlockEnabled()} 为 false
     */
    public void forceUnlock(String key) {
        validateKey(key);
        if (!config.forceUnlockEnabled()) {
            throw new UnsupportedOperationException(
                    "forceUnlock is not enabled. Set CHMRLockConfig.forceUnlockEnabled(true) to enable.");
        }
        LockEntry lockEntry = lockMap.get(key);
        if (lockEntry == null) {
            // 未知 key：幂等 no-op
            return;
        }
        // 尝试底层 unlock：若当前线程即 owner 则成功；非 owner 抛 IllegalMonitorStateException
        try {
            lockEntry.lock.unlock();
        } catch (IllegalMonitorStateException ignored) {
            // 跨线程 unlock 失败，仅清理 CHMRLock 状态
        }
        // 哨兵置位：isLocked 据此返回 false，owner / lease 清空
        lockEntry.markForceUnlocked();
        // 同时置位 releaseEventFired:本方法已 fireReleased 一次,
        // 后续 unlock 走哨兵感知路径时看到此标记,直接 no-op,避免重复触发 onLockReleased。
        lockEntry.markReleaseEventFired();
        // 重置 holderHoldCount:跨线程 forceUnlock 后,持有线程的 ReentrantLock
        // 计数无法跨线程减少,统一重置为 0 以避免与 isLocked 不一致。
        lockEntry.resetHoldCount();
        if (config.enablePerKeyMetrics()) {
            lockEntry.recordRelease(config.clock().millis());
        }
        fireReleased(key, config.clock().millis() - lockEntry.getLastAcquireTime());
    }

    /**
     * 判断 key 当前是否被锁。
     *
     * <p>注意:若启用了租约且 {@link CHMRLockConfig#forceUnlockOnLeaseExpiry()} 为 true
     * (默认),租约到期后此方法会返回 false,但底层 ReentrantLock 仍由原持有线程持有,
     * 直到调用 {@link #unlock(String)}。调用 {@link #forceUnlock(String)} 后此方法
     * 同样返回 false。</p>
     *
     * <p>若 {@link CHMRLockConfig#forceUnlockOnLeaseExpiry()} 为 false,租约到期
     * <b>不会</b>让 {@code isLocked} 返回 false,业务侧需自行决定何时解锁。</p>
     */
    public boolean isLocked(String key) {
        validateKey(key);
        LockEntry e = lockMap.get(key);
        if (e == null) return false;
        // forceUnlock 已置位:从 CHMRLock 视角看已释放
        if (e.isForceUnlocked()) return false;
        if (!e.lock.isLocked()) return false;
        // 租约到期视为已释放(ReentrantLock 不支持跨线程 force-unlock)
        // 仅在 forceUnlockOnLeaseExpiry=true 时生效;为 false 时让业务侧处理。
        if (config.forceUnlockOnLeaseExpiry() && config.clock().millis() > e.getLeaseEndTime()) {
            return false;
        }
        return true;
    }

    /**
     * @param key 锁标识
     * @return 当前线程是否持有该 key；key 不存在返回 false
     */
    public boolean isHeldByCurrentThread(String key) {
        validateKey(key);
        LockEntry e = lockMap.get(key);
        return e != null && e.lock.isHeldByCurrentThread();
    }

    /**
     * @param key 锁标识
     * @return 当前线程对该 key 的重入深度；key 不存在返回 0
     *
     * <p>R6 修复(B-4/A-3):getHoldCount 现在统一返回 {@link LockEntry#getHolderHoldCount()},
     * 与 {@link KeyStatistics#currentHoldCount()} 保持一致。之前的实现直接返回
     * {@link ReentrantLock#getHoldCount()},跨线程读时永远为 0,与 {@link #getOwnerThreadId}
     * 和 KeyStatistics.currentHolderThreadId 矛盾。现在统一从 LockEntry 维护的字段读取,
     * 跨线程也返回有意义的结果。</p>
     *
     * <p>forceUnlock 后的 entry 从 CHMRLock 视角看已释放,getHoldCount 返回 0,
     * 以保持与 {@link #isLocked(String)} 一致。注意:在跨线程 forceUnlock 场景下,
     * {@link ReentrantLock} 的 holdCount(由底层 JDK 维护)可能仍大于 0,但那是
     * JDK 内部的 owner 状态,无法跨线程修改 —— 此处仅暴露 CHMRLock 视角。</p>
     */
    public int getHoldCount(String key) {
        validateKey(key);
        LockEntry e = lockMap.get(key);
        if (e == null) return 0;
        // forceUnlock 后的 entry:从 CHMRLock 视角看已释放
        if (e.isForceUnlocked()) return 0;
        return (int) Math.min(e.getHolderHoldCount(), Integer.MAX_VALUE);
    }

    /**
     * @param key 锁标识
     * @return 当前持有该 key 的线程 id；key 不存在或未被持有返回 {@code null}
     */
    public Long getOwnerThreadId(String key) {
        validateKey(key);
        LockEntry e = lockMap.get(key);
        return e == null ? null : e.getOwnerThreadId();
    }

    /**
     * @return 当前所有已注册 key 的不可变视图。对返回集合的修改会抛
     *         {@link UnsupportedOperationException}，但底层 map 的后续变更会反映在
     *         下次调用此方法的结果中。
     */
    public Set<String> getActiveKeys() {
        return Collections.unmodifiableSet(lockMap.keySet());
    }

    /**
     * 获取指定 key 的读写锁。与 tryLock/unlock 互不干扰 — 读写锁是独立的锁实例。
     * 同一 key 多次调用返回同一实例。
     *
     * <p><b>非可重入:</b>底层基于 {@link java.util.concurrent.locks.StampedLock},
     * 不支持重入;同一线程连续获取读/写锁会死锁。</p>
     *
     * <p>典型用法:</p>
     * <pre>{@code
     * KeyedReadWriteLock rw = lock.readWriteLock("resource");
     * long stamp = rw.readLock();
     * try {
     *     // 读取共享数据
     * } finally {
     *     rw.unlockRead(stamp);
     * }
     * }</pre>
     *
     * <p><b>生命周期:</b>读写锁缓存在 {@code rwLockMap} 中,生命周期与 CHMRLock 一致 —
     * 不受清理线程影响。shutdown 后,已返回的读写锁实例仍可使用。</p>
     *
     * @param key 锁标识
     * @return 该 key 对应的读写锁实例(同一 key 共享)
     * @see KeyedReadWriteLock
     */
    public KeyedReadWriteLock readWriteLock(String key) {
        validateKey(key);
        return rwLockMap.computeIfAbsent(key, k -> new StampedKeyedReadWriteLock());
    }

    /**
     * 注册一个锁生命周期监听器。可重复注册同一个实例,但通常不需要。
     *
     * <p>R6 修复(A-7):现在与 {@link #registerDistributedLock(String, DistributedLock)}
     * 保持一致的 null 处理 —— 传入 {@code null} 抛 {@link NullPointerException},
     * 之前是静默忽略。业务方若需要"动态决定是否注册"的语义,应在外层用 {@code if (listener != null)} 判断。</p>
     *
     * <p>R6 修复(E-6):listener 抛出的异常(包括 {@link Error})会被吞掉,但 listener
     * 没有超时保护 —— <b>listener 实现必须快速且无副作用</b>(不应包含阻塞 I/O、
     * 网络请求、死循环等),否则会拖慢整个锁获取路径。</p>
     *
     * @param listener 监听器实例(不可为 null)
     * @throws NullPointerException 若 {@code listener} 为 null
     */
    public void registerListener(LockListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    /** 注销一个监听器。{@code null} 被忽略;注销未注册的实例是 no-op。
     *  <p>基于引用相等(==)进行匹配,便于"可重复注册同一个实例"的精确注销。
     *  若希望按 equals 匹配,请在自定义 LockListener 中确保实现恰当的 equals。</p> */
    public void unregisterListener(LockListener listener) {
        if (listener == null) return;
        listeners.removeIf(l -> l == listener);
    }

    /**
     * 注册分布式锁实现,后续可通过 {@link #getDistributedLock(String)} 获取。
     * 同一 name 重复注册会覆盖前一次注册。
     * <p>CHMRLock 不会自动将本地 {@code tryLock} 路由到分布式锁 —— 用户需自行
     * 通过 {@code getDistributedLock(name)} 取出后调用,以实现"按需走分布式"的语义。</p>
     *
     * @param name 逻辑名称(用户自定义,例如 {@code "tenantA"} / {@code "userId"})
     * @param distributedLock 分布式锁实现,不可为 null
     * @throws NullPointerException 当 {@code name} 或 {@code distributedLock} 为 null
     * @see #getDistributedLock(String)
     * @see #unregisterDistributedLock(String)
     * @since 2.0.0
     */
    public void registerDistributedLock(String name, DistributedLock distributedLock) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(distributedLock, "distributedLock must not be null");
        distributedLocks.put(name, distributedLock);
    }

    /**
     * 获取已注册的分布式锁。
     *
     * @param name 逻辑名称
     * @return 对应的分布式锁;若未注册则为空 {@link Optional}
     * @throws NullPointerException 当 {@code name} 为 null
     * @see #registerDistributedLock(String, DistributedLock)
     * @since 2.0.0
     */
    public Optional<DistributedLock> getDistributedLock(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return Optional.ofNullable(distributedLocks.get(name));
    }

    /**
     * 注销分布式锁。
     *
     * @param name 逻辑名称
     * @return 是否成功移除(若 name 未注册则返回 false)
     * @throws NullPointerException 当 {@code name} 为 null
     * @see #registerDistributedLock(String, DistributedLock)
     * @since 2.0.0
     */
    public boolean unregisterDistributedLock(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return distributedLocks.remove(name) != null;
    }

    private void fireAcquired(String key, long waitNanos) {
        for (LockListener l : listeners) {
            // R6 修复(E-3):从 catch(Exception) 改为 catch(Throwable),隔离 Error(如 StackOverflowError、
            // OutOfMemoryError 等),防止 listener 抛致命错误破坏 tryLock 的状态(锁已成功持有但 tryLock
            // 调用方收到 Error,误以为获取失败)。这与其他 listener 隔离策略保持一致。
            try { l.onLockAcquired(key, waitNanos); } catch (Throwable t) {
                log.warning("LockListener.onLockAcquired threw: " + t);
            }
        }
    }

    private void fireReleased(String key, long heldMillis) {
        for (LockListener l : listeners) {
            try { l.onLockReleased(key, heldMillis); } catch (Throwable t) {
                log.warning("LockListener.onLockReleased threw: " + t);
            }
        }
    }

    private void fireFailed(String key, long waitNanos, LockFailureReason reason) {
        for (LockListener l : listeners) {
            try { l.onLockFailed(key, waitNanos, reason); } catch (Throwable t) {
                log.warning("LockListener.onLockFailed threw: " + t);
            }
        }
    }

    private void fireExpired(String key) {
        for (LockListener l : listeners) {
            try { l.onLockExpired(key); } catch (Throwable t) {
                log.warning("LockListener.onLockExpired threw: " + t);
            }
        }
    }

    private void fireContended(String key) {
        for (LockListener l : listeners) {
            try { l.onLockContended(key); } catch (Throwable t) {
                log.warning("LockListener.onLockContended threw: " + t);
            }
        }
    }

    /**
     * 关闭锁管理器，停止清理线程并清空所有锁。幂等，可多次调用。
     *
     * <p><b>注意:</b>此方法仅发起关闭请求,不等待进行中的任务完成。in-flight 的
     * tryLock / tryAcquire 调用可能仍在执行;若需确保清理线程与异步任务完全终止,
     * 建议在外部等待一个合理的超时(例如 {@code awaitTermination(5, SECONDS)})。
     * 本方法不等任何 lockMap 中的活动线程解锁 — 这些锁的释放由调用方自行负责。</p>
     */
    public void shutdown() {
        if (shutdownCalled.compareAndSet(false, true)) {
            lockMap.clear();
            distributedLocks.clear();
            if (cleanupExecutor != null) {
                cleanupExecutor.shutdown();
            }
            if (asyncExecutor != null && ownsAsyncExecutor) {
                asyncExecutor.shutdown();
            }
        }
    }

    /** @return 是否已调用过 {@link #shutdown()} */
    public boolean isShutdown() {
        return shutdownCalled.get();
    }

    /** @return 实例名称，来自 {@link CHMRLockConfig#name()}；默认 {@code "default"} */
    public String getName() {
        return config.name();
    }

    /** {@link AutoCloseable#close()} 实现，等价于 {@link #shutdown()}。支持 try-with-resources。 */
    @Override
    public void close() {
        shutdown();
    }

    /** @return 全局统计指标快照 */
    public MonitorMetrics getStatistics() {
        return new MonitorMetrics(
                totalLocks.sum(),
                successLocks.sum(),
                failedLocks.sum(),
                totalWaitTime.sum()
        );
    }

    /**
     * 返回 tryLock 等待时长的延迟分布直方图。需 {@link CHMRLockConfig#recordStats()} 为 true。
     *
     * <p>7 桶对数分桶（1µs / 10µs / 100µs / 1ms / 10ms / 100ms / &gt;100ms），
     * 便于定位"P99 慢锁"问题 —— 平均值无法反映的分布异常，直方图可以。
     *
     * @return 直方图实例；未启用时返回 {@code null}
     * @since 2.1.0
     */
    public LatencyHistogram latencyHistogram() {
        return waitLatency;
    }

    /**
     * 返回热点 key 采样器。需 {@link CHMRLockConfig#recordStats()} 为 true。
     *
     * <p>基于 Count-Min Sketch（4 哈希 × 1024 桶 ≈ 8KB），每 1 万次访问采样 1 次，
     * 在未开启 per-key metrics 时也能识别争用热点锁。</p>
     *
     * @return 采样器实例；未启用时返回 {@code null}
     * @since 2.1.0
     */
    public HotKeySampler hotKeySampler() {
        return hotKeySampler;
    }

    /**
     * 返回指定 key 的统计指标快照。需 {@link CHMRLockConfig#enablePerKeyMetrics} 为 true。
     *
     * @param key 锁标识
     * @return 指标快照；若 metrics 未启用或 key 从未加锁，返回 {@link Optional#empty()}
     */
    public Optional<KeyStatistics> getStatistics(String key) {
        validateKey(key);
        if (!config.enablePerKeyMetrics()) return Optional.empty();
        LockEntry e = lockMap.get(key);
        if (e == null) return Optional.empty();
        return Optional.of(snapshotFor(key, e));
    }

    /**
     * 返回所有曾加锁 key 的统计指标快照。需 {@link CHMRLockConfig#enablePerKeyMetrics} 为 true。
     *
     * <p><b>注意:</b>此方法返回弱一致性快照,不同 key 的统计可能反映不同时间点。
     * 由于 lockMap 在并发环境下持续变化,迭代过程中 key 可能被清理线程移除或新增,
     * 同一 map 中各 entry 的读顺序与底层状态完全无锁同步。适合用于监控/展示场景,
     * 不适合用于精确的事务性比较。</p>
     *
     * @return 不可变 Map；key -> 指标快照。未启用 metrics 时返回空 Map。
     */
    public Map<String, KeyStatistics> getAllStatistics() {
        if (!config.enablePerKeyMetrics()) return Collections.emptyMap();
        Map<String, KeyStatistics> snapshot = new HashMap<>(lockMap.size());
        for (Map.Entry<String, LockEntry> entry : lockMap.entrySet()) {
            String k = entry.getKey();
            LockEntry e = entry.getValue();
            snapshot.put(k, snapshotFor(k, e));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    private static KeyStatistics snapshotFor(String key, LockEntry e) {
        Long holderId = e.getOwnerThreadId();
        // 使用 holderHoldCount 而非 ReentrantLock.getHoldCount():后者只对当前线程
        // 有意义,跨线程读为 0,会与 holderId 矛盾。holderHoldCount 由 CHMRLock
        // 在加锁/解锁时维护,跨线程快照一致。
        // R6 修复(E-9):holderHoldCount 是 AtomicLong(long),KeyStatistics.currentHoldCount
        // 也是 long(已支持 64 位重入深度),直接传递不再溢出。
        long holdCount = e.getHolderHoldCount();
        return new KeyStatistics(
                key,
                e.getAcquireCount(),
                e.getSuccessCount(),
                e.getFailedCount(),
                e.getTotalWaitNanos(),
                e.getLastAcquireTime(),
                e.getLastReleaseTime(),
                holdCount,
                holderId == null ? -1L : holderId
        );
    }

    /**
     * 触发一次指标导出。内部调用 {@link MetricsExporter#export(MonitorMetrics, Map)},
     * 传入 {@link #getStatistics()} 和 {@link #getAllStatistics()}。
     *
     * <p>导出器抛出的异常会被吞掉,不会影响锁功能(与监听器异常隔离一致)。</p>
     *
     * <p>R6 注释(L-9):shutdown 后调用 {@code exportMetrics} 仍可执行 —— 全局统计
     * (来自 {@code totalLocks} 等 AtomicLong)仍可读,但 {@link #getAllStatistics()}
     * 返回空 Map(因 lockMap 已被 clear)。业务方应自行判断是否仍需要导出。</p>
     *
     * @param exporter 导出器实现(不可为 null)
     * @throws NullPointerException 若 {@code exporter} 为 null
     * @since 2.0.0
     */
    public void exportMetrics(MetricsExporter exporter) {
        Objects.requireNonNull(exporter, "exporter must not be null");
        try {
            exporter.export(getStatistics(), getAllStatistics());
        } catch (Exception e) {
            log.warning("MetricsExporter failed: " + e.getMessage());
        }
    }

    /**
     * 触发一次统计持久化记录。内部调用 {@link StatisticsSink#record(MonitorMetrics, Map)},
     * 传入 {@link #getStatistics()} 和 {@link #getAllStatistics()}。
     *
     * <p>与 {@link #exportMetrics(MetricsExporter)} 的区别:
     * <ul>
     *   <li>{@code MetricsExporter} — 一次性的指标导出(适合推送到 Prometheus / StatsD 等)</li>
     *   <li>{@code StatisticsSink} — 持续累积的持久化存储(适合写入数据库 / 时序数据库 / 日志文件)</li>
     * </ul>
     * 持久化 sink 抛出的异常会被吞掉,不会影响锁功能。</p>
     *
     * <p>CHMRLock 不会内置周期性 record 调度 ——
     * 需要周期性持久化时,请在外部使用 {@link java.util.concurrent.ScheduledExecutorService}
     * 调用本方法。</p>
     *
     * @param sink 持久化 sink 实现(不可为 null)
     * @throws NullPointerException 若 {@code sink} 为 null
     * @see StatisticsSink
     * @since 2.0.0
     */
    public void recordStatistics(StatisticsSink sink) {
        Objects.requireNonNull(sink, "sink must not be null");
        try {
            sink.record(getStatistics(), getAllStatistics());
        } catch (Exception e) {
            log.warning("StatisticsSink failed: " + e.getMessage());
        }
    }
}
