# CHMRLock

> 基于 `ConcurrentHashMap` + `ReentrantLock` 的细粒度单机锁库。每个 key 独立加锁，支持重入、租约、监控统计。

[![](https://jitpack.io/v/com.gitee.wb04307201/CHMRLock.svg)](https://jitpack.io/#com.gitee.wb04307201/CHMRLock)

## 特性

- **细粒度锁管理**：以 `String` 为 key 加锁，不同 key 互不影响
- **可重入**：基于 `ReentrantLock`，同一线程可多次获取同一把锁
- **租约模式**：可设置 `leaseTime`，到期后通过 `isLocked` 报告锁已释放（需原线程主动 `unlock`）
- **超时控制**：`tryLock` 支持自定义等待时长
- **可观测**：`MonitorMetrics` 全局统计（成功率、平均等待时间等）
- **per-key 统计**：`getStatistics(key)` / `getAllStatistics()` 提供每个 key 的细粒度指标（需 `enablePerKeyMetrics=true`）
- **事件监听**：`LockListener` 接口支持监听锁的获取/释放/失败/到期/争用事件
- **指标导出**：`MetricsExporter` SPI + 默认 `JsonMetricsExporter` 实现，支持自定义导出格式（Prometheus / StatsD 等）
- **生命周期安全**：`shutdown()` 幂等，清理线程为守护线程
- **try-with-resources**：`AcquiredLock` 实现 `AutoCloseable`
- **内存安全**：`maxKeys` 配置可限制最大 key 数，防止内存泄漏
- **可配置**：`CHMRLockConfig` 暴露 idleThreshold / cleanupInterval / Clock 等
- **零三方依赖**

## 引入

### JitPack 仓库

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

### 依赖

```xml
<dependency>
    <groupId>com.gitee.wb04307201</groupId>
    <artifactId>CHMRLock</artifactId>
    <version>1.0.1</version>
</dependency>
```

## 快速开始

### 基础用法

```java
CHMRLock lock = new CHMRLock();
try {
    if (lock.tryLock("resource_id")) {
        try {
            doWork();
        } finally {
            lock.unlock("resource_id");
        }
    }
} finally {
    lock.shutdown();
}
```

### try-with-resources

```java
try (CHMRLock lock = new CHMRLock();
     AcquiredLock al = lock.tryAcquire("resource_id").orElseThrow()) {
    doWork();
}  // 自动 unlock 和 shutdown
```

### 租约模式

```java
// 等待最多 1s 拿锁，拿到后 30s 内必须用完，否则 isLocked 报告锁已释放
if (lock.tryLock("resource_id", 1, 30, TimeUnit.SECONDS)) {
    try {
        doWork();
    } finally {
        lock.unlock("resource_id");
    }
}
```

> **注意**：租约到期后 `isLocked` 返回 `false`，但底层 `ReentrantLock` 仍由原持有线程持有，需调用 `unlock` 释放。

### 自定义配置

```java
CHMRLockConfig config = CHMRLockConfig.builder()
    .defaultWaitTime(Duration.ofSeconds(5))
    .idleThreshold(Duration.ofMinutes(10))
    .cleanupInterval(Duration.ofSeconds(30))
    .maxKeys(10_000)
    .enablePerKeyMetrics(true)   // 开启 per-key 统计（默认 false）
    .build();
CHMRLock lock = new CHMRLock(config);
```

## API 概览

| 方法 | 说明 |
|------|------|
| `tryLock(key)` / `tryLock(key, waitTime, unit)` | 非阻塞获取 |
| `tryLock(key, waitTime, leaseTime, unit)` | 带租约的获取 |
| `tryAcquire(key)` | 返回 `Optional<AcquiredLock>`，支持 try-with-resources |
| `unlock(key)` | 释放（未持有抛 `LockNotFoundException`） |
| `isLocked(key)` / `isHeldByCurrentThread(key)` / `getHoldCount(key)` | 查询 |
| `getActiveKeys()` | 当前所有 key（不可变视图） |
| `getStatistics()` | 全局指标 |
| `getStatistics(key)` / `getAllStatistics()` | per-key 统计（需 `enablePerKeyMetrics=true`） |
| `registerListener(LockListener)` / `unregisterListener(LockListener)` | 事件监听 |
| `shutdown()` / `close()` | 生命周期终止（幂等） |

## 监控

```java
MonitorMetrics global = lock.getStatistics();
System.out.println("总请求: " + global.getTotalLocks());
System.out.println("成功率: " + global.getSuccessRate());
System.out.println("平均等待: " + global.getAvgWaitTime() + "ms");
```

### per-key 统计

启用 per-key 统计后，可以查看每个 key 的细粒度指标：

```java
CHMRLockConfig config = CHMRLockConfig.builder()
    .enablePerKeyMetrics(true)  // 默认 false，零开销
    .build();
try (CHMRLock lock = new CHMRLock(config)) {
    if (lock.tryLock("resource_id")) {
        try {
            doWork();
        } finally {
            lock.unlock("resource_id");
        }
    }

    // 查看单个 key 的指标
    lock.getStatistics("resource_id").ifPresent(stats -> {
        System.out.println("获取次数: " + stats.acquireCount());
        System.out.println("成功率: " + stats.getSuccessRate());
        System.out.println("平均等待: " + stats.getAvgWaitTimeMillis() + "ms");
    });

    // 查看所有 key
    lock.getAllStatistics().forEach((key, stats) -> {
        System.out.println(key + " -> " + stats);
    });
}
```

> 默认 `enablePerKeyMetrics=false`，所有 per-key 计数器不会触达，零性能开销。

### 事件监听

实现 `LockListener` 接口监听锁生命周期事件：

```java
lock.registerListener(new LockListener() {
    @Override
    public void onLockAcquired(String key, long waitNanos) {
        System.out.println("acquired: " + key);
    }
    @Override
    public void onLockFailed(String key, long waitNanos, String reason) {
        System.err.println("failed: " + key + " (" + reason + ")");
    }
});
```

5 个事件：`onLockAcquired` / `onLockReleased` / `onLockFailed` / `onLockExpired` / `onLockContended`。所有方法默认 no-op，实现类按需覆盖。监听器抛出的异常会被吞掉，不会影响锁功能。

### 指标导出

`MetricsExporter` SPI 用于把指标导出到外部系统：

```java
// 使用默认的 JSON 导出器（输出到 System.out）
JsonMetricsExporter exporter = new JsonMetricsExporter();
// ... Task 38 will add CHMRLock.exportMetrics(exporter); for now:
exporter.export(lock.getStatistics(), lock.getAllStatistics());
```

输出格式：

```json
{
  "global": {
    "totalLocks": 10,
    "successLocks": 9,
    "failedLocks": 1,
    "totalWaitTime": 1500,
    "successRate": 0.9,
    "avgWaitTime": 150.0
  },
  "perKey": {
    "resource_id": {
      "acquireCount": 10,
      "successCount": 9,
      ...
    }
  }
}
```

可以自定义实现（对接 Prometheus / StatsD / InfluxDB 等）。

## 行为约定

- **非公平锁**：默认使用 `ReentrantLock(false)`
- **清理线程**：默认守护线程，命名 `chmrlock-cleanup-N`，可配置清理间隔
- **租约到期**：通过 `isLocked` 报告锁已释放，原线程需主动 `unlock`
- **`shutdown()` 幂等**：可重复调用
- **跨线程 `unlock` 抛 `IllegalMonitorStateException`**：调用方需保证锁由当前线程释放
- **`unlock` 未知 key 抛 `LockNotFoundException`**：防止误用掩盖 bug
- **`maxKeys` 限制同时持有的 key 数**：超过后 `tryLock` 返回 `false`
- **per-key 统计默认关闭**：`enablePerKeyMetrics=false` 时所有 per-key 计数操作零开销
- **事件监听器异常隔离**：监听器抛异常会被捕获并忽略，不影响锁功能
- **租约到期事件**：由于 `ReentrantLock` 不支持跨线程强制释放，`onLockExpired` 触发后租约状态已清理，但底层锁可能仍由原持有线程持有 — 需通过 `isLocked` 验证或显式 `unlock`

## 注意事项

1. **`tryLock` 成功后必须 `unlock`**：建议使用 `try-with-resources` 或 `finally` 块
2. **合理设计 key**：避免动态生成的无界 key（用 `maxKeys` 兜底）
3. **租约时长要 > 业务最长处理时间**：留 2-3 倍余量
4. **记得 `shutdown()`**：不调用也能让 JVM 退出（守护线程），但显式调用更干净
5. **本库仅用于单 JVM 内**：跨进程需自行实现分布式锁

## License

Apache 2.0
