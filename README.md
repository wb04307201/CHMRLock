# CHMLock - 基于ConcurrentHashMap和 ReentrantLock的锁实现

> 一个基于Java `ConcurrentHashMap` 和 `ReentrantLock` 的锁实现。它提供了锁的获取、释放、自动清理过期锁以及监控指标统计等功能。

[![](https://jitpack.io/v/com.gitee.wb04307201/CHMRLock.svg)](https://jitpack.io/#com.gitee.wb04307201/CHMRLock)

## 特性

- **细粒度锁管理**：基于key进行加锁，不同key之间互不影响
- **可重入性**：基于`ReentrantLock`实现，支持同一线程多次获取同一把锁
- **超时控制**：支持设置锁等待超时时间，避免无限等待
- **监控统计**：提供详细的锁获取统计信息，包括成功率、等待时间等
- **高并发安全**：使用`ConcurrentHashMap`存储锁，保证线程安全

---

## 引入

### 增加 JitPack 仓库
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```
### 添加依赖
```xml
<dependency>
    <groupId>com.gitee.wb04307201</groupId>
    <artifactId>CHMRLock</artifactId>
    <version>1.0.0</version>
</dependency>
```


## 使用示例

```java
public class Example {
    private static CHMRLock lock = new CHMRLock();

    public static void main(String[] args) {
        String key = "shared_resource";

        if (lock.tryLock(key)) {
            try {
                // 执行需要同步的业务逻辑
                doSomething();
            } finally {
                lock.unlock(key);
            }
        } else {
            System.out.println("获取锁失败");
        }

        // 查看统计信息
        MonitorMetrics metrics = lock.getStatistics();
        System.out.println("总请求数: " + metrics.getTotalLocks());
        System.out.println("成功率: " + metrics.getSuccessRate());
        System.out.println("平均等待时间: " + metrics.getAvgWaitTime() + "ms");
    }

    private static void doSomething() {
        // 业务逻辑
    }
}
```

## MonitorMetrics
监控指标类，提供锁获取的统计信息：
- 总请求数
- 成功获取数
- 获取失败数
- 总等待时间
- 成功率
- 平均等待时间

## 注意事项

1. **必须配对使用**：每次调用[tryLock](src/main/java/cn/wubo/lock/CHMRLock.java#L31-L33)成功后，必须在适当的时候调用[unlock](src/main/java/cn/wubo/lock/CHMRLock.java#L66-L69)释放锁
2. **异常处理**：建议在`finally`块中释放锁，确保即使发生异常也能正确释放
3. **key的设计**：合理设计锁的key，避免过多的锁对象占用内存
4. **超时设置**：根据业务场景合理设置等待超时时间，避免线程长时间阻塞