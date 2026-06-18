# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CHMRLock is a small Java library that provides a per-key distributed-style lock built on top of `java.util.concurrent.ConcurrentHashMap` and `ReentrantLock`. It supports:

- Fine-grained locking keyed by `String` (different keys are independent)
- Reentrancy (inherited from `ReentrantLock`)
- Per-call wait timeout via `tryLock(waitTime, timeUnit)`
- Statistics via `MonitorMetrics` (total/success/failed counts, total wait time, success rate, avg wait time)
- A background cleanup thread that removes `LockEntry` instances that have been idle (not held) for more than 5 minutes

The artifact is published to Maven Central via Sonatype Central (`io.github.wb04307201:CHMRLock`), with GitHub Actions `.github/workflows/publish.yml` triggered on release. The Maven `groupId`/`artifactId` are `io.github.wb04307201:CHMRLock` (version `2.0.0`).

## Build & Test Commands

This is a plain Maven project — there is no Maven wrapper, so a system `mvn` on the PATH is required.

```bash
mvn compile          # Compile main sources
mvn test             # Run all tests (uses JUnit 5 / Jupiter)
mvn package          # Build the jar into target/
mvn install          # Install to local ~/.m2 repository
mvn clean            # Remove the target/ directory
```

Run a single test class:

```bash
mvn test -Dtest=CHMRLockTest
```

Run a single test method:

```bash
mvn test -Dtest=CHMRLockTest#testHighConcurrencyLockRequests
mvn test -Dtest=CHMRLockTest#testMultipleKeysManagement
mvn test -Dtest=CHMRLockTest#testShutdownResourceCleanup
mvn test -Dtest=CHMRLockTest#testLockFairness
```

Java target is 17 (`maven.compiler.source` / `maven.compiler.target` in `pom.xml`).

## Source Layout

All code lives under `src/main/java/cn/wubo/lock/`. The package is small enough to read in full:

- `CHMRLock.java` — public facade. Holds the `ConcurrentHashMap<String, LockEntry>`, the `AtomicLong` statistics counters, the `ScheduledExecutorService` cleanup thread, and the default wait time (3 seconds). Exposes `tryLock`, `unlock`, `shutdown`, `getStatistics`.
- `LockEntry.java` — package-private holder for a single `ReentrantLock` plus a `volatile long lastAcquireTime` (no setter is exposed; updated directly by `CHMRLock` because it's in the same package).
- `MonitorMetrics.java` — immutable POJO computed from the four counters. Computes `getSuccessRate()` and `getAvgWaitTime()` on demand; no caching.

Tests live at `src/test/java/cn/wubo/lock/CHMRLockTest.java` (JUnit 5).

## Key Architectural Details

**Per-key lock map.** `tryLock(key, ...)` uses `lockMap.computeIfAbsent(key, k -> new LockEntry())` to lazily create the entry. `unlock(key)` reads via `lockMap.get(key)` and calls `lockEntry.lock.unlock()` only if the entry exists — calling `unlock` for a never-locked key is a silent no-op.

**Statistics accounting.** `totalLocks` is incremented unconditionally before the `tryLock` call. `successLocks` / `failedLocks` are incremented in their respective branches (the `InterruptedException` catch counts as failure). `totalWaitTime` is accumulated in `finally` from the elapsed wall-clock time, so it includes the time spent waiting even when the lock was not acquired.

**Background cleanup.** A single `ScheduledExecutorService` runs `scheduleAtFixedRate` at 1-second initial delay / 1-second period. Each tick walks `lockMap.entrySet().removeIf(...)` and removes entries where the `ReentrantLock` is not held AND `currentTime - lastAcquireTime > 5 minutes`. The thread is created in every constructor; `shutdown()` must be called to terminate it (tests do this in `@AfterEach`).

**No fairness.** `ReentrantLock` is constructed with the default (non-fair) policy. `testLockFairness` does not assert ordering — it only checks that at least one waiter eventually acquires.

**tryLock overloads.** Three forms exist: `tryLock(String)`, `tryLock(String, long, TimeUnit)`, and `tryLock(String, long)` (milliseconds). The single-argument form uses the `defaultWaitTime` set in the constructor (3 seconds by default). `unlock` is the only release path — there is no `lockInterruptibly` or `lock()`-style blocking API.

**Concurrency safety for `lastAcquireTime`.** It is `volatile` and only written after a successful `tryLock`. The cleanup thread reads it without synchronization; the `volatile` provides the happens-before edge needed for the cleanup scan to see a recent successful acquisition.

## Conventions for Changes

- Keep the package as `cn.wubo.lock` — the JitPack-published artifact name depends on it.
- Any new public method on `CHMRLock` should also be exercised in `CHMRLockTest`; the existing tests use `CountDownLatch` + a fixed-size `ExecutorService` + `lockManager.shutdown()` in `@AfterEach`.
- Statistics are an evolving contract (`MonitorMetrics` is part of the public API). Adding a counter means updating both the `AtomicLong` field, the constructor call in `getStatistics()`, and the `MonitorMetrics` constructor + getters.
- The 5-minute idle threshold and 1-second cleanup cadence in `startCleanupThread` are hard-coded magic numbers; change both consistently if a configurable value is introduced.
