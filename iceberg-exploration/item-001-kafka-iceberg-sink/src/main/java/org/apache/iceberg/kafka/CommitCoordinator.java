/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.iceberg.kafka;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decides when a micro-batch should be committed to Iceberg.
 *
 * <h2>Adaptive trigger logic</h2>
 *
 * A commit fires when <em>either</em> condition is satisfied:
 * <ol>
 *   <li>The time elapsed since the last commit exceeds {@code commitIntervalMs}.</li>
 *   <li>The total uncompressed bytes buffered across all partition writers exceeds
 *       {@code targetFileSizeBytes}.</li>
 * </ol>
 *
 * This adapts automatically to traffic: under sustained load the size trigger fires
 * frequently, keeping individual Iceberg data files close to the target size.
 * Under low load the time trigger fires, bounding end-to-end latency.
 *
 * <h2>Thread safety</h2>
 *
 * {@link #recordBytes(long)} is called from the consumer thread; {@link #shouldCommit()}
 * and {@link #reset()} are called from the commit coordinator thread. The byte counter
 * uses an {@link AtomicLong} to avoid coordination overhead on the hot path.
 */
public final class CommitCoordinator {

  private final long commitIntervalMs;
  private final long targetFileSizeBytes;
  private final Clock clock;

  private volatile Instant lastCommitTime;
  private final AtomicLong bufferedBytes = new AtomicLong(0);

  public CommitCoordinator(long commitIntervalMs, long targetFileSizeBytes) {
    this(commitIntervalMs, targetFileSizeBytes, Clock.systemUTC());
  }

  CommitCoordinator(long commitIntervalMs, long targetFileSizeBytes, Clock clock) {
    this.commitIntervalMs = commitIntervalMs;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.clock = clock;
    this.lastCommitTime = clock.instant();
  }

  /**
   * Record that {@code bytes} have been buffered since the last commit.
   * Called on the hot consumer path; must be fast.
   */
  public void recordBytes(long bytes) {
    bufferedBytes.addAndGet(bytes);
  }

  /**
   * Returns {@code true} if a commit should be issued now.
   */
  public boolean shouldCommit() {
    return sizeThresholdExceeded() || timeThresholdExceeded();
  }

  /**
   * Returns a human-readable description of why a commit was triggered.
   * Useful for debug logging.
   */
  public String triggerReason() {
    if (sizeThresholdExceeded()) {
      return String.format(
          "size trigger: buffered=%d bytes >= target=%d bytes",
          bufferedBytes.get(), targetFileSizeBytes);
    }
    if (timeThresholdExceeded()) {
      return String.format(
          "time trigger: elapsed=%s >= interval=%dms",
          Duration.between(lastCommitTime, clock.instant()), commitIntervalMs);
    }
    return "no trigger";
  }

  /**
   * Resets state after a successful commit. Must be called from the commit thread.
   */
  public void reset() {
    bufferedBytes.set(0);
    lastCommitTime = clock.instant();
  }

  /** Returns the number of bytes buffered since the last reset. */
  public long bufferedBytes() {
    return bufferedBytes.get();
  }

  /** Returns the elapsed time since the last commit. */
  public Duration elapsedSinceLastCommit() {
    return Duration.between(lastCommitTime, clock.instant());
  }

  private boolean sizeThresholdExceeded() {
    return bufferedBytes.get() >= targetFileSizeBytes;
  }

  private boolean timeThresholdExceeded() {
    return Duration.between(lastCommitTime, clock.instant()).toMillis() >= commitIntervalMs;
  }
}
