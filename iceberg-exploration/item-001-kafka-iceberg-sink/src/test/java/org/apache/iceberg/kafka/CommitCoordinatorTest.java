/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.iceberg.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class CommitCoordinatorTest {

  private static final long INTERVAL_MS = 60_000L;
  private static final long TARGET_BYTES = 128L * 1024 * 1024; // 128 MB

  // ---------------------------------------------------------------------------
  // Size trigger
  // ---------------------------------------------------------------------------

  @Test
  void sizeThresholdTriggersCommit() {
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, fixedClock(0));

    coordinator.recordBytes(TARGET_BYTES - 1);
    assertThat(coordinator.shouldCommit()).isFalse();

    coordinator.recordBytes(1);
    assertThat(coordinator.shouldCommit()).isTrue();
    assertThat(coordinator.triggerReason()).contains("size trigger");
  }

  @Test
  void resetClearsByteCount() {
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, fixedClock(0));

    coordinator.recordBytes(TARGET_BYTES);
    assertThat(coordinator.shouldCommit()).isTrue();

    coordinator.reset();
    assertThat(coordinator.shouldCommit()).isFalse();
    assertThat(coordinator.bufferedBytes()).isZero();
  }

  // ---------------------------------------------------------------------------
  // Time trigger
  // ---------------------------------------------------------------------------

  @Test
  void timeThresholdTriggersCommitWhenNoBytesBuffered() {
    MutableClock clock = new MutableClock(0);
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, clock);

    assertThat(coordinator.shouldCommit()).isFalse();

    clock.advanceMs(INTERVAL_MS);
    assertThat(coordinator.shouldCommit()).isTrue();
    assertThat(coordinator.triggerReason()).contains("time trigger");
  }

  @Test
  void resetResetsTimer() {
    MutableClock clock = new MutableClock(0);
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, clock);

    clock.advanceMs(INTERVAL_MS);
    assertThat(coordinator.shouldCommit()).isTrue();

    coordinator.reset();
    // Immediately after reset the timer should not fire
    assertThat(coordinator.shouldCommit()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Whichever-comes-first semantics
  // ---------------------------------------------------------------------------

  @Test
  void sizeWinsOverTimerWhenSizeReachedFirst() {
    MutableClock clock = new MutableClock(0);
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, clock);

    // Fill bytes before time elapses
    coordinator.recordBytes(TARGET_BYTES);
    clock.advanceMs(INTERVAL_MS / 2);

    assertThat(coordinator.shouldCommit()).isTrue();
    assertThat(coordinator.triggerReason()).contains("size trigger");
  }

  @Test
  void elapsedSinceLastCommitReflectsActualTime() {
    MutableClock clock = new MutableClock(0);
    CommitCoordinator coordinator = new CommitCoordinator(INTERVAL_MS, TARGET_BYTES, clock);

    clock.advanceMs(5_000);
    assertThat(coordinator.elapsedSinceLastCommit().toMillis()).isEqualTo(5_000);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Clock fixedClock(long epochMs) {
    return Clock.fixed(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
  }

  /** A {@link Clock} whose current time can be advanced programmatically. */
  static class MutableClock extends Clock {
    private long epochMs;

    MutableClock(long startEpochMs) {
      this.epochMs = startEpochMs;
    }

    void advanceMs(long ms) {
      this.epochMs += ms;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(epochMs);
    }
  }
}
