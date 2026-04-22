/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 */
package org.apache.iceberg.spark.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StatisticsFileResolver}.
 *
 * <p>These tests deliberately do not spin up a real Iceberg table; they mock the minimal
 * {@link Table} surface needed to exercise the ancestor-walk logic.
 */
class StatisticsFileResolverTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Table mockTable(List<StatisticsFile> statsFiles, Snapshot... snapshots) {
    Table table = mock(Table.class);
    when(table.statisticsFiles()).thenReturn(statsFiles);
    for (Snapshot snap : snapshots) {
      when(table.snapshot(snap.snapshotId())).thenReturn(snap);
    }
    return table;
  }

  private static Snapshot mockSnapshot(long snapshotId, Long parentId) {
    Snapshot snap = mock(Snapshot.class);
    when(snap.snapshotId()).thenReturn(snapshotId);
    when(snap.parentId()).thenReturn(parentId);
    return snap;
  }

  private static StatisticsFile statsFile(long snapshotId) {
    return new GenericStatisticsFile(
        snapshotId, "/path/to/stats-" + snapshotId + ".puffin", 1024L, 0L, List.of());
  }

  // ---------------------------------------------------------------------------
  // Test: exact match
  // ---------------------------------------------------------------------------

  @Test
  void exactMatchReturnsFile() {
    Snapshot s1 = mockSnapshot(1L, null);
    StatisticsFile f1 = statsFile(1L);
    Table table = mockTable(List.of(f1), s1);

    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s1);

    assertThat(result).isPresent().contains(f1);
  }

  // ---------------------------------------------------------------------------
  // Test: THE BUG — new snapshot after stats were computed, no exact match
  // ---------------------------------------------------------------------------

  @Test
  void ancestorMatchReturnsFileWhenCurrentSnapshotHasNoStats() {
    // S1 (has stats) → S2 (no stats, new append after stats were computed)
    Snapshot s1 = mockSnapshot(1L, null);
    Snapshot s2 = mockSnapshot(2L, 1L);
    StatisticsFile f1 = statsFile(1L);
    Table table = mockTable(List.of(f1), s1, s2);

    // Old behaviour: exact match on s2.snapshotId() == 2L → finds nothing
    // New behaviour: walks to parent s1, finds f1
    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s2);

    assertThat(result)
        .as("should find stats from parent snapshot when current snapshot has none")
        .isPresent()
        .contains(f1);
  }

  @Test
  void ancestorMatchWalksMultipleGenerations() {
    // S1 (has stats) → S2 → S3 → S4
    Snapshot s1 = mockSnapshot(1L, null);
    Snapshot s2 = mockSnapshot(2L, 1L);
    Snapshot s3 = mockSnapshot(3L, 2L);
    Snapshot s4 = mockSnapshot(4L, 3L);
    StatisticsFile f1 = statsFile(1L);
    Table table = mockTable(List.of(f1), s1, s2, s3, s4);

    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s4);

    assertThat(result).isPresent().contains(f1);
  }

  // ---------------------------------------------------------------------------
  // Test: depth limit respected
  // ---------------------------------------------------------------------------

  @Test
  void depthLimitPreventsStaleAncestorLookup() {
    // S1 (has stats) → S2 → S3
    // With maxDepth=1, walking from S3 should only visit S3 and S2, not S1.
    Snapshot s1 = mockSnapshot(1L, null);
    Snapshot s2 = mockSnapshot(2L, 1L);
    Snapshot s3 = mockSnapshot(3L, 2L);
    StatisticsFile f1 = statsFile(1L);
    Table table = mockTable(List.of(f1), s1, s2, s3);

    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s3, 1);

    assertThat(result)
        .as("depth limit of 1 should not reach ancestor 2 hops back")
        .isEmpty();
  }

  @Test
  void depthLimitZeroMeansExactMatchOnly() {
    Snapshot s1 = mockSnapshot(1L, null);
    Snapshot s2 = mockSnapshot(2L, 1L);
    StatisticsFile f1 = statsFile(1L);
    Table table = mockTable(List.of(f1), s1, s2);

    Optional<StatisticsFile> exactOnS1 = StatisticsFileResolver.resolve(table, s1, 0);
    Optional<StatisticsFile> exactOnS2 = StatisticsFileResolver.resolve(table, s2, 0);

    assertThat(exactOnS1).isPresent().contains(f1);
    assertThat(exactOnS2).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test: no statistics files at all
  // ---------------------------------------------------------------------------

  @Test
  void emptyStatisticsFilesReturnsEmpty() {
    Snapshot s1 = mockSnapshot(1L, null);
    Table table = mockTable(List.of(), s1);

    assertThat(StatisticsFileResolver.resolve(table, s1)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test: most-recent stats file wins when multiple exist for ancestors
  // ---------------------------------------------------------------------------

  @Test
  void mostRecentStatsFileWinsOnSameSnapshot() {
    // Two stats files for the same snapshot ID (e.g. stats recomputed twice) →
    // the one with the higher sequence number wins.
    Snapshot s1 = mockSnapshot(1L, null);

    StatisticsFile old = new GenericStatisticsFile(1L, "/old.puffin", 512L, 0L, List.of());
    StatisticsFile recent = new GenericStatisticsFile(1L, "/recent.puffin", 1024L, 1L, List.of());

    Table table = mockTable(List.of(old, recent), s1);

    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s1);

    assertThat(result).isPresent();
    assertThat(result.get().path()).isEqualTo("/recent.puffin");
  }

  @Test
  void exactMatchPrefersNewerSnapshotStatOverDistantAncestor() {
    // S1 (old stats) → S2 (fresh stats) → S3 (no stats)
    // Walking from S3 should find S2's stats first.
    Snapshot s1 = mockSnapshot(1L, null);
    Snapshot s2 = mockSnapshot(2L, 1L);
    Snapshot s3 = mockSnapshot(3L, 2L);
    StatisticsFile f1 = statsFile(1L);
    StatisticsFile f2 = statsFile(2L);
    Table table = mockTable(List.of(f1, f2), s1, s2, s3);

    Optional<StatisticsFile> result = StatisticsFileResolver.resolve(table, s3);

    assertThat(result).isPresent().contains(f2);
  }

  // ---------------------------------------------------------------------------
  // Test: ancestorSnapshotIds helper
  // ---------------------------------------------------------------------------

  @Test
  void ancestorSnapshotIdsReturnsCorrectSet() {
    Snapshot s1 = mockSnapshot(10L, null);
    Snapshot s2 = mockSnapshot(20L, 10L);
    Snapshot s3 = mockSnapshot(30L, 20L);
    Table table = mockTable(List.of(), s1, s2, s3);

    assertThat(StatisticsFileResolver.ancestorSnapshotIds(table, s3, StatisticsFileResolver.UNLIMITED_DEPTH))
        .containsExactly(30L, 20L, 10L);

    assertThat(StatisticsFileResolver.ancestorSnapshotIds(table, s3, 1))
        .containsExactly(30L, 20L);
  }
}
