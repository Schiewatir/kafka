/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 */
package org.apache.iceberg.spark.stats;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.stats.PuffinColumnStatsConverter.SparkColumnStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drop-in replacement for the statistics-resolution block inside
 * {@code SparkScan.estimateStatistics(Snapshot)}.
 *
 * <h2>Usage (inside SparkScan)</h2>
 *
 * <pre>{@code
 * // BEFORE (buggy — exact match only, returns no stats after any new append):
 * Optional<StatisticsFile> statsFile =
 *     table.statisticsFiles().stream()
 *         .filter(f -> f.snapshotId() == snapshot.snapshotId())
 *         .findFirst();
 *
 * // AFTER (fixed — walks ancestry, tolerates stale-but-valid stats):
 * Map<String, SparkColumnStat> colStats =
 *     SparkStatsIntegration.resolveColumnStats(table, snapshot);
 * }</pre>
 *
 * The returned map can be consumed directly when constructing Spark's {@code ColumnStat}
 * objects (via {@code CatalogColumnStat} or the equivalent internal Spark API).
 */
public final class SparkStatsIntegration {

  private static final Logger LOG = LoggerFactory.getLogger(SparkStatsIntegration.class);

  /**
   * Max ancestor depth before we consider statistics too stale to be useful.
   *
   * <p>A depth of 50 allows roughly 50 appends / overwrites since the last stats computation
   * before we fall back to no-stats behaviour. This is a conservative default; production
   * tables with frequent streaming writes may want to lower this to 10–20 and rely on a
   * more frequent stats refresh schedule instead.
   *
   * <p>Set to {@link StatisticsFileResolver#UNLIMITED_DEPTH} to always use the most recent
   * ancestor stats regardless of how old they are.
   */
  public static final int DEFAULT_MAX_ANCESTOR_DEPTH = 50;

  private SparkStatsIntegration() {}

  /**
   * Returns per-column NDV statistics derived from the best available Puffin statistics
   * file for {@code snapshot}.
   *
   * @param table the Iceberg table being scanned
   * @param snapshot the snapshot being estimated
   * @return map of Spark column name → {@link SparkColumnStat}; empty if no stats are available
   */
  public static Map<String, SparkColumnStat> resolveColumnStats(Table table, Snapshot snapshot) {
    return resolveColumnStats(table, snapshot, DEFAULT_MAX_ANCESTOR_DEPTH);
  }

  /**
   * Same as {@link #resolveColumnStats(Table, Snapshot)} but with an explicit staleness limit.
   */
  public static Map<String, SparkColumnStat> resolveColumnStats(
      Table table, Snapshot snapshot, int maxAncestorDepth) {

    Optional<StatisticsFile> statsFile =
        StatisticsFileResolver.resolve(table, snapshot, maxAncestorDepth);

    if (statsFile.isEmpty()) {
      LOG.debug(
          "No statistics file found for snapshot {} (ancestor depth limit={}); "
              + "CBO will run without NDV estimates",
          snapshot.snapshotId(),
          maxAncestorDepth);
      return Map.of();
    }

    StatisticsFile file = statsFile.get();
    boolean exactMatch = file.snapshotId() == snapshot.snapshotId();
    if (!exactMatch) {
      LOG.debug(
          "Using statistics from ancestor snapshot {} for current snapshot {} "
              + "(stats file: {})",
          file.snapshotId(),
          snapshot.snapshotId(),
          file.path());
    }

    Map<String, SparkColumnStat> colStats =
        PuffinColumnStatsConverter.convert(file, table.schema());

    if (colStats.isEmpty()) {
      LOG.debug(
          "Statistics file {} contained no usable Theta-sketch blobs for the current schema",
          file.path());
    } else {
      LOG.debug(
          "Resolved NDV statistics for {} column(s) from snapshot {} (exact={})",
          colStats.size(),
          file.snapshotId(),
          exactMatch);
    }

    return colStats;
  }
}
