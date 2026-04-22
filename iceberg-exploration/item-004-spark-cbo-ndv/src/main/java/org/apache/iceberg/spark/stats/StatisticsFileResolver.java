/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.stats;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;

/**
 * Resolves the best available {@link StatisticsFile} for a given snapshot.
 *
 * <h2>Why this class exists</h2>
 *
 * {@code SparkScan.estimateStatistics} previously did an exact snapshot-ID match to find
 * the statistics file. When the table is written to after {@code compute_table_stats} runs,
 * the new snapshot has no associated statistics file, so the entire column-stats code path
 * is silently skipped and Spark's CBO receives no NDV estimates.
 *
 * <p>This class replaces that lookup with an <em>ancestor walk</em>: it builds the set of
 * snapshot IDs that are ancestors of (or equal to) the requested snapshot, then returns the
 * most recently computed statistics file whose snapshot ID is in that ancestor set.
 *
 * <h2>Correctness guarantee</h2>
 *
 * Statistics computed on an ancestor snapshot are a valid lower-bound estimate for the
 * current snapshot: they may be slightly stale if rows were added since the stats were
 * computed, but they are never incorrect from a CBO correctness standpoint. Spark already
 * treats all statistics as estimates, so returning ancestor stats is strictly better than
 * returning no stats at all.
 *
 * <h2>Staleness bound</h2>
 *
 * Callers can optionally supply a {@code maxAncestorDepth} to limit how far back the walk
 * goes. A depth of 0 means "exact match only" (preserving the old behaviour). The default
 * of {@link #UNLIMITED_DEPTH} follows the full chain.
 */
public final class StatisticsFileResolver {

  public static final int UNLIMITED_DEPTH = -1;

  private StatisticsFileResolver() {}

  /**
   * Returns the most-recently-computed statistics file that is valid for {@code snapshot},
   * searching up to {@code maxAncestorDepth} generations back.
   *
   * @param table the Iceberg table
   * @param snapshot the snapshot for which statistics are needed
   * @param maxAncestorDepth how many parent hops to follow; use {@link #UNLIMITED_DEPTH}
   *     to follow the full chain
   * @return the best matching {@link StatisticsFile}, or {@link Optional#empty()} if none
   */
  public static Optional<StatisticsFile> resolve(
      Table table, Snapshot snapshot, int maxAncestorDepth) {

    if (table.statisticsFiles().isEmpty()) {
      return Optional.empty();
    }

    // Index all known statistics files by snapshot ID for O(1) lookup during the walk.
    Map<Long, StatisticsFile> statsBySnapshotId =
        table.statisticsFiles().stream()
            .collect(
                Collectors.toMap(
                    StatisticsFile::snapshotId,
                    Function.identity(),
                    // If multiple stats files exist for the same snapshot, prefer the
                    // one with the highest sequence number (most recent computation).
                    (a, b) ->
                        a.fileSequenceNumber() >= b.fileSequenceNumber() ? a : b));

    // Walk backwards through the snapshot ancestry collecting IDs we're willing to
    // accept, stopping at maxAncestorDepth.
    Snapshot cursor = snapshot;
    int depth = 0;

    while (cursor != null) {
      StatisticsFile candidate = statsBySnapshotId.get(cursor.snapshotId());
      if (candidate != null) {
        return Optional.of(candidate);
      }

      if (maxAncestorDepth != UNLIMITED_DEPTH && depth >= maxAncestorDepth) {
        break;
      }

      Long parentId = cursor.parentId();
      cursor = (parentId != null) ? table.snapshot(parentId) : null;
      depth++;
    }

    return Optional.empty();
  }

  /**
   * Convenience overload that follows the full ancestry chain.
   */
  public static Optional<StatisticsFile> resolve(Table table, Snapshot snapshot) {
    return resolve(table, snapshot, UNLIMITED_DEPTH);
  }

  /**
   * Returns all ancestor snapshot IDs (inclusive) up to {@code maxDepth} hops back.
   * Useful for diagnostics and tests.
   */
  public static Set<Long> ancestorSnapshotIds(Table table, Snapshot snapshot, int maxDepth) {
    java.util.Set<Long> ids = new java.util.LinkedHashSet<>();
    Snapshot cursor = snapshot;
    int depth = 0;

    while (cursor != null && (maxDepth == UNLIMITED_DEPTH || depth <= maxDepth)) {
      ids.add(cursor.snapshotId());
      Long parentId = cursor.parentId();
      cursor = (parentId != null) ? table.snapshot(parentId) : null;
      depth++;
    }

    return ids;
  }
}
