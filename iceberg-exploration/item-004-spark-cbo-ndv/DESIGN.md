# ITEM-004: Spark CBO / Puffin NDV Statistics — End-to-End Fix

## Problem statement

`SparkScan.estimateStatistics(Snapshot)` does an **exact snapshot-ID match** when looking
up the statistics file associated with the current read. When you:

1. Create a table
2. Append initial data (snapshot S1)
3. Run `CALL system.compute_table_stats(...)` → writes a `StatisticsFile` keyed to S1
4. Append more rows (snapshot S2)
5. Run a query

…Spark calls `estimateStatistics(S2)`, finds no statistics file with `snapshot_id == S2`,
and silently falls through with zero column-level stats. The CBO log emits:

```
[CBO] No statistics for order_id#150L
[CBO] No statistics for customer#152
```

No GET call is ever made to the `.stat` file in S3/GCS/ADLS, confirming the retrieval
path is never entered.

**This is confirmed by issue apache/iceberg#13374 (opened June 2025, Iceberg 1.9.0 / Spark 3.5.4).**

---

## Root-cause trace

```
SparkScan.estimateStatistics(Snapshot snapshot)
  └─ StatisticsUtil.findValidStatisticsFile(table, snapshot.snapshotId())   ← BUG HERE
       └─ table.statisticsFiles()
            .stream()
            .filter(f -> f.snapshotId() == snapshot.snapshotId())   // exact match only
            .findFirst()
```

When no exact match exists the stream returns `Optional.empty()`, the method returns
`null`, and the caller skips all column-stat construction.

---

## Fix

Replace exact-match lookup with a **"most-recent valid ancestor"** strategy:

> Walk backwards through the snapshot's ancestry (using `table.snapshot(id).parentId()`)
> and return the first statistics file whose `snapshot_id` matches any ancestor, including
> the snapshot itself. This is safe because statistics are monotonically computed on a
> point-in-time snapshot; they remain a valid lower-bound estimate for any descendant
> snapshot (they may be slightly stale but never incorrect from a correctness standpoint,
> and Spark's CBO already treats all statistics as estimates).

An additional gap: even when the statistics file IS found, the field-ID → Spark column-name
mapping uses the schema at the time the stats were computed, which may differ from the
current schema if columns were renamed or reordered since then. The fix uses the table's
**current schema field IDs** (which are stable across renames) to correctly correlate.

---

## Files in this prototype

| File | Purpose |
|------|---------|
| `StatisticsFileResolver.java` | Ancestor-walk statistics file lookup (the core fix) |
| `PuffinColumnStatsConverter.java` | Converts `BlobMetadata` Theta sketch NDV → Spark `CatalogColumnStat` |
| `SparkStatsIntegration.java` | Wires both together; drop-in replacement for the buggy path |
| `StatisticsFileResolverTest.java` | Unit tests: exact match, ancestor match, no stats, branched history |
| `PuffinColumnStatsConverterTest.java` | Unit tests: ndv present, ndv absent, wrong blob type, multi-column |

---

## How to contribute upstream

1. Reproduce the bug with the `SparkCBONdvReproducerIT` integration test (included).
2. Apply `StatisticsFileResolver` and `PuffinColumnStatsConverter` into:
   - `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkScan.java`
   - `spark/v3.4/spark/src/main/java/org/apache/iceberg/spark/SparkScan.java`
3. Add the integration test to `spark/v3.5/spark/src/test/...`.
4. Open a PR against `apache/iceberg` main, referencing issue #13374.
