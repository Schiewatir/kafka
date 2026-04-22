# Apache Iceberg Exploration Backlog

> Maintained by Alex Schiewe | Fork: schiewatir/kafka | Branch: claude/iceberg-exploration-9qwpu
> Last updated: 2026-04-22

This document is a living backlog of ideas I want to explore, prototype, or drive toward
upstream contribution. Each item includes a problem statement, community value proposition,
rough technical approach, complexity rating, risks, and an upstream-contribution assessment.

Items are grouped thematically and ordered by priority within each group.

---

## Group 1 — Streaming & Kafka Integration

### ITEM-001 · Kafka-Native Iceberg Sink with Adaptive Micro-Batching

**Problem / Opportunity**
Writing Kafka events to Iceberg today requires either Flink (heavyweight, stateful), Kafka
Connect (limited transactional guarantees), or a hand-rolled consumer. The fundamental tension
is that Kafka wants continuous, low-latency writes, while Iceberg performs best with large,
infrequent commits. No first-class, standalone sink exists that intelligently adapts its
commit cadence to the actual write load.

**Why it matters**
The 2026 streaming-lakehouse trend means more teams are trying to land Kafka data in Iceberg
without adopting a full stream-processing framework. A lightweight, operationally simple sink
that automatically trades off latency against file size would unblock a large class of users.

**Technical approach**
- Build a standalone Kafka consumer library (Java + Python) that buffers records in memory or
  local spill and issues Iceberg commits when either a time threshold or a byte-size threshold
  is crossed — whichever comes first.
- Use Iceberg's `FastAppend` API to avoid rewriting existing data.
- Implement idempotent writes by embedding Kafka partition + offset metadata into Iceberg
  custom properties on each data file, enabling exactly-once delivery via offset replay
  detection at restart.
- Expose a simple metrics endpoint (Prometheus-compatible) reporting lag, commit rate, and
  average file size.

**Complexity:** Medium

**Risks / Dependencies**
- Exactly-once guarantees require careful coordination with Iceberg's optimistic concurrency;
  concurrent writers to the same partition branch need conflict resolution logic.
- The Python variant will depend on PyIceberg's `FastAppend` support reaching parity with Java.
- Small-file accumulation still needs a compaction trigger; this item does not replace
  compaction but should integrate with ITEM-002.

**Upstream contribution potential**
High. After a working prototype, this could be proposed to the `iceberg-python` repo and as a
standalone module in the main Java repo. A design doc on the dev list would be the first step.

---

### ITEM-002 · Autonomous Compaction Service with Pluggable Scheduling Policies

**Problem / Opportunity**
Iceberg's `RewriteDataFiles` action exists, but there is no first-class, always-on compaction
daemon. Teams running streaming ingestion (see ITEM-001) quickly accumulate thousands of small
files per partition, degrading scan performance by orders of magnitude. Today, users must wire
up their own cron jobs, Airflow DAGs, or Spark jobs — all of which are fragile and hard to tune.

**Why it matters**
This is one of the most commonly cited operational pain points in community surveys and Slack.
PyIceberg and iceberg-rust lack any compaction tooling whatsoever, even though Java has the
`SparkActions` wrappers. A language-agnostic compaction service would significantly lower the
operational bar.

**Technical approach**
- Design a `CompactionPolicy` interface with two built-in implementations:
  - `SizeBasedPolicy`: trigger when average file size drops below a configurable threshold
    (default 128 MB).
  - `AgeBasedPolicy`: trigger when the oldest file in a partition exceeds a configurable TTL.
- Implement a long-running daemon process (in Java initially, with a Python port) that polls
  table snapshots, evaluates policies, and issues `RewriteDataFiles` actions.
- Expose policy configuration via Iceberg table properties, so the daemon picks up changes
  without restart.
- Integrate with the Iceberg REST Catalog's planned `scan-planning` endpoint so the daemon
  can query file metadata without scanning the object store directly.

**Complexity:** Medium-High

**Risks / Dependencies**
- Requires careful locking to avoid compaction races with active writers — Iceberg's MVCC
  helps but concurrent compaction + append can still hit snapshot conflicts.
- Depends on the REST Catalog scan-planning endpoint (ITEM-005) for efficient metadata queries.

**Upstream contribution potential**
High. The Java daemon could be contributed to `iceberg` core; the Python port to
`iceberg-python`. Propose via an Iceberg Improvement Proposal (IIP) on the dev list.

---

## Group 2 — Multi-Table Transactions

### ITEM-003 · Interactive Commit API Prototype (Cross-Table Atomicity)

**Problem / Opportunity**
Iceberg today only supports transactions within a single table. The community has an active
open issue (#10617) and mailing-list threads proposing a "Multi-Table Transaction" capability,
but the design is still being debated (Catalog Commit Sequence Numbers vs. catalog-authored
timestamps). No working prototype exists to validate either approach.

**Why it matters**
Cross-table atomicity is essential for data lakehouse workloads that mirror transactional
database semantics — for example, atomically updating a `fact_orders` table and a
`dim_customers` table together. Without this, teams resort to compensating transactions,
leading to observable intermediate states.

**Technical approach**
- Implement the CSN (Catalog Commit Sequence Number) approach as described in the mailing-list
  proposal: the catalog assigns a monotonically increasing CSN to each commit; a
  multi-table transaction reserves a CSN range and only commits when all participating tables
  can write at their reserved CSN.
- Prototype the catalog-side logic as an extension to the SQLite-backed `SqlCatalog`
  (easiest to iterate on locally), then validate against a REST catalog implementation.
- Write integration tests covering: commit success, partial failure with rollback, concurrent
  conflicting transactions, and reader isolation (readers never see a partial commit).

**Complexity:** High

**Risks / Dependencies**
- The design is still contested upstream; this prototype may need to pivot to the
  "catalog-authored timestamps" alternative if the community converges there.
- Requires catalog-side changes, which creates a versioning surface problem for existing
  catalog implementations that do not want to adopt the new endpoint.
- Reader isolation guarantees depend on query engines respecting the new metadata fields —
  engine adoption will lag specification finalization.

**Upstream contribution potential**
High, but only after community design convergence. This prototype's value is primarily to
generate concrete benchmark data and test cases that can inform the specification debate.

---

## Group 3 — Statistics & Query Planning

### ITEM-004 · Spark CBO / Puffin NDV Statistics — End-to-End Fix

**Problem / Opportunity**
Iceberg's Puffin file format allows storing extended statistics (e.g., Number of Distinct
Values via Theta Sketches) alongside table data. However, a documented open issue (#13374)
confirms that Spark's Cost-Based Optimizer (CBO) does not actually consume these NDV
statistics when performing join reordering. The stats are computed and stored but silently
ignored, causing suboptimal query plans.

**Why it matters**
Join reordering is one of the highest-impact CBO decisions. For star-schema analytics on
large fact tables, choosing the wrong build side or join order can produce 10x–100x slower
plans. Fixing this integration would immediately benefit every Spark + Iceberg user running
analytical workloads without any application-level changes required.

**Technical approach**
- Trace the existing code path: `IcebergSource → TableScan → Statistics → SparkPlan`.
- Identify the precise gap: whether the NDV values are not being surfaced in the
  `Statistics` object that Spark expects, or whether Spark is not calling the right API.
- Implement a fix in the `iceberg-spark` module that properly maps Puffin Theta Sketch
  NDV estimates to Spark's `ColumnStat` structure.
- Add integration tests that verify join order changes when NDV statistics are present vs. absent.

**Complexity:** Medium

**Risks / Dependencies**
- Requires understanding both Iceberg's Puffin/statistics API and Spark's internal
  `Statistics` and `ColumnStat` APIs — two deep codebases.
- The fix must not break non-Iceberg Spark sources.
- Theta Sketch NDV is an estimate; the fix must correctly communicate the confidence
  interval to Spark rather than presenting it as an exact count.

**Upstream contribution potential**
Very High. This is a clear bug with a documented issue. A well-tested PR would likely be
welcomed immediately by the community.

---

### ITEM-005 · REST Catalog Scan-Planning Endpoint — Reference Implementation

**Problem / Opportunity**
The Iceberg REST Catalog specification is expected to gain a `scan-planning` endpoint,
allowing query engines to delegate file-list computation to the catalog rather than doing
it client-side. This is architecturally significant: it enables catalog-managed table
formats (not just Iceberg), multi-engine scan optimization, and pushdown of partition
pruning to the catalog tier. As of early 2026, no open-source reference implementation exists.

**Why it matters**
Without a reference implementation, catalog vendors are left guessing at semantics, and
engine teams have nothing to test against. A clean, well-documented reference server will
accelerate the whole ecosystem's adoption of the new endpoint.

**Technical approach**
- Extend the existing `iceberg-rest-fixture` test server in the Java repo to implement
  the draft scan-planning endpoint.
- Support the full planning lifecycle: `planTable` (returns a planning token), `fetchScanTasks`
  (paginated file list retrieval), and `cancelPlan` (resource cleanup).
- Implement partition pruning, column projection, and predicate pushdown server-side using
  Iceberg's existing `TableScan` API.
- Write a conformance test suite that any catalog implementation can run against.

**Complexity:** Medium-High

**Risks / Dependencies**
- The endpoint specification is not yet finalized; the implementation may need to track
  spec changes closely during development.
- Pagination semantics (cursor-based vs. offset-based) are still being debated upstream.

**Upstream contribution potential**
High. The reference server and conformance suite are exactly the kind of contribution that
benefits every catalog vendor and would be enthusiastically accepted.

---

## Group 4 — Multi-Language Parity

### ITEM-006 · PyIceberg Autonomous Compaction via DuckDB / Arrow

**Problem / Opportunity**
PyIceberg has no compaction capability. Python data teams using PyIceberg for ingestion
quickly accumulate small files with no native remedy. The Java `SparkActions.rewriteDataFiles()`
is unavailable to them, and spinning up a Spark cluster just for compaction is prohibitive.

**Why it matters**
Python is the dominant language in the data science and ML engineering communities. If
PyIceberg cannot compact its own tables, it remains a second-class citizen for production
workloads.

**Technical approach**
- Implement a `compaction` module in PyIceberg that uses DuckDB (already a supported reader)
  as the compute engine for reading and rewriting Parquet files.
- Expose a high-level API: `table.compact(strategy="binpack", target_file_size_bytes=128*1024*1024)`.
- Implement `BinpackStrategy`: group small files into bins that approach the target size, read
  each bin with DuckDB/Arrow, and write a new Parquet file via PyArrow.
- Preserve all Iceberg metadata: partition values, sort order, and column statistics (Puffin).
- Integrate with `SizeBasedPolicy` from ITEM-002 when the daemon lands in Python.

**Complexity:** Medium

**Risks / Dependencies**
- DuckDB version pinning is already a known tension in PyIceberg's optional dependency tree;
  the compaction module must not tighten that pin further.
- Correctness requires careful handling of delete files (positional and equality) — files
  with outstanding deletes must be compacted with delete application, not blindly rewritten.
- V3 deletion vectors must be properly resolved before rewrite (depends on PyIceberg's V3
  roadmap).

**Upstream contribution potential**
High. This would be a major addition to `iceberg-python` and is a frequently requested
feature. A design discussion on the `iceberg-python` dev list would be the right first step.

---

### ITEM-007 · iceberg-rust: V3 Deletion Vector Write Support

**Problem / Opportunity**
The Rust implementation (`apache/iceberg-rust`) can read V3 deletion vectors as of 0.7.0,
but write support is absent. This means Rust-based writers (e.g., DataFusion-powered
pipelines) cannot issue row-level deletes against V3 tables, forcing them to either use
copy-on-write (expensive) or maintain a Java sidecar for delete file generation.

**Why it matters**
Rust is the highest-performance non-JVM Iceberg implementation and is increasingly used for
high-throughput ingestion pipelines. V3 deletion vector write support is essential for
CDC workloads where row-level deletes are the dominant operation.

**Technical approach**
- Implement `DeletionVectorWriter` in `iceberg-rust` that produces Puffin files with a
  Roaring Bitmap encoding of deleted row positions.
- Integrate with the existing `DataFileWriter` pipeline so that a `MergeOnReadWriter` can
  emit paired (data file, deletion vector) outputs atomically.
- Ensure the bitmap encoding matches the V3 spec exactly: file-scoped bitmaps, using
  64-bit row positions, stored in little-endian byte order.
- Add round-trip tests: write a deletion vector with iceberg-rust, read it back with the
  Java reference implementation, and verify correctness.

**Complexity:** Medium-High

**Risks / Dependencies**
- Puffin file serialization in Rust is partially implemented; the DV-specific blob type and
  footer metadata must be extended.
- The Roaring Bitmap crate (`roaring-rs`) is the natural choice but needs evaluation for
  spec compliance.
- Cross-implementation round-trip testing requires a Java test harness alongside the Rust test suite.

**Upstream contribution potential**
High. This directly addresses a known V3 feature gap documented in the Rust repo's issue
tracker and would be a welcome PR from any contributor.

---

## Group 5 — Catalog & Metadata Intelligence

### ITEM-008 · Iceberg Table Health Dashboard (Metadata-Only)

**Problem / Opportunity**
There is no standard way to assess the "health" of an Iceberg table — whether it has too
many small files, stale snapshots, orphan files eating storage, or statistics that are out
of date. Teams either build ad-hoc Spark jobs or rely on vendor-specific monitoring tools.

**Why it matters**
Operational visibility is a prerequisite for maintaining Iceberg tables at scale. A
standard, open-source health check tool would give every team a starting point and would
be especially valuable for teams that cannot afford a commercial lakehouse management product.

**Technical approach**
- Build a `TableHealthReport` API in the Java core (no Spark dependency) that reads only
  table metadata (snapshots, manifests, manifest entries) to compute:
  - `snapshot_count`: total live snapshots.
  - `avg_data_file_size_bytes`: indicator of small-file accumulation.
  - `p50_data_file_size_bytes` and `p95_data_file_size_bytes`.
  - `delete_to_data_file_ratio`: high ratio indicates compaction is overdue.
  - `days_since_last_compaction`: derived from snapshot summary properties.
  - `orphan_file_count_estimate`: files referenced by expired snapshots not yet cleaned up.
  - `statistics_freshness_days`: age of the most recent Puffin statistics file.
- Expose the report as a simple JSON/YAML output from a CLI tool (`iceberg health <table>`).
- Provide recommended remediation actions (e.g., "run compaction", "expire snapshots").

**Complexity:** Low-Medium

**Risks / Dependencies**
- Orphan file detection requires listing the object store, which can be expensive at scale;
  the report should clearly mark this metric as "estimate" and make it opt-in.
- Statistics freshness requires Puffin support, which is engine-dependent.

**Upstream contribution potential**
High. A metadata-only health API with no Spark dependency would be a genuinely useful
addition to `iceberg` core. The CLI wrapper could live in a separate module.

---

### ITEM-009 · Federated Catalog Query Router

**Problem / Opportunity**
Enterprises often have tables spread across multiple Iceberg catalogs (e.g., Polaris for
governed data, Nessie for experimental branches, Glue for AWS-native workloads). Today there
is no standard way to query across them in a single statement; users must manually switch
catalog contexts or maintain duplicate registrations.

**Why it matters**
As catalog fragmentation grows — especially with the rise of Polaris, Gravitino, and Unity
as competing catalog standards — cross-catalog queries become a genuine operational need.
Apache Polaris is experimenting with "federated catalogs" but the implementation is
read-only and Polaris-specific.

**Technical approach**
- Design a `FederatedCatalog` implementation of the Iceberg `Catalog` interface that wraps
  multiple underlying catalogs and routes namespace/table lookups based on a configurable
  prefix mapping (e.g., `prod.` → Polaris REST endpoint, `dev.` → Nessie endpoint).
- Implement transparent credential forwarding so each underlying catalog receives
  appropriate auth tokens.
- Handle namespace collision with a clear precedence rule and surfacing conflicts to the caller.
- Add a dry-run / explain mode that shows which physical catalog each table identifier resolves to.

**Complexity:** Medium

**Risks / Dependencies**
- Auth token management across catalogs is complex; OAuth2 token exchange (RFC 8693) may
  be needed for some combinations.
- Distributed transactions across federated catalogs (see ITEM-003) are out of scope for
  this item; the router is read-path focused initially.

**Upstream contribution potential**
Medium. This may be better positioned as a standalone library initially, since it touches
auth and catalog internals in ways the ASF project may want to design carefully.

---

## Group 6 — Developer Experience & Testing

### ITEM-010 · Iceberg Spec Conformance Test Suite (Language-Agnostic)

**Problem / Opportunity**
Each language implementation (Java, Python, Rust, Go, C++) maintains its own tests, but
there is no shared, language-agnostic conformance suite that verifies spec correctness.
This means subtle spec violations can exist in one implementation for months before anyone
notices, and cross-language interoperability is validated only informally (by hand-crafted
test fixtures shared on the dev list).

**Why it matters**
As the ecosystem expands to five or more language implementations and Iceberg V3 introduces
new binary formats (deletion vectors, row lineage), the cost of a spec divergence grows.
A shared conformance suite would catch regressions early and give new implementations a
clear checklist for graduation from experimental status.

**Technical approach**
- Define a JSON/YAML fixture format that encodes input Iceberg table state (metadata JSON,
  Parquet files, Puffin files) and expected output (query results, new metadata state).
- Implement a test runner interface that each language implementation can plug into:
  the runner feeds fixtures in, runs operations, and compares output to expected.
- Cover the following spec areas initially:
  - Snapshot commit: append, overwrite, delete.
  - Schema evolution: add column, rename, reorder, promote type.
  - Partition evolution: add field, replace field, void field.
  - V3 features: deletion vector read/write, row lineage, default column values.
  - Manifest compaction: merging manifest lists.
- Publish fixtures as a standalone `iceberg-spec-tests` repository under the Apache org.

**Complexity:** Medium-High

**Risks / Dependencies**
- Getting buy-in from all language implementation teams is a social challenge as much as a
  technical one; framing this as "spec tests, not implementation tests" is important.
- Fixture format design must be stable before implementations adopt it, otherwise updating
  fixtures becomes a multi-repo coordination headache.

**Upstream contribution potential**
Very High. This is the kind of community infrastructure contribution that PMC members
actively advocate for. Propose via an IIP and present at a community sync call.

---

## Priority Matrix

| Item | Group | Complexity | Upstream Potential | My Priority |
|------|-------|------------|-------------------|-------------|
| ITEM-004 | Statistics | Medium | Very High | **P0** |
| ITEM-001 | Streaming | Medium | High | **P1** |
| ITEM-006 | PyIceberg | Medium | High | **P1** |
| ITEM-007 | Rust | Medium-High | High | **P1** |
| ITEM-005 | Catalog | Medium-High | High | **P2** |
| ITEM-008 | Operations | Low-Medium | High | **P2** |
| ITEM-010 | Dev Experience | Medium-High | Very High | **P2** |
| ITEM-002 | Streaming | Medium-High | High | **P3** |
| ITEM-003 | Transactions | High | High | **P3** |
| ITEM-009 | Catalog | Medium | Medium | **P4** |

---

## Next Steps

1. **ITEM-004 (Spark CBO fix)** — Start by reproducing the bug locally with a minimal Spark
   + Iceberg test case. The fix scope should be clear within a day of exploration.
2. **ITEM-001 (Kafka sink)** — Begin with the Java implementation; the Python port follows
   once the API surface is stable.
3. **ITEM-006 (PyIceberg compaction)** — Open a discussion issue on `apache/iceberg-python`
   to gauge community appetite before writing code.

---

*All items are proposals only. None of the work described here represents commitments by the
Apache Software Foundation or any affiliated project.*
