/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.iceberg.kafka;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Prometheus-compatible metrics for {@link IcebergKafkaSink}.
 *
 * <p>Metrics are exposed as a simple text format via {@link #scrape()} — embed this in
 * a Prometheus HTTP handler or log it periodically.
 *
 * <h2>Metrics exposed</h2>
 * <ul>
 *   <li>{@code iceberg_sink_records_consumed_total} — Kafka records processed</li>
 *   <li>{@code iceberg_sink_records_skipped_total} — records skipped (null converter output)</li>
 *   <li>{@code iceberg_sink_commits_total} — Iceberg commits issued</li>
 *   <li>{@code iceberg_sink_commit_failures_total} — failed Iceberg commits</li>
 *   <li>{@code iceberg_sink_bytes_written_total} — estimated bytes written to object store</li>
 *   <li>{@code iceberg_sink_files_written_total} — data files created</li>
 *   <li>{@code iceberg_sink_consumer_lag_records} — last known Kafka consumer lag</li>
 *   <li>{@code iceberg_sink_last_commit_timestamp_seconds} — unix epoch of last commit</li>
 * </ul>
 */
public final class SinkMetrics {

  private final LongAdder recordsConsumed = new LongAdder();
  private final LongAdder recordsSkipped = new LongAdder();
  private final LongAdder commits = new LongAdder();
  private final LongAdder commitFailures = new LongAdder();
  private final LongAdder bytesWritten = new LongAdder();
  private final LongAdder filesWritten = new LongAdder();
  private final AtomicLong consumerLag = new AtomicLong(0);
  private final AtomicLong lastCommitTimestampMs = new AtomicLong(0);

  public void recordConsumed(long count) {
    recordsConsumed.add(count);
  }

  public void recordSkipped(long count) {
    recordsSkipped.add(count);
  }

  public void commitSucceeded(long fileCount, long approxBytes) {
    commits.increment();
    filesWritten.add(fileCount);
    bytesWritten.add(approxBytes);
    lastCommitTimestampMs.set(System.currentTimeMillis());
  }

  public void commitFailed() {
    commitFailures.increment();
  }

  public void updateConsumerLag(long lag) {
    consumerLag.set(lag);
  }

  /**
   * Returns a Prometheus text-format scrape string suitable for use in an HTTP /metrics
   * endpoint or for logging.
   */
  public String scrape() {
    long nowSeconds = System.currentTimeMillis() / 1000;
    return String.format(
        "# HELP iceberg_sink_records_consumed_total Kafka records processed by the sink\n"
            + "# TYPE iceberg_sink_records_consumed_total counter\n"
            + "iceberg_sink_records_consumed_total %d\n"
            + "# HELP iceberg_sink_records_skipped_total Records skipped (null converter output)\n"
            + "# TYPE iceberg_sink_records_skipped_total counter\n"
            + "iceberg_sink_records_skipped_total %d\n"
            + "# HELP iceberg_sink_commits_total Successful Iceberg commits\n"
            + "# TYPE iceberg_sink_commits_total counter\n"
            + "iceberg_sink_commits_total %d\n"
            + "# HELP iceberg_sink_commit_failures_total Failed Iceberg commits\n"
            + "# TYPE iceberg_sink_commit_failures_total counter\n"
            + "iceberg_sink_commit_failures_total %d\n"
            + "# HELP iceberg_sink_bytes_written_total Estimated bytes written to object store\n"
            + "# TYPE iceberg_sink_bytes_written_total counter\n"
            + "iceberg_sink_bytes_written_total %d\n"
            + "# HELP iceberg_sink_files_written_total Data files created in Iceberg\n"
            + "# TYPE iceberg_sink_files_written_total counter\n"
            + "iceberg_sink_files_written_total %d\n"
            + "# HELP iceberg_sink_consumer_lag_records Last known Kafka consumer lag\n"
            + "# TYPE iceberg_sink_consumer_lag_records gauge\n"
            + "iceberg_sink_consumer_lag_records %d\n"
            + "# HELP iceberg_sink_last_commit_timestamp_seconds Unix timestamp of last commit\n"
            + "# TYPE iceberg_sink_last_commit_timestamp_seconds gauge\n"
            + "iceberg_sink_last_commit_timestamp_seconds %d\n",
        recordsConsumed.sum(),
        recordsSkipped.sum(),
        commits.sum(),
        commitFailures.sum(),
        bytesWritten.sum(),
        filesWritten.sum(),
        consumerLag.get(),
        lastCommitTimestampMs.get() / 1000);
  }
}
