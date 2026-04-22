/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 */
package org.apache.iceberg.spark.stats;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.spark.stats.PuffinColumnStatsConverter.SparkColumnStat;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PuffinColumnStatsConverter}.
 */
class PuffinColumnStatsConverterTest {

  // ---------------------------------------------------------------------------
  // Schema fixtures
  // ---------------------------------------------------------------------------

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "order_id", Types.LongType.get()),
          Types.NestedField.optional(2, "customer", Types.StringType.get()),
          Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

  private static final Schema RENAMED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "order_key", Types.LongType.get()),   // renamed from order_id
          Types.NestedField.optional(2, "customer_name", Types.StringType.get()), // renamed
          Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static BlobMetadata thetaBlob(List<Integer> fieldIds, long ndv) {
    return new GenericBlobMetadata(
        PuffinColumnStatsConverter.THETA_SKETCH_BLOB_TYPE,
        1L, 0,
        fieldIds,
        Map.of(PuffinColumnStatsConverter.NDV_PROPERTY_KEY, String.valueOf(ndv)));
  }

  private static BlobMetadata thetaBlobNoNdv(List<Integer> fieldIds) {
    return new GenericBlobMetadata(
        PuffinColumnStatsConverter.THETA_SKETCH_BLOB_TYPE,
        1L, 0,
        fieldIds,
        Map.of()); // no ndv property
  }

  private static BlobMetadata unknownBlob(List<Integer> fieldIds) {
    return new GenericBlobMetadata(
        "some-unknown-blob-type",
        1L, 0,
        fieldIds,
        Map.of("ndv", "999")); // ndv present but wrong blob type
  }

  private static StatisticsFile statsFile(List<BlobMetadata> blobs) {
    return new GenericStatisticsFile(1L, "/stats.puffin", 1024L, 0L, blobs);
  }

  // ---------------------------------------------------------------------------
  // Test: happy path
  // ---------------------------------------------------------------------------

  @Test
  void convertsNdvForAllMatchingColumns() {
    StatisticsFile file = statsFile(List.of(
        thetaBlob(List.of(1), 500L),
        thetaBlob(List.of(2), 1200L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).hasSize(2);
    assertThat(result.get("order_id").ndv()).isEqualTo(500L);
    assertThat(result.get("customer").ndv()).isEqualTo(1200L);
  }

  // ---------------------------------------------------------------------------
  // Test: field ID survives column rename (key correctness fix)
  // ---------------------------------------------------------------------------

  @Test
  void fieldIdResolvesToRenamedColumnName() {
    // Stats were computed when columns were called order_id / customer.
    // The current schema has renamed them to order_key / customer_name.
    // Field IDs (1, 2) are stable → stats should resolve to new names.
    StatisticsFile file = statsFile(List.of(
        thetaBlob(List.of(1), 800L),
        thetaBlob(List.of(2), 300L)
    ));

    Map<String, SparkColumnStat> result =
        PuffinColumnStatsConverter.convert(file, RENAMED_SCHEMA);

    assertThat(result).hasSize(2);
    assertThat(result.get("order_key").ndv()).isEqualTo(800L);
    assertThat(result.get("customer_name").ndv()).isEqualTo(300L);
  }

  // ---------------------------------------------------------------------------
  // Test: blob type filtering
  // ---------------------------------------------------------------------------

  @Test
  void unknownBlobTypeIsIgnored() {
    StatisticsFile file = statsFile(List.of(
        unknownBlob(List.of(1)),   // unknown type, should be skipped
        thetaBlob(List.of(2), 42L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).hasSize(1).containsKey("customer");
    assertThat(result.get("customer").ndv()).isEqualTo(42L);
  }

  // ---------------------------------------------------------------------------
  // Test: missing ndv property
  // ---------------------------------------------------------------------------

  @Test
  void missingNdvPropertySkipsColumn() {
    StatisticsFile file = statsFile(List.of(
        thetaBlobNoNdv(List.of(1)),
        thetaBlob(List.of(2), 77L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).hasSize(1).containsKey("customer");
  }

  // ---------------------------------------------------------------------------
  // Test: dropped column (field ID not in current schema)
  // ---------------------------------------------------------------------------

  @Test
  void droppedColumnFieldIdIsSkipped() {
    // field ID 99 does not exist in the schema
    StatisticsFile file = statsFile(List.of(
        thetaBlob(List.of(99), 123L),
        thetaBlob(List.of(1), 456L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).hasSize(1).containsKey("order_id");
    assertThat(result.get("order_id").ndv()).isEqualTo(456L);
  }

  // ---------------------------------------------------------------------------
  // Test: multi-column blob is skipped
  // ---------------------------------------------------------------------------

  @Test
  void multiColumnBlobIsSkipped() {
    StatisticsFile file = statsFile(List.of(
        thetaBlob(List.of(1, 2), 100L), // joint NDV — not mappable to single ColumnStat
        thetaBlob(List.of(3), 50L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).hasSize(1).containsKey("amount");
  }

  // ---------------------------------------------------------------------------
  // Test: empty blobs
  // ---------------------------------------------------------------------------

  @Test
  void emptyBlobListReturnsEmptyMap() {
    StatisticsFile file = statsFile(List.of());

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, SCHEMA);

    assertThat(result).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test: nested struct column name resolution
  // ---------------------------------------------------------------------------

  @Test
  void nestedStructColumnNamesUseDotNotation() {
    Schema nestedSchema = new Schema(
        Types.NestedField.required(
            10, "address",
            Types.StructType.of(
                Types.NestedField.required(11, "city", Types.StringType.get()),
                Types.NestedField.optional(12, "zip", Types.StringType.get()))));

    StatisticsFile file = statsFile(List.of(
        thetaBlob(List.of(11), 200L),
        thetaBlob(List.of(12), 90000L)
    ));

    Map<String, SparkColumnStat> result = PuffinColumnStatsConverter.convert(file, nestedSchema);

    assertThat(result).hasSize(2);
    assertThat(result.get("address.city").ndv()).isEqualTo(200L);
    assertThat(result.get("address.zip").ndv()).isEqualTo(90000L);
  }
}
