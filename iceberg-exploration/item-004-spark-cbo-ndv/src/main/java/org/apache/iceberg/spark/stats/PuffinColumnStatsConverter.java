/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 */
package org.apache.iceberg.spark.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Puffin {@link BlobMetadata} entries in a {@link StatisticsFile} into a map of
 * Spark column names → {@link SparkColumnStat}.
 *
 * <h2>Blob types handled</h2>
 *
 * <ul>
 *   <li>{@value THETA_SKETCH_BLOB_TYPE} – Apache DataSketches Theta Sketch; carries the
 *       {@code ndv} property as a serialised long string.</li>
 * </ul>
 *
 * <h2>Field-ID to column-name resolution</h2>
 *
 * {@link BlobMetadata#fields()} returns a list of Iceberg field IDs. We resolve them
 * against the <em>current</em> table schema (not the schema at stats-computation time),
 * so column renames that occurred after the stats were computed are handled correctly.
 * If a field ID no longer exists in the current schema (e.g. the column was dropped), the
 * blob is silently skipped.
 */
public final class PuffinColumnStatsConverter {

  private static final Logger LOG = LoggerFactory.getLogger(PuffinColumnStatsConverter.class);

  /** Blob type string for Apache DataSketches Theta Sketch v1. */
  public static final String THETA_SKETCH_BLOB_TYPE = "apache-datasketches-theta-v1";

  /** Property key inside {@link BlobMetadata#properties()} that carries the NDV estimate. */
  public static final String NDV_PROPERTY_KEY = "ndv";

  private PuffinColumnStatsConverter() {}

  /**
   * Converts all Theta-sketch blobs in {@code statsFile} to {@link SparkColumnStat} objects.
   *
   * @param statsFile the statistics file whose blobs to convert
   * @param currentSchema the current table schema (used for field-ID → name resolution)
   * @return map of Spark column name → {@link SparkColumnStat} (may be empty, never null)
   */
  public static Map<String, SparkColumnStat> convert(
      StatisticsFile statsFile, Schema currentSchema) {

    Map<Integer, String> fieldIdToName = buildFieldIdToNameMap(currentSchema);
    Map<String, SparkColumnStat> result = new HashMap<>();

    for (BlobMetadata blob : statsFile.blobMetadata()) {
      if (!THETA_SKETCH_BLOB_TYPE.equals(blob.type())) {
        continue;
      }

      List<Integer> fieldIds = blob.fields();
      if (fieldIds == null || fieldIds.isEmpty()) {
        LOG.warn(
            "Theta-sketch blob in statistics file {} has no field IDs; skipping",
            statsFile.path());
        continue;
      }

      // Theta sketches can cover multiple columns (for joint NDV), but the common case
      // is a single column. We only project single-column blobs to Spark ColumnStat.
      if (fieldIds.size() > 1) {
        LOG.debug(
            "Skipping multi-column Theta-sketch blob (fields={}) — Spark ColumnStat "
                + "does not model joint NDV",
            fieldIds);
        continue;
      }

      int fieldId = fieldIds.get(0);
      String columnName = fieldIdToName.get(fieldId);
      if (columnName == null) {
        LOG.debug(
            "Field ID {} in statistics file {} is not present in the current schema; skipping",
            fieldId,
            statsFile.path());
        continue;
      }

      OptionalLong ndv = readNdv(blob, statsFile.path());
      if (ndv.isPresent()) {
        result.put(columnName, new SparkColumnStat(ndv.getAsLong()));
        LOG.debug("Resolved NDV={} for column '{}' (field ID={})", ndv.getAsLong(), columnName, fieldId);
      }
    }

    return result;
  }

  private static OptionalLong readNdv(BlobMetadata blob, String statsFilePath) {
    String ndvString = blob.properties().get(NDV_PROPERTY_KEY);
    if (ndvString == null || ndvString.isEmpty()) {
      LOG.debug(
          "Theta-sketch blob in {} has no '{}' property; NDV estimate unavailable",
          statsFilePath,
          NDV_PROPERTY_KEY);
      return OptionalLong.empty();
    }
    try {
      long ndv = Long.parseLong(ndvString);
      if (ndv < 0) {
        LOG.warn("NDV value {} in {} is negative; ignoring", ndv, statsFilePath);
        return OptionalLong.empty();
      }
      return OptionalLong.of(ndv);
    } catch (NumberFormatException e) {
      LOG.warn(
          "Failed to parse NDV property '{}' as long in {}; ignoring",
          ndvString,
          statsFilePath);
      return OptionalLong.empty();
    }
  }

  /**
   * Builds a flat field-ID → column-name map by walking all non-nested fields in the schema.
   * Nested structs are projected as {@code parent.child} to match Spark's dot-notation.
   */
  static Map<Integer, String> buildFieldIdToNameMap(Schema schema) {
    Map<Integer, String> map = new HashMap<>();
    collectFields(schema.columns(), "", map);
    return map;
  }

  private static void collectFields(
      List<Types.NestedField> fields, String prefix, Map<Integer, String> out) {
    for (Types.NestedField field : fields) {
      String qualifiedName = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
      out.put(field.fieldId(), qualifiedName);
      if (field.type().isStructType()) {
        collectFields(field.type().asStructType().fields(), qualifiedName, out);
      }
    }
  }

  /**
   * Minimal value object representing per-column statistics derived from Puffin blobs.
   * Extend this as more blob types are supported (e.g. min/max histograms).
   */
  public static final class SparkColumnStat {
    private final long ndv;

    public SparkColumnStat(long ndv) {
      this.ndv = ndv;
    }

    /** Estimated number of distinct values; suitable for use in {@code ColumnStat.distinctCount}. */
    public long ndv() {
      return ndv;
    }

    @Override
    public String toString() {
      return "SparkColumnStat{ndv=" + ndv + "}";
    }
  }
}
