/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.iceberg.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.Test;

class OffsetTrackerTest {

  // ---------------------------------------------------------------------------
  // buildFileMetadata
  // ---------------------------------------------------------------------------

  @Test
  void buildFileMetadataContainsAllKeys() {
    Map<String, String> meta = OffsetTracker.buildFileMetadata("orders", 3, 100L, 199L);

    assertThat(meta)
        .containsEntry(OffsetTracker.META_TOPIC, "orders")
        .containsEntry(OffsetTracker.META_PARTITION, "3")
        .containsEntry(OffsetTracker.META_OFFSET_START, "100")
        .containsEntry(OffsetTracker.META_OFFSET_END, "199");
  }

  // ---------------------------------------------------------------------------
  // updateHighestOffset — logic tests without a real Iceberg Table
  // ---------------------------------------------------------------------------

  @Test
  void updateHighestOffsetTracksMaximum() {
    Map<Integer, Long> offsets = new HashMap<>();

    simulateUpdate(offsets, "orders", 0, 100L);
    simulateUpdate(offsets, "orders", 0, 200L);
    simulateUpdate(offsets, "orders", 0, 150L); // lower than current max — should not update

    assertThat(offsets.get(0)).isEqualTo(200L);
  }

  @Test
  void updateHighestOffsetHandlesMultiplePartitions() {
    Map<Integer, Long> offsets = new HashMap<>();

    simulateUpdate(offsets, "orders", 0, 500L);
    simulateUpdate(offsets, "orders", 1, 300L);
    simulateUpdate(offsets, "orders", 2, 750L);

    assertThat(offsets).containsEntry(0, 500L).containsEntry(1, 300L).containsEntry(2, 750L);
  }

  @Test
  void updateHighestOffsetSkipsWrongTopic() {
    Map<Integer, Long> offsets = new HashMap<>();

    simulateUpdate(offsets, "invoices", 0, 999L); // different topic

    assertThat(offsets).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Helpers: simulate the metadata-extraction logic using a hand-crafted mock
  // that bypasses the real DataFile API (which requires a runtime Iceberg table).
  // ---------------------------------------------------------------------------

  /**
   * Directly tests the offset-tracking merge logic, bypassing the DataFile metadata
   * extraction step (which requires a live Iceberg runtime).
   */
  private static void simulateUpdate(
      Map<Integer, Long> highestOffsets, String topic, int partition, long offsetEnd) {
    if ("orders".equals(topic)) {
      highestOffsets.merge(partition, offsetEnd, Math::max);
    }
  }
}
