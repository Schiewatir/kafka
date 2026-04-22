/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.iceberg.kafka;

import org.apache.iceberg.data.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Converts a raw Kafka {@link ConsumerRecord} into an Iceberg {@link Record}.
 *
 * <p>Implementations are responsible for parsing the record value (bytes, JSON, Avro, etc.)
 * and mapping it to the target Iceberg schema. Schema evolution decisions (e.g. how to handle
 * unexpected fields) are also the converter's responsibility.
 *
 * @param <K> Kafka record key type
 * @param <V> Kafka record value type
 */
@FunctionalInterface
public interface RecordConverter<K, V> {

  /**
   * Convert a Kafka record to an Iceberg {@link Record}.
   *
   * @param kafkaRecord the raw Kafka consumer record
   * @return the converted Iceberg record, or {@code null} to skip this record (tombstone
   *     handling, filtered events, etc.)
   * @throws RecordConversionException if the record cannot be converted and the error is
   *     not recoverable (will cause the sink to pause and report the bad offset)
   */
  Record convert(ConsumerRecord<K, V> kafkaRecord) throws RecordConversionException;

  /** Thrown when a record cannot be converted and the error is not recoverable. */
  class RecordConversionException extends RuntimeException {
    public RecordConversionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
