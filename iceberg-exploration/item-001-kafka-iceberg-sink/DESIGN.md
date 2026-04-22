# ITEM-001: Kafka-Native Iceberg Sink with Adaptive Micro-Batching

## Problem statement

Writing Kafka events to Iceberg today requires either:
- **Apache Flink** — full stateful stream-processing runtime, heavy operationally
- **Kafka Connect** — limited transactional guarantees, no native Iceberg support without a plugin
- **Hand-rolled consumer** — correctness burden on the application team

None of these give a lightweight, standalone, self-tuning writer. The core tension:

| Force | Preference |
|-------|------------|
| Kafka | Continuous, low-latency writes |
| Iceberg | Infrequent, large commits (small files hurt scan performance) |

No first-class library manages this trade-off automatically.

---

## Design

### Commit triggers (adaptive micro-batching)

A commit is issued when **either** threshold is crossed, whichever comes first:

1. **Time threshold** (`commit.interval.ms`, default 60 000 ms)  
   Limits maximum end-to-end latency even under low traffic.
2. **Size threshold** (`target.file.size.bytes`, default 128 MB)  
   Limits small-file accumulation under high traffic.

Under sustained load the size trigger fires frequently, keeping files large.
Under low load the time trigger fires, keeping latency bounded.

### Exactly-once delivery

Each Kafka partition is its own write stream. Kafka `(partition, offset)` pairs are
embedded in each Iceberg data file's `custom_metadata` map:

```
iceberg.kafka.source.topic   = orders
iceberg.kafka.source.partition = 3
iceberg.kafka.source.offset.start = 1000
iceberg.kafka.source.offset.end   = 1999
```

On restart, the sink reads the latest Iceberg snapshot's data files for each Kafka
partition, extracts the highest committed offset, and resumes from `offset + 1`.
This gives exactly-once semantics without a separate offset store.

### API surface

```java
IcebergKafkaSink sink = IcebergKafkaSink.builder()
    .catalog(catalog)
    .table(TableIdentifier.of("db", "orders"))
    .kafkaProps(Map.of(
        "bootstrap.servers", "broker:9092",
        "group.id", "iceberg-sink"))
    .topics(List.of("orders"))
    .recordConverter(new JsonToIcebergRecordConverter(schema))
    .commitIntervalMs(60_000)
    .targetFileSizeBytes(128 * 1024 * 1024)
    .build();

sink.start();      // blocks; call sink.stop() from another thread / signal handler
```

### File structure

```
item-001-kafka-iceberg-sink/
├── DESIGN.md
└── src/
    ├── main/java/org/apache/iceberg/kafka/
    │   ├── IcebergKafkaSink.java          # public entry point, builder, main loop
    │   ├── PartitionWriter.java           # per-Kafka-partition write buffer + commit
    │   ├── CommitCoordinator.java         # fires commits based on time/size triggers
    │   ├── OffsetTracker.java             # reads/writes (partition, offset) metadata
    │   ├── RecordConverter.java           # interface: ConsumerRecord → Record
    │   └── SinkMetrics.java               # Prometheus-compatible metrics
    └── test/java/org/apache/iceberg/kafka/
        ├── OffsetTrackerTest.java
        ├── CommitCoordinatorTest.java
        └── PartitionWriterTest.java
```
