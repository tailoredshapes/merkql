# merkql

Kafka semantics. Merkle integrity. Zero infrastructure.

An embedded event log for Rust. Topics, partitions, consumer groups, offset management — the parts of Kafka you actually use — backed by a content-addressed merkle tree that makes every record cryptographically verifiable.

No JVM. No ZooKeeper. No network. Just a directory on disk.

```rust
let broker = Broker::open(BrokerConfig::new("/tmp/events")).unwrap();

let producer = Broker::producer(&broker);
producer.send(&ProducerRecord::new("orders", Some("order-42".into()),
    r#"{"item":"widget","qty":5}"#)).unwrap();

let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    group_id: "billing".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});
consumer.subscribe(&["orders"]).unwrap();
let records = consumer.poll(Duration::from_millis(100)).unwrap();
consumer.commit_sync().unwrap();
```

## Why

Event streaming is the right abstraction for a lot of problems — append-only logs, consumer groups, offset tracking. Running a Kafka cluster for a single-node service, an embedded device, or your integration tests is not.

merkql gives you the programming model without the infrastructure. Every partition is a merkle tree, so every record gets a cryptographic inclusion proof. You can verify that a specific record existed at a specific offset without trusting anything except the math.

## Use cases

- **Tamper-evident audit logs.** SOX, HIPAA, PCI-DSS, GDPR — hand an auditor an inclusion proof and a root hash. They verify independently.
- **Event sourcing without infrastructure.** Your event store starts when your process starts. No Docker, no cluster management.
- **Integration testing.** Replace Kafka in your test suite. Same produce/subscribe/poll/commit lifecycle, no port conflicts, no isolation problems.
- **Edge and embedded systems.** IoT gateways, POS terminals, medical devices. In-process, no network, LZ4 compression, configurable retention.
- **Local development.** Run Kafka-based services locally with zero startup time.

## Features

| | |
|---|---|
| Kafka-compatible API | Topics, partitions, consumer groups, offset management |
| Merkle tree integrity | SHA-256 content addressing, inclusion proofs, tamper detection |
| Crash-safe | Atomic writes (temp+fsync+rename), CRC32 checksums, index validation |
| Concurrent reads | `RwLock` index + per-read file handles — readers never block readers or writers |
| Segment rolling | Configurable `max_segment_records` per partition, automatic segment sealing |
| Compaction | Delete sealed segments below retention window, reclaim disk space |
| Batch operations | `send_batch()` amortizes fsync; `get_batch()` amortizes file opens |
| LZ4 compression | Transparent, per-broker, mixed-mode reads |
| Retention | Configurable `max_records` per topic |
| Cold start | Only active segment scanned on open — sealed segments are metadata-only |
| Zero dependencies | No external services, no JVM, no network |
| Notification plugins | Optional webhook, SNS, SQS plugins for event-driven architectures |

## Performance

| Operation | Result |
|---|---|
| Append (256B) | 31 µs / 7.9 MB/s |
| Append (64KB) | 103 µs / 608 MB/s |
| Sequential read (10K) | 179K records/sec |
| Random-access read | 4.8 µs |
| Proof generate+verify (10K) | 150 µs |
| Broker reopen (50K records) | 14 µs |

See [BENCHMARKS.md](BENCHMARKS.md) for full results.

## Quick start

```toml
[dependencies]
merkql = "0.2"
```

```rust
use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use std::time::Duration;

let broker = Broker::open(BrokerConfig::new("/tmp/my-log")).unwrap();

// Produce
let producer = Broker::producer(&broker);
producer.send(&ProducerRecord::new("events", Some("user-1".into()),
    r#"{"action":"login"}"#)).unwrap();

// Consume
let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    group_id: "my-service".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});
consumer.subscribe(&["events"]).unwrap();
let records = consumer.poll(Duration::from_millis(100)).unwrap();
consumer.commit_sync().unwrap();
```

## Segment rolling

Partitions can be configured to roll into new segments after a fixed number of records. Each segment is an independent unit with its own pack file, index, and merkle tree. Sealed segments are read-only and eligible for compaction.

```rust
use merkql::partition::Partition;
use merkql::compression::Compression;

// Roll to a new segment every 100,000 records
let partition = Partition::open_with_config(
    0, "/tmp/my-partition", Compression::Lz4, Some(100_000)
).unwrap();
```

Reads are transparent across segments — `read()` and `read_range()` work the same whether the partition has one segment or fifty. Cold start is fast: only the active segment's index is scanned; sealed segments load from persisted metadata.

## Compaction

Sealed segments whose offset range falls entirely below the retention window are eligible for compaction. Call `compact()` to reclaim their disk space:

```rust
// Set minimum valid offset (e.g. from retention policy)
partition.set_retention(50_000);

// Delete sealed segments entirely below offset 50,000
let deleted = partition.compact().unwrap();
```

Compaction deletes entire segment directories. Active segments are never compacted.

## Concurrent access

The object store uses `RwLock` for the index and a separate `Mutex` for writes. Reads open a fresh file handle per call, so:

- Multiple readers run concurrently with zero contention
- Readers never block writers
- Writers only block other writers (briefly, during append)

For bulk reads, `get_batch()` opens a single file handle and reads all requested objects in sequence — fewer file descriptors, better throughput.

## Verify integrity

```rust
let topic = broker.topic("events").unwrap();
let partition = topic.partition(0).unwrap().read().unwrap();
let proof = partition.proof(0).unwrap().unwrap();

use merkql::tree::MerkleTree;
assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
```

## Notification plugins

merkql supports optional notification plugins for event-driven architectures. When records are written, plugins can fire webhooks, publish to AWS SNS, or enqueue to AWS SQS.

```toml
[dependencies]
merkql = { version = "0.2", features = ["notify"] }
merkql-notify-sns = "0.1"     # AWS SNS
merkql-notify-sqs = "0.1"     # AWS SQS
merkql-notify-webhook = "0.1" # HTTP webhooks
```

## Correctness

merkql ships with a Jepsen-style test suite: data-backed claims about correctness at scale, fault injection with real assertions, and property-based testing with random operation sequences.

Properties verified across 10,000+ records: total order, durability across restart cycles, exactly-once delivery, merkle proof validity, byte fidelity for edge-case payloads. Fault injection covers ungraceful crashes, truncated snapshots, truncated indices, and missing state files.

145 tests passing, including concurrent read/write stress tests and cross-segment range reads.

See [FAILURE-MODES.md](FAILURE-MODES.md) for a complete catalog of failure scenarios and recovery behavior.

## Documentation

[tailoredshapes.github.io/merkql](https://tailoredshapes.github.io/merkql/)

## License

MIT

---

A [TailoredShapes](https://tailoredshapes.github.io) project.
