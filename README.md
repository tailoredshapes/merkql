# merkql

Merkle tree backed persistent log with Kafka-compatible topic/partition/consumer-group semantics.

merkql provides an embedded, file-based alternative to Apache Kafka for event streaming workloads. Each partition is a content-addressed merkle tree, giving you cryptographic integrity verification and tamper detection on top of standard log semantics.

## Features

- **Kafka-compatible API** — Topics, partitions, consumer groups, offset management, subscribe/poll/commit_sync/close lifecycle
- **Merkle tree integrity** — Every partition is a merkle tree. Generate inclusion proofs, verify records, detect tampering
- **Content-addressed storage** — Git-style object store with SHA-256 hashing. Identical data is never stored twice
- **Crash-safe** — Atomic writes (temp+fsync+rename) for all metadata. Index fsync on every write. No data loss on crash
- **Concurrent** — Fine-grained locking: `RwLock` per partition, `RwLock` on topic map, `Mutex` per consumer group. Readers never block readers. Writers only block their own partition
- **LZ4 compression** — Optional per-broker transparent compression. Objects hashed before compression so merkle proofs work regardless of compression mode. Mixed-mode reads supported
- **Retention** — Configurable `max_records` per topic. Old records become unreachable without ObjectStore GC
- **Batch API** — `send_batch()` amortizes fsync (1 per batch instead of 1 per record)
- **Persistent** — All state survives process restarts. Topics, partitions, offsets, and tree snapshots are durable on disk
- **Zero dependencies at runtime** — No external services. No ZooKeeper, no JVM, no network servers

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
merkql = "0.1"
```

### Producer / Consumer

```rust
use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use std::time::Duration;

// Open a broker
let config = BrokerConfig::new("/tmp/my-log");
let broker = Broker::open(config).unwrap();

// Produce records
let producer = Broker::producer(&broker);
producer.send(&ProducerRecord::new("events", Some("user-1".into()), r#"{"action":"login"}"#)).unwrap();
producer.send(&ProducerRecord::new("events", None, r#"{"action":"heartbeat"}"#)).unwrap();

// Consume records
let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    group_id: "my-service".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});

consumer.subscribe(&["events"]).unwrap();
let records = consumer.poll(Duration::from_millis(100)).unwrap();

for record in &records {
    println!("{}: {}", record.topic, record.value);
}

consumer.commit_sync().unwrap();
consumer.close().unwrap();
```

### Merkle Proofs

```rust
// After producing records, verify integrity
let topic = broker.topic("events").unwrap();
let part_arc = topic.partition(0).unwrap();
let partition = part_arc.read().unwrap();

// Generate a proof for offset 0
let proof = partition.proof(0).unwrap().unwrap();

// Verify the proof
use merkql::tree::MerkleTree;
assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
```

### Compression

```rust
use merkql::compression::Compression;

let config = BrokerConfig {
    compression: Compression::Lz4,
    ..BrokerConfig::new("/tmp/my-log")
};
let broker = Broker::open(config).unwrap();
// All writes are now LZ4-compressed. Reads auto-detect compression via per-object markers.
```

### Retention

```rust
use merkql::topic::RetentionConfig;

let config = BrokerConfig {
    default_retention: RetentionConfig { max_records: Some(10_000) },
    ..BrokerConfig::new("/tmp/my-log")
};
let broker = Broker::open(config).unwrap();
// Topics auto-trim to the most recent 10,000 records per partition.
```

## Architecture

```
.merkql/
  topics/
    {topic-name}/
      meta.bin                    # Topic config (partition count)
      partitions/
        {id}/
          objects/ab/cdef...      # Content-addressed merkle nodes + records
          offsets.idx             # Fixed-width index: offset -> record hash
          tree.snapshot           # Incremental tree state (atomic write)
          retention.bin           # Retention marker (atomic write)
  groups/
    {group-id}/
      offsets.bin                 # Committed offsets per topic-partition (atomic write)
  config.bin                      # Broker config
```

## Kafka API Mapping

| merkql | Kafka | Notes |
|---|---|---|
| `Broker::open(config)` | `bootstrap.servers` | Startup |
| `Broker::consumer(broker, config)` | `new KafkaConsumer<>(props)` | Per-phase lifecycle |
| `consumer.subscribe(&["topic"])` | `consumer.subscribe(List.of(...))` | Set active topics |
| `consumer.poll(timeout)` | `consumer.poll(Duration)` | Returns `Vec<Record>` |
| `consumer.commit_sync()` | `consumer.commitSync()` | Persist offsets |
| `consumer.close()` | `consumer.close()` | End phase |
| `Broker::producer(broker)` | `new KafkaProducer<>(props)` | Send records |
| `producer.send(record)` | `producer.send(record)` | Auto-creates topics |
| `producer.send_batch(records)` | N/A | Batch API for amortized fsync |

## Correctness Verification

merkql includes a Jepsen-style test suite that makes data-backed claims about correctness properties at scale, injects faults with real assertions, and exercises random operation sequences via property-based testing.

### Properties Verified

| Property | Claim | Sample Size |
|---|---|---|
| **Total Order** | Partition offsets are monotonically increasing and gap-free | 10,000 records across 4 partitions |
| **Durability** | All records survive broker close/reopen cycles | 5,000 records across 3 reopen cycles |
| **Exactly-Once** | Consumer groups deliver every record exactly once across commit/restart | 1,000 records across 4 phases |
| **Merkle Integrity** | 100% of records have valid inclusion proofs | 10,000 proofs across 4 partitions |
| **No Data Loss** | Every confirmed append is immediately readable | 5,000 records verified immediately after write |
| **Byte Fidelity** | Values preserved exactly for edge-case payloads | 500 payloads (boundary lengths, unicode, CJK, RTL, combining chars, structured data, control chars, random ASCII up to 64KB) |

### Fault Injection (Nemesis)

All nemesis tests assert correctness — they do not just observe behavior.

| Fault | Assertion |
|---|---|
| **Crash (drop without close)** | All 1,000 records recovered after 10 ungraceful drop cycles |
| **Truncated tree.snapshot** | Broker refuses to reopen (safe failure) or reopens gracefully |
| **Truncated offsets.idx** | Broker reopens with 99 records (loses partial entry), zero read errors |
| **Missing tree.snapshot** | Broker reopens, all 100 records readable, new appends succeed |
| **Index ahead of snapshot** | 101 records readable, at least 100 valid proofs |

### v2 Feature Tests

| Feature | Tests |
|---|---|
| **Concurrent producers** | N threads writing to same topic, verify no gaps |
| **Concurrent consumers** | Independent groups consuming simultaneously |
| **Batch API** | send_batch with various sizes, empty batch |
| **LZ4 compression** | Round-trip, persistence, mixed-mode, merkle proofs |
| **Retention** | max_records window, consumer view, persistence |

### Property-Based Testing

50-100 proptest cases per family:

- **Random operation sequences** — Append/Read/Commit/CloseReopen across 3 topics, verify total order and proof validity
- **Payload fidelity** — Random printable ASCII 1B-1MB, exact preservation
- **Binary payload fidelity** — Arbitrary byte sequences (via `from_utf8_lossy`) 1B-64KB, tests full byte spectrum
- **Multi-topic/partition** — 1-5 topics x 1-8 partitions x 10-200 records, total order and proof validity
- **Multi-phase exactly-once** — 50-500 records, 2-6 phases, optional broker reopen between phases

### Running the Test Suite

```bash
# All tests (96 total)
cargo test

# Jepsen checkers only (6 tests including 500-payload byte fidelity)
cargo test --test jepsen_checkers

# Property-based tests (5 families including 1MB payloads and binary data)
cargo test --test jepsen_proptest

# Fault injection with assertions
cargo test --test jepsen_nemesis

# v2 features: concurrency, batch, compression, retention
cargo test --test v2_features_test

# Full JSON report (stdout + target/jepsen-report.json)
cargo test --test jepsen_report -- --nocapture

# Criterion benchmarks (HTML report in target/criterion/)
cargo bench
```

## Building

```bash
cargo build
cargo test
```

## License

MIT
