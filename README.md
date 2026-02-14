# merkql

Merkle tree backed persistent log with Kafka-compatible topic/partition/consumer-group semantics.

merkql provides an embedded, file-based alternative to Apache Kafka for event streaming workloads. Each partition is a content-addressed merkle tree, giving you cryptographic integrity verification and tamper detection on top of standard log semantics.

## Features

- **Kafka-compatible API** — Topics, partitions, consumer groups, offset management, subscribe/poll/commit_sync/close lifecycle
- **Merkle tree integrity** — Every partition is a merkle tree. Generate inclusion proofs, verify records, detect tampering
- **Content-addressed storage** — Git-style object store with SHA-256 hashing. Identical data is never stored twice
- **Persistent** — All state survives process restarts. Topics, partitions, offsets, and tree snapshots are durable on disk
- **Zero dependencies at runtime** — No external services. No ZooKeeper, no JVM, no network servers
- **Multi-phase consumption** — Subscribe, drain, commit, close, re-subscribe with new topics. Offsets persist across phases

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
let broker_guard = broker.lock().unwrap();
let topic = broker_guard.topic("events").unwrap();
let partition = topic.partition(0).unwrap();

// Generate a proof for offset 0
let proof = partition.proof(0).unwrap().unwrap();

// Verify the proof
use merkql::tree::MerkleTree;
assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
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
          HEAD                    # Hash of latest commit
          offsets.idx             # Fixed-width index: offset -> record hash
          tree.snapshot           # Incremental tree state
  groups/
    {group-id}/
      offsets.bin                 # Committed offsets per topic-partition
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
| **Missing HEAD** | Broker reopens, all 100 records readable, new appends succeed |
| **Index ahead of snapshot** | 101 records readable, at least 100 valid proofs |

### Property-Based Testing

50-100 proptest cases per family:

- **Random operation sequences** — Append/Read/Commit/CloseReopen across 3 topics, verify total order and proof validity
- **Payload fidelity** — Random printable ASCII 1B-1MB, exact preservation
- **Binary payload fidelity** — Arbitrary byte sequences (via `from_utf8_lossy`) 1B-64KB, tests full byte spectrum
- **Multi-topic/partition** — 1-5 topics x 1-8 partitions x 10-200 records, total order and proof validity
- **Multi-phase exactly-once** — 50-500 records, 2-6 phases, optional broker reopen between phases

### Performance

Measured on the Jepsen report runner (unoptimized build):

| Operation | P50 | P95 | P99 | Throughput |
|---|---|---|---|---|
| Append (256B) | 139 us | 193 us | 263 us | 6,825 ops/s |
| Read (sequential) | 8 us | 9 us | 12 us | 113,241 ops/s |
| Proof generate + verify (10K log) | 516 us | 596 us | 804 us | 1,897 ops/s |
| Broker reopen (10K records) | 28 us | 32 us | 72 us | — |

Optimized build (Criterion):

| Operation | Time |
|---|---|
| Append (256B payload) | 103 us |
| Append throughput (256KB payload) | 948 MiB/s |
| Read latency (random access, 10K log) | 4.8 us |
| Read throughput (10K sequential scan) | 141K elem/s |
| Proof generate + verify (10K log) | 149 us |
| Broker reopen (50K records) | 28 us |

### Running the Test Suite

```bash
# All tests (66 total)
cargo test

# Jepsen checkers only (6 tests including 500-payload byte fidelity)
cargo test --test jepsen_checkers

# Property-based tests (5 families including 1MB payloads and binary data)
cargo test --test jepsen_proptest

# Fault injection with assertions
cargo test --test jepsen_nemesis

# Full JSON report (stdout + target/jepsen-report.json)
# Includes 6 property checks, 5 nemesis checks, 6+ benchmarks
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
