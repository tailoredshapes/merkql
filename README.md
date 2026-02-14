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

## Building

```bash
cargo build
cargo test
```

## License

MIT
