# merkql — Product Overview

## The problem

Event streaming is the right architecture for a growing number of systems: audit logs, event sourcing, CQRS, change data capture, real-time pipelines. Apache Kafka proved the model works.

But Kafka carries a cost:

- **Infrastructure overhead** — A minimum Kafka deployment is 3 brokers + ZooKeeper (or KRaft). That's 4+ JVM processes, each wanting 6GB+ of heap, plus monitoring, plus operational expertise.
- **Development friction** — Running Kafka locally means Docker Compose, port management, and 30-second startup times. Integration tests need a running cluster or a test container that adds minutes to CI.
- **No integrity guarantees** — Kafka stores records, but it can't prove a record hasn't been modified. For regulated industries, you need a separate system to establish tamper evidence.
- **Overkill for single-node** — Many applications that benefit from log semantics don't need distributed consensus. They need an append-only log with consumer groups, running on one machine.

## What merkql does

merkql gives you the Kafka programming model — topics, partitions, consumer groups, offset management — as a Rust library. No network, no JVM, no external processes.

Every partition is also a merkle tree. Every record gets a SHA-256 content-addressed hash and a position in a cryptographic tree structure. You can generate an inclusion proof for any record and independently verify it against the tree root. This is not an optional add-on — it's the storage format itself.

## Who should use merkql

### Teams building audit-sensitive systems

If your application handles financial transactions, medical records, access logs, or any data subject to regulatory audit, you need to demonstrate that records haven't been tampered with after the fact. merkql's merkle tree gives you a mathematically verifiable proof for every record.

Traditional approaches to audit integrity:
- Write-ahead logs with checksums (detect corruption, not tampering)
- Append-only databases with ACLs (trust the admin)
- Blockchain (enormous infrastructure for a simple requirement)

merkql sits in the gap: cryptographic tamper evidence without the infrastructure cost of a blockchain and without the trust assumptions of a checksum.

### Teams replacing Kafka in single-node deployments

You have a service that uses Kafka for event streaming, but it runs on a single machine — or you're deploying to edge locations, embedded systems, or air-gapped environments where running a Kafka cluster isn't practical. merkql gives you the same produce/consume/commit lifecycle with the same consumer group semantics. Your application code changes minimally.

### Teams that want testable event-driven architectures

Integration tests that depend on Kafka are slow and fragile. merkql runs in-process in a temp directory, starts in microseconds, and provides the same consumer group semantics as Kafka. Write your test, produce events, consume them, assert results — no containers, no ports, no cleanup.

### Teams building event-sourced applications

Event sourcing needs an append-only log with:
- Multiple consumer groups (one per projection)
- Offset tracking (each projection tracks its own position)
- Retention (you don't need every event forever — just the recent window)
- Durability (events are the source of truth — losing them is catastrophic)

merkql provides all four, plus compression to keep storage manageable and merkle proofs to verify event integrity. As a library, it eliminates the deployment gap between "we want event sourcing" and "we don't want to run Kafka."

## What merkql does not do

- **Distributed replication** — merkql runs on one machine. It does not replicate across nodes. If you need fault tolerance across machines, use Kafka.
- **Network protocol** — merkql is an embedded library, not a server. If you need remote producers/consumers, put an API layer in front of it.
- **Transactions across topics** — Each partition is independent. There are no cross-partition transactions.
- **Schema registry** — Records are opaque byte strings. Schema management is your responsibility.

merkql is for workloads where the Kafka *programming model* is right but the Kafka *deployment model* is wrong.

## Technical properties

### Crash safety

Every metadata write (tree snapshots, retention markers, consumer group offsets) uses the temp+fsync+rename pattern. The index is fsynced on every write. This isn't aspirational — the test suite includes Jepsen-style fault injection tests that truncate files, delete snapshots, and drop the broker without closing, then assert that all committed data is recoverable.

### Concurrency

The broker uses fine-grained internal locking:
- `RwLock` per partition — multiple readers, single writer, per-partition scope
- `RwLock` on the topic map — topic lookups are read-only most of the time
- `Mutex` per consumer group — groups are independent of each other

A producer writing to partition 0 never blocks a consumer reading partition 1. Multiple consumers reading the same partition run concurrently.

### Merkle tree design

The tree uses a binary carry chain (like binary addition). Appending a leaf is O(log n). The tree state is captured in a compact snapshot — a vector of `(height, hash, min_offset, max_offset)` tuples — that survives restarts without replaying the entire log. Proofs are generated by walking from the root to the target leaf, collecting sibling hashes along the way. Verification reconstructs the path and checks that it produces the claimed root.

Objects are hashed *before* compression, so the merkle tree is compression-independent. You can switch between compressed and uncompressed modes without invalidating proofs. Per-object markers allow mixed-mode reads — a partition can contain both compressed and uncompressed objects.

### Performance

Batch writes amortize fsync: `send_batch()` performs one flush and one fsync for the entire batch instead of one per record. For high-throughput scenarios, this is the difference between I/O-bound and CPU-bound.

Sequential reads are fast because the offset index is a fixed-width array — offset N is at byte position N*32 in the index file. Random reads by offset are O(1) for the index lookup plus one content-addressed store read.

## Comparison

| | merkql | Kafka | SQLite WAL | Custom append-only file |
|---|---|---|---|---|
| Consumer groups | Yes | Yes | No | No |
| Offset tracking | Yes | Yes | No | No |
| Partitions | Yes | Yes | No | No |
| Tamper detection | Merkle proofs | No | No | No |
| Deployment | Library | Cluster | Library | Library |
| Compression | LZ4 | Multiple | No | Manual |
| Retention | Yes | Yes | Manual | Manual |
| Crash safety | Atomic writes + fsync | Replicated log | WAL + checkpoints | Manual |
| Distributed | No | Yes | No | No |

## Getting started

```toml
[dependencies]
merkql = "0.1"
```

See [README.md](README.md) for API documentation and code examples.
