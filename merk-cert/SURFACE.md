# The merk surface

This document names what `merk-cert` certifies. An implementation that exposes
this surface (the suite compiles against it) and passes the suite (the behavior
holds) is a drop-in merkql: an embedder swaps one dependency line —

```toml
merkql = { package = "merk-aws", git = "...", tag = "..." }
```

— recompiles, and everything works. Anything *not* named here is implementation
detail and free to differ.

## Types and signatures (checked by compilation)

| Path | Certified items |
|---|---|
| `broker` | `Broker`, `BrokerConfig::new(location)`, `BrokerRef`, `Broker::open(BrokerConfig) -> Result<BrokerRef>`, `Broker::producer(&BrokerRef) -> Producer`, `Broker::consumer(&BrokerRef, ConsumerConfig) -> Consumer`, `ensure_topic(&str)`, `create_topic(&str, u32)`, `topic(&str) -> Option<Arc<Topic>>` |
| `producer` | `Producer::send(&ProducerRecord) -> Result<Record>`, `send_batch(&[ProducerRecord]) -> Result<Vec<Record>>` |
| `consumer` | `Consumer::subscribe(&[&str])`, `poll(Duration) -> Result<Vec<Record>>`, `commit_sync()`, `close()`; `ConsumerConfig { group_id, auto_commit, offset_reset }`; `OffsetReset::{Earliest, Latest}` |
| `record` | `Record { key, value, topic, partition, offset, timestamp }` (`timestamp: chrono::DateTime<Utc>`), `Record::{hash, serialize, deserialize}`; `ProducerRecord::new(topic, key, value)` |
| `topic` | `Topic::{num_partitions, partition(u32) -> Option<Arc<RwLock<Partition>>>}` |
| `partition` | `Partition::{merkle_root() -> Result<Option<Hash>>, proof(u64) -> Result<Option<Proof>>, verify_proof(&Proof) -> Result<bool>}` |
| `hash` | `Hash::{digest, to_hex, from_hex}` |

`BrokerConfig::new` takes an opaque location (`impl Into<PathBuf>`): a
directory for local merkql, a URI-shaped string (`s3://…`, `az://…`) for cloud
implementations.

## Behavior (checked by the tests)

- **Round-trip** (`cert_produce_consume`): send → poll preserves key, value,
  topic; offsets assigned densely from 0; batch offsets sequential; drained
  consumers poll empty then follow the tail; topics isolated; closed consumers
  reject operations, close is idempotent.
- **Groups** (`cert_groups_offsets`): commit persists position per group;
  uncommitted positions are not persisted; groups are independent; `Latest`
  skips history; a committed offset beats the reset policy; `auto_commit`
  commits on close.
- **Persistence** (`cert_persistence`): records, committed offsets, and topic
  partition counts survive close-and-reopen of the same location.
- **Partitioning** (`cert_partitioning`): identical keys always route to the
  same partition; per-partition offsets are dense and ordered; a subscriber
  sees every partition, in order within each.
- **Merkle integrity** (`cert_merkle`): a root exists after append and changes
  with growth; every offset yields a proof that verifies; out-of-range offsets
  yield none; the root survives reopen.
- **Wire format** (`cert_format`): record serialization round-trips; the record
  hash and the hash algorithm (SHA-256) are pinned to golden values. Breaking
  these breaks every log on disk and cross-implementation verification —
  a change here is a breaking release everywhere.

## Deliberately NOT certified

- Durability mechanics (fsync vs conditional PUT vs append CAS) — each
  implementation documents its own failure modes.
- Retention, compaction, compression, segment layout, notify plugins.
- Concurrency across processes (locking vs CAS) — cert runs single-process.
- `poll` timeout semantics: the parameter exists for Kafka familiarity; the
  suite never relies on blocking behavior.

## Certifying an implementation

1. Add its dependency under the matching `impl-*` feature in `Cargo.toml`.
2. Point the `subject` alias at it in `src/lib.rs` and provide a
   `fresh_site()` in `src/site.rs` (the only implementation-specific code).
3. `cargo test -p merk-cert --no-default-features --features impl-aws`

Compilation failure = surface drift. Test failure = behavior drift. Green =
certified.
