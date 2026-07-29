---
name: merkql-architecture
description: Use when choosing an event log or storage backend, deciding between merkql and an object-storage backend, sizing an event-sourced design, or reasoning about whether merkql can be deployed as a service or reached from another language.
---

# merkql: what it is, and which one to reach for

**merkql is an embedded Rust library, not a deployed service.** This
misconception recurs, so state it plainly: there is no server, no wire protocol,
and no client for any other language. `Broker::open(path)` is a function call in
your process. From the README: *"No JVM. No ZooKeeper. No network. Just a
directory on disk."*

It gives you Kafka's *programming model* — topics, partitions, consumer groups,
offsets, plus merkle inclusion proofs — without Kafka's *deployment model*. If
you need remote producers or consumers, you put an API in front of it. That is a
deliberate boundary, not a gap to fill.

## The family

| Crate | Storage | Use when |
|---|---|---|
| `merkql` | local files, fsync, flock | Single node, edge, embedded, tests. Microsecond appends. |
| `merk-object` | any store with CAS append | The shared engine. You never deploy this directly. |
| `merk-aws` | S3 Express One Zone | Serverless on AWS. Scale to zero. |
| `merk-azure` | Azure append blobs | Serverless on Azure. Certifies locally against Azurite. |

All pass **`merk-cert`**, a 27-test suite that *defines* the surface. Swapping
implementations is one Cargo line: `merkql = { package = "merk-aws", ... }`.
Certify a new backend by invoking `merk_cert::merk_cert_suite!` — compiling it
proves the API matches, passing it proves the behaviour does.

## Formats agree; containers do not

Records serialize to identical bytes, merkle roots match at every tree size, and
proofs cross-verify in both directions. But merkql stores records in
content-addressed pack files and `merk-object` stores length-prefixed frames in
segment objects. **A merkql directory is not a merk-aws location.** Moving a log
between them means replaying records — lossless, but a conversion, not a copy.

## Choosing

**merkql** when it is one process on one machine and you want microseconds.

**merk-aws / merk-azure** when the workload is *intermittent* — bursty,
business-hours, domain events rather than telemetry — and you would rather not
run and pay for a cluster that idles. Costs approximately nothing when nothing
is happening. Expect ~8 ms per append and ~12,000 events/s per partition at
batch 100 (measured, in-region).

**Kafka** when the log runs hot continuously, you need sub-millisecond latency,
push-based consumers, or the ecosystem. What you pay a broker for is
*serialization* — a partition leader orders concurrent writes for free. Without
one, writers serialize themselves against the store and degrade under
contention. If you are pushing merk-aws's documented limits, that is a signal
you have stopped being a scale-to-zero workload.

**REQUIRED when building on the cloud backends:** the merk-cloud repo's
`designing-on-merk-cloud` skill. Several rules there are load-bearing and not
guessable from the API.

## Anti-patterns

- Calling merkql "standalone", "a queue service", or "deployed separately".
- Assuming a non-Rust process can read a merkql log — only the REST/API layer
  you put in front of it is language-agnostic.
- Assuming a merkql data directory can be handed to merk-aws, or vice versa.
- Treating merkql as required for event sourcing. It is one backend choice.
