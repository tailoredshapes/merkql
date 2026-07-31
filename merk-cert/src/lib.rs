//! # merk-cert
//!
//! The certification suite that *defines* the merkql surface.
//!
//! The suite is a macro so that it runs inside the crate being certified,
//! against that crate's real type names. Certification is therefore two checks
//! in one:
//!
//! 1. **Compile** — the implementation exposes the exact module paths, types,
//!    and signatures the suite uses. If it compiles, an embedder can swap the
//!    dependency and recompile unchanged.
//! 2. **Pass** — the implementation honors the behavioral contract.
//!
//! ## Certifying an implementation
//!
//! Add `merk-cert` as a dev-dependency, then in `tests/cert.rs`:
//!
//! ```ignore
//! struct Site { /* whatever the implementation needs */ }
//! impl Site {
//!     fn config(&self) -> my_impl::broker::BrokerConfig { /* ... */ }
//! }
//!
//! fn fresh_site() -> Site { /* provision an empty, isolated location */ }
//!
//! merk_cert::merk_cert_suite!(my_impl, crate::fresh_site);
//! ```
//!
//! The two arguments are the crate (or module) exposing the surface, and a
//! function returning a fresh site whose `config()` yields a `BrokerConfig`
//! pointing at it. Provisioning is the *only* implementation-specific code;
//! everything else is fixed by the suite.
//!
//! See `SURFACE.md` for what is certified and what is deliberately left free.

/// Emit the full certification suite for `$subject`, provisioning storage with
/// `$fresh_site`.
#[macro_export]
macro_rules! merk_cert_suite {
    ($subject:path, $fresh_site:path) => {
        mod cert_produce_consume {
            use std::time::Duration;
            use $fresh_site as fresh_site;
            use $subject as subject;

            fn earliest(
                broker: &subject::broker::BrokerRef,
                group: &str,
            ) -> subject::consumer::Consumer {
                subject::broker::Broker::consumer(
                    broker,
                    subject::consumer::ConsumerConfig {
                        group_id: group.into(),
                        auto_commit: false,
                        offset_reset: subject::consumer::OffsetReset::Earliest,
                    },
                )
            }

            #[test]
            fn round_trip_preserves_record_fields() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();

                let producer = subject::broker::Broker::producer(&broker);
                producer
                    .send(&subject::record::ProducerRecord::new(
                        "events",
                        Some("k1".into()),
                        "hello",
                    ))
                    .unwrap();

                let mut consumer = earliest(&broker, "g");
                consumer.subscribe(&["events"]).unwrap();
                let records = consumer.poll(Duration::from_millis(100)).unwrap();

                assert_eq!(records.len(), 1);
                let r = &records[0];
                assert_eq!(r.topic, "events");
                assert_eq!(r.key.as_deref(), Some("k1"));
                assert_eq!(r.value, "hello");
                assert_eq!(r.offset, 0);
                assert!(r.timestamp.timestamp() > 1_500_000_000, "timestamp is set");
            }

            #[test]
            fn send_returns_record_with_assigned_offset() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                let a = producer
                    .send(&subject::record::ProducerRecord::new("t", None, "a"))
                    .unwrap();
                let b = producer
                    .send(&subject::record::ProducerRecord::new("t", None, "b"))
                    .unwrap();

                assert_eq!(a.offset, 0);
                assert_eq!(b.offset, 1);
                assert_eq!(a.value, "a");
                assert_eq!(b.value, "b");
            }

            #[test]
            fn send_batch_assigns_sequential_offsets() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                let batch: Vec<subject::record::ProducerRecord> = (0..5)
                    .map(|i| {
                        subject::record::ProducerRecord::new("batched", None, format!("msg-{i}"))
                    })
                    .collect();
                let results = producer.send_batch(&batch).unwrap();

                assert_eq!(results.len(), 5);
                for (i, r) in results.iter().enumerate() {
                    assert_eq!(r.value, format!("msg-{i}"));
                    assert_eq!(r.offset, i as u64);
                }
            }

            #[test]
            fn poll_drains_then_follows_the_tail() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                for i in 0..3 {
                    producer
                        .send(&subject::record::ProducerRecord::new(
                            "tail",
                            None,
                            format!("v{i}"),
                        ))
                        .unwrap();
                }

                let mut consumer = earliest(&broker, "g");
                consumer.subscribe(&["tail"]).unwrap();

                assert_eq!(consumer.poll(Duration::from_millis(50)).unwrap().len(), 3);
                assert_eq!(
                    consumer.poll(Duration::from_millis(50)).unwrap().len(),
                    0,
                    "drained consumer polls empty"
                );

                producer
                    .send(&subject::record::ProducerRecord::new("tail", None, "v3"))
                    .unwrap();
                producer
                    .send(&subject::record::ProducerRecord::new("tail", None, "v4"))
                    .unwrap();

                let new = consumer.poll(Duration::from_millis(50)).unwrap();
                assert_eq!(
                    new.len(),
                    2,
                    "poll picks up records appended after subscribe"
                );
                assert_eq!(new[0].value, "v3");
                assert_eq!(new[1].value, "v4");
            }

            #[test]
            fn empty_topic_polls_empty() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                broker.ensure_topic("nothing-here").unwrap();

                let mut consumer = earliest(&broker, "g");
                consumer.subscribe(&["nothing-here"]).unwrap();
                assert!(consumer.poll(Duration::from_millis(50)).unwrap().is_empty());
            }

            #[test]
            fn topics_are_isolated() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                producer
                    .send(&subject::record::ProducerRecord::new("alpha", None, "a"))
                    .unwrap();
                producer
                    .send(&subject::record::ProducerRecord::new("beta", None, "b"))
                    .unwrap();

                let mut consumer = earliest(&broker, "g");
                consumer.subscribe(&["alpha"]).unwrap();
                let records = consumer.poll(Duration::from_millis(50)).unwrap();

                assert_eq!(records.len(), 1);
                assert_eq!(records[0].topic, "alpha");
                assert_eq!(records[0].value, "a");
            }

            #[test]
            fn closed_consumer_rejects_operations() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();

                let mut consumer = earliest(&broker, "g");
                consumer.subscribe(&["t"]).unwrap();
                consumer.close().unwrap();

                assert!(consumer.poll(Duration::from_millis(10)).is_err());
                assert!(consumer.subscribe(&["t"]).is_err());
                assert!(consumer.commit_sync().is_err());
                assert!(consumer.close().is_ok(), "close is idempotent");
            }
        }

        mod cert_groups_offsets {
            use std::time::Duration;
            use $fresh_site as fresh_site;
            use $subject as subject;

            fn consumer(
                broker: &subject::broker::BrokerRef,
                group: &str,
                auto_commit: bool,
                offset_reset: subject::consumer::OffsetReset,
            ) -> subject::consumer::Consumer {
                subject::broker::Broker::consumer(
                    broker,
                    subject::consumer::ConsumerConfig {
                        group_id: group.into(),
                        auto_commit,
                        offset_reset,
                    },
                )
            }

            fn produce_n(broker: &subject::broker::BrokerRef, topic: &str, n: usize) {
                let producer = subject::broker::Broker::producer(broker);
                for i in 0..n {
                    producer
                        .send(&subject::record::ProducerRecord::new(
                            topic,
                            None,
                            format!("v{i}"),
                        ))
                        .unwrap();
                }
            }

            #[test]
            fn committed_group_resumes_after_committed_offset() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 3);

                let mut a = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                a.subscribe(&["t"]).unwrap();
                assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
                a.commit_sync().unwrap();
                a.close().unwrap();

                let mut b = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                b.subscribe(&["t"]).unwrap();
                assert_eq!(
                    b.poll(Duration::from_millis(50)).unwrap().len(),
                    0,
                    "same group starts after the committed offset"
                );

                produce_n(&broker, "t", 2);
                assert_eq!(b.poll(Duration::from_millis(50)).unwrap().len(), 2);
            }

            #[test]
            fn uncommitted_positions_are_not_persisted() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 3);

                let mut a = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                a.subscribe(&["t"]).unwrap();
                assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
                a.close().unwrap(); // no commit, auto_commit off

                let mut b = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                b.subscribe(&["t"]).unwrap();
                assert_eq!(
                    b.poll(Duration::from_millis(50)).unwrap().len(),
                    3,
                    "unchanged group position: records are redelivered"
                );
            }

            #[test]
            fn groups_are_independent() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 3);

                let mut a = consumer(
                    &broker,
                    "g1",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                a.subscribe(&["t"]).unwrap();
                assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
                a.commit_sync().unwrap();

                let mut b = consumer(
                    &broker,
                    "g2",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                b.subscribe(&["t"]).unwrap();
                assert_eq!(
                    b.poll(Duration::from_millis(50)).unwrap().len(),
                    3,
                    "another group's commit is invisible"
                );
            }

            #[test]
            fn offset_reset_latest_skips_history() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 3);

                let mut c = consumer(&broker, "g", false, subject::consumer::OffsetReset::Latest);
                c.subscribe(&["t"]).unwrap();
                assert_eq!(
                    c.poll(Duration::from_millis(50)).unwrap().len(),
                    0,
                    "Latest sees nothing before subscribe"
                );

                produce_n(&broker, "t", 2);
                assert_eq!(c.poll(Duration::from_millis(50)).unwrap().len(), 2);
            }

            #[test]
            fn committed_offset_overrides_reset_policy() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 2);

                let mut a = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                a.subscribe(&["t"]).unwrap();
                assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 2);
                a.commit_sync().unwrap();
                a.close().unwrap();

                produce_n(&broker, "t", 2);

                // Latest policy, but the group has a committed offset — it must win.
                let mut b = consumer(&broker, "g", false, subject::consumer::OffsetReset::Latest);
                b.subscribe(&["t"]).unwrap();
                assert_eq!(
                    b.poll(Duration::from_millis(50)).unwrap().len(),
                    2,
                    "committed offset beats the reset policy"
                );
            }

            #[test]
            fn auto_commit_commits_on_close() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                produce_n(&broker, "t", 3);

                let mut a = consumer(&broker, "g", true, subject::consumer::OffsetReset::Earliest);
                a.subscribe(&["t"]).unwrap();
                assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
                a.close().unwrap();

                let mut b = consumer(
                    &broker,
                    "g",
                    false,
                    subject::consumer::OffsetReset::Earliest,
                );
                b.subscribe(&["t"]).unwrap();
                assert_eq!(
                    b.poll(Duration::from_millis(50)).unwrap().len(),
                    0,
                    "auto_commit consumer committed on close"
                );
            }
        }

        mod cert_persistence {
            use std::time::Duration;
            use $fresh_site as fresh_site;
            use $subject as subject;

            fn config(group: &str) -> subject::consumer::ConsumerConfig {
                subject::consumer::ConsumerConfig {
                    group_id: group.into(),
                    auto_commit: false,
                    offset_reset: subject::consumer::OffsetReset::Earliest,
                }
            }

            #[test]
            fn records_survive_reopen() {
                let site = fresh_site();

                {
                    let broker = subject::broker::Broker::open(site.config()).unwrap();
                    let producer = subject::broker::Broker::producer(&broker);
                    for i in 0..3 {
                        producer
                            .send(&subject::record::ProducerRecord::new(
                                "t",
                                Some(format!("k{i}")),
                                format!("v{i}"),
                            ))
                            .unwrap();
                    }
                }

                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let mut c = subject::broker::Broker::consumer(&broker, config("g"));
                c.subscribe(&["t"]).unwrap();
                let records = c.poll(Duration::from_millis(50)).unwrap();

                assert_eq!(records.len(), 3);
                for (i, r) in records.iter().enumerate() {
                    assert_eq!(r.value, format!("v{i}"));
                    assert_eq!(r.key.as_deref(), Some(format!("k{i}").as_str()));
                    assert_eq!(r.offset, i as u64);
                }
            }

            #[test]
            fn committed_offsets_survive_reopen() {
                let site = fresh_site();

                {
                    let broker = subject::broker::Broker::open(site.config()).unwrap();
                    let producer = subject::broker::Broker::producer(&broker);
                    for i in 0..3 {
                        producer
                            .send(&subject::record::ProducerRecord::new(
                                "t",
                                None,
                                format!("v{i}"),
                            ))
                            .unwrap();
                    }
                    let mut c = subject::broker::Broker::consumer(&broker, config("g"));
                    c.subscribe(&["t"]).unwrap();
                    assert_eq!(c.poll(Duration::from_millis(50)).unwrap().len(), 3);
                    c.commit_sync().unwrap();
                    c.close().unwrap();
                }

                let broker = subject::broker::Broker::open(site.config()).unwrap();

                let mut same_group = subject::broker::Broker::consumer(&broker, config("g"));
                same_group.subscribe(&["t"]).unwrap();
                assert_eq!(
                    same_group.poll(Duration::from_millis(50)).unwrap().len(),
                    0,
                    "committed offset survived reopen"
                );

                let mut new_group = subject::broker::Broker::consumer(&broker, config("fresh"));
                new_group.subscribe(&["t"]).unwrap();
                assert_eq!(
                    new_group.poll(Duration::from_millis(50)).unwrap().len(),
                    3,
                    "records themselves are still all there"
                );
            }

            #[test]
            fn topic_partition_count_survives_reopen() {
                let site = fresh_site();

                {
                    let broker = subject::broker::Broker::open(site.config()).unwrap();
                    broker.create_topic("wide", 3).unwrap();
                    let producer = subject::broker::Broker::producer(&broker);
                    producer
                        .send(&subject::record::ProducerRecord::new(
                            "wide",
                            Some("k".into()),
                            "v",
                        ))
                        .unwrap();
                }

                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let topic = broker.topic("wide").expect("topic survives reopen");
                assert_eq!(topic.num_partitions(), 3);
            }
        }

        mod cert_partitioning {
            use std::collections::HashMap;
            use std::time::Duration;
            use $fresh_site as fresh_site;
            use $subject as subject;

            #[test]
            fn same_key_always_routes_to_same_partition() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                broker.create_topic("routed", 4).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                let mut key_to_partition: HashMap<String, u32> = HashMap::new();
                for round in 0..5 {
                    for key in ["a", "b", "c", "d", "e"] {
                        let rec = producer
                            .send(&subject::record::ProducerRecord::new(
                                "routed",
                                Some(key.into()),
                                format!("{key}-{round}"),
                            ))
                            .unwrap();
                        let prev = key_to_partition.insert(key.into(), rec.partition);
                        if let Some(prev) = prev {
                            assert_eq!(prev, rec.partition, "key {key} switched partitions");
                        }
                    }
                }
            }

            #[test]
            fn offsets_are_monotonic_per_partition() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                broker.create_topic("routed", 4).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                let mut per_partition: HashMap<u32, Vec<u64>> = HashMap::new();
                for i in 0..40 {
                    let rec = producer
                        .send(&subject::record::ProducerRecord::new(
                            "routed",
                            Some(format!("k{}", i % 7)),
                            format!("v{i}"),
                        ))
                        .unwrap();
                    per_partition
                        .entry(rec.partition)
                        .or_default()
                        .push(rec.offset);
                }

                assert!(per_partition.len() > 1, "keys spread across partitions");
                for (partition, offsets) in per_partition {
                    for (expected, actual) in offsets.iter().enumerate() {
                        assert_eq!(
                            *actual, expected as u64,
                            "partition {partition} offsets are dense and ordered"
                        );
                    }
                }
            }

            #[test]
            fn consumer_sees_every_partition() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                broker.create_topic("routed", 4).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                for i in 0..20 {
                    producer
                        .send(&subject::record::ProducerRecord::new(
                            "routed",
                            Some(format!("k{i}")),
                            format!("v{i}"),
                        ))
                        .unwrap();
                }

                let mut c = subject::broker::Broker::consumer(
                    &broker,
                    subject::consumer::ConsumerConfig {
                        group_id: "g".into(),
                        auto_commit: false,
                        offset_reset: subject::consumer::OffsetReset::Earliest,
                    },
                );
                c.subscribe(&["routed"]).unwrap();
                let records = c.poll(Duration::from_millis(50)).unwrap();

                assert_eq!(records.len(), 20, "no partition's records are lost");

                // Within each partition, delivery preserves append order.
                let mut per_partition: HashMap<u32, Vec<u64>> = HashMap::new();
                for r in &records {
                    per_partition.entry(r.partition).or_default().push(r.offset);
                }
                for (partition, offsets) in per_partition {
                    let mut sorted = offsets.clone();
                    sorted.sort_unstable();
                    assert_eq!(offsets, sorted, "partition {partition} delivered in order");
                }
            }
        }

        mod cert_merkle {
            use $fresh_site as fresh_site;
            use $subject as subject;

            #[test]
            fn root_exists_after_append_and_changes_with_growth() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                producer
                    .send(&subject::record::ProducerRecord::new("m", None, "one"))
                    .unwrap();
                let topic = broker.topic("m").unwrap();
                let part_arc = topic.partition(0).unwrap();

                let root1 = part_arc
                    .read()
                    .unwrap()
                    .merkle_root()
                    .unwrap()
                    .expect("root exists after first append");

                producer
                    .send(&subject::record::ProducerRecord::new("m", None, "two"))
                    .unwrap();
                let root2 = part_arc
                    .read()
                    .unwrap()
                    .merkle_root()
                    .unwrap()
                    .expect("root exists");

                assert_ne!(
                    root1.to_hex(),
                    root2.to_hex(),
                    "root moves as the log grows"
                );
            }

            #[test]
            fn every_offset_has_a_verifying_proof() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);

                for i in 0..5 {
                    producer
                        .send(&subject::record::ProducerRecord::new(
                            "m",
                            None,
                            format!("v{i}"),
                        ))
                        .unwrap();
                }

                let topic = broker.topic("m").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let part = part_arc.read().unwrap();

                for offset in 0..5u64 {
                    let proof = part
                        .proof(offset)
                        .unwrap()
                        .unwrap_or_else(|| panic!("proof exists for offset {offset}"));
                    assert!(
                        part.verify_proof(&proof).unwrap(),
                        "proof for offset {offset} verifies against the root"
                    );
                }
            }

            #[test]
            fn out_of_range_offset_has_no_proof() {
                let site = fresh_site();
                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let producer = subject::broker::Broker::producer(&broker);
                producer
                    .send(&subject::record::ProducerRecord::new("m", None, "only"))
                    .unwrap();

                let topic = broker.topic("m").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let part = part_arc.read().unwrap();

                assert!(part.proof(99).unwrap().is_none());
            }

            #[test]
            fn merkle_root_survives_reopen() {
                let site = fresh_site();

                let root_before = {
                    let broker = subject::broker::Broker::open(site.config()).unwrap();
                    let producer = subject::broker::Broker::producer(&broker);
                    for i in 0..3 {
                        producer
                            .send(&subject::record::ProducerRecord::new(
                                "m",
                                None,
                                format!("v{i}"),
                            ))
                            .unwrap();
                    }
                    let topic = broker.topic("m").unwrap();
                    let part_arc = topic.partition(0).unwrap();
                    let root = part_arc.read().unwrap().merkle_root().unwrap().unwrap();
                    root.to_hex()
                };

                let broker = subject::broker::Broker::open(site.config()).unwrap();
                let topic = broker.topic("m").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let root_after = part_arc
                    .read()
                    .unwrap()
                    .merkle_root()
                    .unwrap()
                    .expect("root exists after reopen")
                    .to_hex();

                assert_eq!(root_before, root_after, "reopen does not perturb the root");
            }
        }

        mod cert_format {
            use $subject as subject;

            fn fixed_record() -> subject::record::Record {
                subject::record::Record {
                    key: Some("golden-key".into()),
                    value: r#"{"n":42}"#.into(),
                    topic: "golden".into(),
                    partition: 1,
                    offset: 7,
                    timestamp: ::chrono::DateTime::<::chrono::Utc>::from_timestamp(
                        1_700_000_000,
                        0,
                    )
                    .unwrap(),
                }
            }

            #[test]
            fn record_serialization_round_trips() {
                let record = fixed_record();
                let bytes = record.serialize();
                let back = subject::record::Record::deserialize(&bytes).unwrap();

                assert_eq!(back.key, record.key);
                assert_eq!(back.value, record.value);
                assert_eq!(back.topic, record.topic);
                assert_eq!(back.partition, record.partition);
                assert_eq!(back.offset, record.offset);
                assert_eq!(back.timestamp, record.timestamp);
            }

            #[test]
            fn record_hash_is_pinned() {
                // Golden value from the reference implementation. If this fails,
                // the wire format changed: cross-implementation verification and
                // every existing log on disk break. That is a breaking release.
                assert_eq!(
                    fixed_record().hash().to_hex(),
                    "1acc6b28eef646fde3348f1eea22a50895605e60a49d929333abdca695b16787"
                );
            }

            #[test]
            fn hash_hex_round_trips() {
                let h = subject::hash::Hash::digest(b"merk");
                let hex = h.to_hex();
                assert_eq!(hex.len(), 64);
                let back = subject::hash::Hash::from_hex(&hex).unwrap();
                assert_eq!(back.to_hex(), hex);
            }

            #[test]
            fn hash_digest_is_pinned() {
                // SHA-256 of the literal bytes "merk" (verified against sha256sum)
                // — pins the hash algorithm itself.
                assert_eq!(
                    subject::hash::Hash::digest(b"merk").to_hex(),
                    "357d3a1111b12e15ea855ff09a8bc0b20a814667dd56d15ab90fe6667f17e9da"
                );
            }
        }
    };
}
