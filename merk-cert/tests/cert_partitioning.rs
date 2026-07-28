//! Contract: key routing and per-partition ordering.

use merk_cert::site::fresh_site;
use merk_cert::subject::broker::Broker;
use merk_cert::subject::consumer::{ConsumerConfig, OffsetReset};
use merk_cert::subject::record::ProducerRecord;
use std::collections::HashMap;
use std::time::Duration;

#[test]
fn same_key_always_routes_to_same_partition() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    broker.create_topic("routed", 4).unwrap();
    let producer = Broker::producer(&broker);

    let mut key_to_partition: HashMap<String, u32> = HashMap::new();
    for round in 0..5 {
        for key in ["a", "b", "c", "d", "e"] {
            let rec = producer
                .send(&ProducerRecord::new(
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
    let broker = Broker::open(site.config()).unwrap();
    broker.create_topic("routed", 4).unwrap();
    let producer = Broker::producer(&broker);

    let mut per_partition: HashMap<u32, Vec<u64>> = HashMap::new();
    for i in 0..40 {
        let rec = producer
            .send(&ProducerRecord::new(
                "routed",
                Some(format!("k{}", i % 7)),
                format!("v{i}"),
            ))
            .unwrap();
        per_partition.entry(rec.partition).or_default().push(rec.offset);
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
    let broker = Broker::open(site.config()).unwrap();
    broker.create_topic("routed", 4).unwrap();
    let producer = Broker::producer(&broker);

    for i in 0..20 {
        producer
            .send(&ProducerRecord::new(
                "routed",
                Some(format!("k{i}")),
                format!("v{i}"),
            ))
            .unwrap();
    }

    let mut c = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "g".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
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
