//! Contract: produce/consume round-trips.

use merk_cert::site::fresh_site;
use merk_cert::subject::broker::Broker;
use merk_cert::subject::consumer::{Consumer, ConsumerConfig, OffsetReset};
use merk_cert::subject::record::ProducerRecord;
use std::time::Duration;

fn earliest(broker: &merk_cert::subject::broker::BrokerRef, group: &str) -> Consumer {
    Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: group.into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    )
}

#[test]
fn round_trip_preserves_record_fields() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();

    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new("events", Some("k1".into()), "hello"))
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
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    let a = producer.send(&ProducerRecord::new("t", None, "a")).unwrap();
    let b = producer.send(&ProducerRecord::new("t", None, "b")).unwrap();

    assert_eq!(a.offset, 0);
    assert_eq!(b.offset, 1);
    assert_eq!(a.value, "a");
    assert_eq!(b.value, "b");
}

#[test]
fn send_batch_assigns_sequential_offsets() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    let batch: Vec<ProducerRecord> = (0..5)
        .map(|i| ProducerRecord::new("batched", None, format!("msg-{i}")))
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
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    for i in 0..3 {
        producer
            .send(&ProducerRecord::new("tail", None, format!("v{i}")))
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

    producer.send(&ProducerRecord::new("tail", None, "v3")).unwrap();
    producer.send(&ProducerRecord::new("tail", None, "v4")).unwrap();

    let new = consumer.poll(Duration::from_millis(50)).unwrap();
    assert_eq!(new.len(), 2, "poll picks up records appended after subscribe");
    assert_eq!(new[0].value, "v3");
    assert_eq!(new[1].value, "v4");
}

#[test]
fn empty_topic_polls_empty() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    broker.ensure_topic("nothing-here").unwrap();

    let mut consumer = earliest(&broker, "g");
    consumer.subscribe(&["nothing-here"]).unwrap();
    assert!(consumer.poll(Duration::from_millis(50)).unwrap().is_empty());
}

#[test]
fn topics_are_isolated() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    producer.send(&ProducerRecord::new("alpha", None, "a")).unwrap();
    producer.send(&ProducerRecord::new("beta", None, "b")).unwrap();

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
    let broker = Broker::open(site.config()).unwrap();

    let mut consumer = earliest(&broker, "g");
    consumer.subscribe(&["t"]).unwrap();
    consumer.close().unwrap();

    assert!(consumer.poll(Duration::from_millis(10)).is_err());
    assert!(consumer.subscribe(&["t"]).is_err());
    assert!(consumer.commit_sync().is_err());
    assert!(consumer.close().is_ok(), "close is idempotent");
}
