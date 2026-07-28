//! Contract: everything survives close-and-reopen of the same site.

use merk_cert::site::fresh_site;
use merk_cert::subject::broker::Broker;
use merk_cert::subject::consumer::{ConsumerConfig, OffsetReset};
use merk_cert::subject::record::ProducerRecord;
use std::time::Duration;

fn config(group: &str) -> ConsumerConfig {
    ConsumerConfig {
        group_id: group.into(),
        auto_commit: false,
        offset_reset: OffsetReset::Earliest,
    }
}

#[test]
fn records_survive_reopen() {
    let site = fresh_site();

    {
        let broker = Broker::open(site.config()).unwrap();
        let producer = Broker::producer(&broker);
        for i in 0..3 {
            producer
                .send(&ProducerRecord::new("t", Some(format!("k{i}")), format!("v{i}")))
                .unwrap();
        }
    }

    let broker = Broker::open(site.config()).unwrap();
    let mut c = Broker::consumer(&broker, config("g"));
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
        let broker = Broker::open(site.config()).unwrap();
        let producer = Broker::producer(&broker);
        for i in 0..3 {
            producer
                .send(&ProducerRecord::new("t", None, format!("v{i}")))
                .unwrap();
        }
        let mut c = Broker::consumer(&broker, config("g"));
        c.subscribe(&["t"]).unwrap();
        assert_eq!(c.poll(Duration::from_millis(50)).unwrap().len(), 3);
        c.commit_sync().unwrap();
        c.close().unwrap();
    }

    let broker = Broker::open(site.config()).unwrap();

    let mut same_group = Broker::consumer(&broker, config("g"));
    same_group.subscribe(&["t"]).unwrap();
    assert_eq!(
        same_group.poll(Duration::from_millis(50)).unwrap().len(),
        0,
        "committed offset survived reopen"
    );

    let mut new_group = Broker::consumer(&broker, config("fresh"));
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
        let broker = Broker::open(site.config()).unwrap();
        broker.create_topic("wide", 3).unwrap();
        let producer = Broker::producer(&broker);
        producer.send(&ProducerRecord::new("wide", Some("k".into()), "v")).unwrap();
    }

    let broker = Broker::open(site.config()).unwrap();
    let topic = broker.topic("wide").expect("topic survives reopen");
    assert_eq!(topic.num_partitions(), 3);
}
