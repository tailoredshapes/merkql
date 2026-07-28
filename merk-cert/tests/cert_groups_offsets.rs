//! Contract: consumer-group offset semantics.

use merk_cert::site::fresh_site;
use merk_cert::subject::broker::Broker;
use merk_cert::subject::consumer::{Consumer, ConsumerConfig, OffsetReset};
use merk_cert::subject::record::ProducerRecord;
use std::time::Duration;

fn consumer(
    broker: &merk_cert::subject::broker::BrokerRef,
    group: &str,
    auto_commit: bool,
    offset_reset: OffsetReset,
) -> Consumer {
    Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: group.into(),
            auto_commit,
            offset_reset,
        },
    )
}

fn produce_n(broker: &merk_cert::subject::broker::BrokerRef, topic: &str, n: usize) {
    let producer = Broker::producer(broker);
    for i in 0..n {
        producer
            .send(&ProducerRecord::new(topic, None, format!("v{i}")))
            .unwrap();
    }
}

#[test]
fn committed_group_resumes_after_committed_offset() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 3);

    let mut a = consumer(&broker, "g", false, OffsetReset::Earliest);
    a.subscribe(&["t"]).unwrap();
    assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
    a.commit_sync().unwrap();
    a.close().unwrap();

    let mut b = consumer(&broker, "g", false, OffsetReset::Earliest);
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
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 3);

    let mut a = consumer(&broker, "g", false, OffsetReset::Earliest);
    a.subscribe(&["t"]).unwrap();
    assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
    a.close().unwrap(); // no commit, auto_commit off

    let mut b = consumer(&broker, "g", false, OffsetReset::Earliest);
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
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 3);

    let mut a = consumer(&broker, "g1", false, OffsetReset::Earliest);
    a.subscribe(&["t"]).unwrap();
    assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
    a.commit_sync().unwrap();

    let mut b = consumer(&broker, "g2", false, OffsetReset::Earliest);
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
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 3);

    let mut c = consumer(&broker, "g", false, OffsetReset::Latest);
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
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 2);

    let mut a = consumer(&broker, "g", false, OffsetReset::Earliest);
    a.subscribe(&["t"]).unwrap();
    assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 2);
    a.commit_sync().unwrap();
    a.close().unwrap();

    produce_n(&broker, "t", 2);

    // Latest policy, but the group has a committed offset — it must win.
    let mut b = consumer(&broker, "g", false, OffsetReset::Latest);
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
    let broker = Broker::open(site.config()).unwrap();
    produce_n(&broker, "t", 3);

    let mut a = consumer(&broker, "g", true, OffsetReset::Earliest);
    a.subscribe(&["t"]).unwrap();
    assert_eq!(a.poll(Duration::from_millis(50)).unwrap().len(), 3);
    a.close().unwrap();

    let mut b = consumer(&broker, "g", false, OffsetReset::Earliest);
    b.subscribe(&["t"]).unwrap();
    assert_eq!(
        b.poll(Duration::from_millis(50)).unwrap().len(),
        0,
        "auto_commit consumer committed on close"
    );
}
