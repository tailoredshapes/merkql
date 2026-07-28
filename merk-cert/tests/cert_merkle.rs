//! Contract: merkle integrity — every record provably included under the root.

use merk_cert::site::fresh_site;
use merk_cert::subject::broker::Broker;
use merk_cert::subject::record::ProducerRecord;

#[test]
fn root_exists_after_append_and_changes_with_growth() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    producer.send(&ProducerRecord::new("m", None, "one")).unwrap();
    let topic = broker.topic("m").unwrap();
    let part_arc = topic.partition(0).unwrap();

    let root1 = part_arc
        .read()
        .unwrap()
        .merkle_root()
        .unwrap()
        .expect("root exists after first append");

    producer.send(&ProducerRecord::new("m", None, "two")).unwrap();
    let root2 = part_arc
        .read()
        .unwrap()
        .merkle_root()
        .unwrap()
        .expect("root exists");

    assert_ne!(root1.to_hex(), root2.to_hex(), "root moves as the log grows");
}

#[test]
fn every_offset_has_a_verifying_proof() {
    let site = fresh_site();
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);

    for i in 0..5 {
        producer
            .send(&ProducerRecord::new("m", None, format!("v{i}")))
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
    let broker = Broker::open(site.config()).unwrap();
    let producer = Broker::producer(&broker);
    producer.send(&ProducerRecord::new("m", None, "only")).unwrap();

    let topic = broker.topic("m").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let part = part_arc.read().unwrap();

    assert!(part.proof(99).unwrap().is_none());
}

#[test]
fn merkle_root_survives_reopen() {
    let site = fresh_site();

    let root_before = {
        let broker = Broker::open(site.config()).unwrap();
        let producer = Broker::producer(&broker);
        for i in 0..3 {
            producer
                .send(&ProducerRecord::new("m", None, format!("v{i}")))
                .unwrap();
        }
        let topic = broker.topic("m").unwrap();
        let part_arc = topic.partition(0).unwrap();
        let root = part_arc.read().unwrap().merkle_root().unwrap().unwrap();
        root.to_hex()
    };

    let broker = Broker::open(site.config()).unwrap();
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
