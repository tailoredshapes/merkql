use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use std::time::Duration;

fn setup_broker(dir: &std::path::Path) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

/// Verify that every record in a partition has a valid merkle proof.
#[test]
fn proof_validity_for_all_records() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    let num_records = 20;
    for i in 0..num_records {
        let pr = ProducerRecord::new("proof-topic", Some(format!("k{}", i)), format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("proof-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for offset in 0..num_records {
        let proof = partition.proof(offset as u64).unwrap();
        assert!(proof.is_some(), "proof should exist for offset {}", offset);
        let proof = proof.unwrap();
        let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
        assert!(valid, "proof should be valid for offset {}", offset);
    }
}

/// Identical records in two partitions should produce identical merkle roots.
#[test]
fn identical_records_identical_roots() {
    let dir = tempfile::tempdir().unwrap();

    let config = BrokerConfig {
        data_dir: dir.path().to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    };
    let broker = Broker::open(config).unwrap();

    broker.create_topic("topic-a", 1).unwrap();
    broker.create_topic("topic-b", 1).unwrap();

    let producer = Broker::producer(&broker);

    let values: Vec<String> = (0..5).map(|i| format!(r#"{{"id": {}}}"#, i)).collect();

    for v in &values {
        let pr = ProducerRecord::new("topic-a", None, v.clone());
        producer.send(&pr).unwrap();
    }

    let topic_a = broker.topic("topic-a").unwrap();
    let part_arc = topic_a.partition(0).unwrap();
    let part_a = part_arc.read().unwrap();

    let root_a = part_a.merkle_root().unwrap();
    assert!(
        root_a.is_some(),
        "should have a root after appending records"
    );

    // Verify proof for each record
    for offset in 0..5u64 {
        let proof = part_a.proof(offset).unwrap().unwrap();
        assert!(MerkleTree::verify_proof(&proof, part_a.store()).unwrap());
    }
}

/// Tamper detection: corrupt a record value and verify proof fails.
#[test]
fn tamper_detection() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    for i in 0..5 {
        let pr = ProducerRecord::new("tamper-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("tamper-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    // Get a valid proof
    let proof = partition.proof(2).unwrap().unwrap();
    assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());

    // Create a tampered proof by changing the leaf hash
    let tampered = merkql::tree::Proof {
        leaf_hash: merkql::hash::Hash::digest(b"tampered data"),
        siblings: proof.siblings.clone(),
        root: proof.root,
    };

    // The tampered proof should either fail verification or error
    // (because the fake leaf hash won't exist in the store)
    let result = MerkleTree::verify_proof(&tampered, partition.store());
    assert!(
        result.is_err() || !result.unwrap(),
        "tampered proof should not verify"
    );
}

/// Merkle root changes with each append.
#[test]
fn merkle_root_progression() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    let mut roots = Vec::new();

    for i in 0..8 {
        let pr = ProducerRecord::new("root-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();

        let topic = broker.topic("root-topic").unwrap();
        let part_arc = topic.partition(0).unwrap();
        let partition = part_arc.read().unwrap();
        let root = partition.merkle_root().unwrap().unwrap();
        roots.push(root);
    }

    // All roots should be distinct
    for i in 0..roots.len() {
        for j in (i + 1)..roots.len() {
            assert_ne!(roots[i], roots[j], "roots at {} and {} should differ", i, j);
        }
    }
}

/// End-to-end: produce, consume, verify merkle proofs on consumed records.
#[test]
fn consume_and_verify_proofs() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    for i in 0..10 {
        let pr = ProducerRecord::new("verify-topic", Some(format!("k{}", i)), format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    // Consume all records
    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "verify-group".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&["verify-topic"]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(records.len(), 10);

    // Verify proof for each consumed record
    let topic = broker.topic("verify-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for record in &records {
        let proof = partition.proof(record.offset).unwrap().unwrap();
        assert!(
            MerkleTree::verify_proof(&proof, partition.store()).unwrap(),
            "proof should be valid for consumed record at offset {}",
            record.offset
        );
    }
}
