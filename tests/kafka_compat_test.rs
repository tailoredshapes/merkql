use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
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

/// Reproduce the meshql `consumeLoop()` pattern:
/// produce to multiple topics, consume in dependency order with commit_sync between phases.
#[test]
fn multi_phase_cdc_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);

    // Phase 1 topics: reference data
    let topics_phase1 = ["cdc.public.country", "cdc.public.fuel_type"];
    // Phase 2 topics: entities depending on reference data
    let topics_phase2 = ["cdc.public.power_plant", "cdc.public.generation_data"];

    // Produce records to all topics
    for topic in topics_phase1.iter().chain(topics_phase2.iter()) {
        for i in 0..5 {
            let pr = ProducerRecord::new(
                *topic,
                Some(format!("key-{}", i)),
                format!(r#"{{"id": {}, "topic": "{}"}}"#, i, topic),
            );
            producer.send(&pr).unwrap();
        }
    }

    let consumer_config = ConsumerConfig {
        group_id: "legacy-processor".into(),
        auto_commit: false,
        offset_reset: OffsetReset::Earliest,
    };

    // Phase 1: consume reference data
    {
        let mut consumer = Broker::consumer(&broker, consumer_config.clone());
        consumer.subscribe(topics_phase1.as_ref()).unwrap();

        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 10); // 5 per topic × 2 topics
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    // Phase 2: consume entity data — offsets should continue from where phase 1 left off
    {
        let mut consumer = Broker::consumer(&broker, consumer_config.clone());
        consumer.subscribe(topics_phase2.as_ref()).unwrap();

        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 10);
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    // Phase 3: re-subscribe to phase 1 topics — should get nothing (offsets committed)
    {
        let mut consumer = Broker::consumer(&broker, consumer_config.clone());
        consumer.subscribe(topics_phase1.as_ref()).unwrap();

        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 0, "should not re-consume committed records");
    }
}

/// The drain pattern: poll until N consecutive empty results.
#[test]
fn drain_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    for i in 0..20 {
        let pr = ProducerRecord::new("drain-topic", None, format!("record-{}", i));
        producer.send(&pr).unwrap();
    }

    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "drain-group".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&["drain-topic"]).unwrap();

    let mut all_records = Vec::new();
    let mut empty_count = 0;
    let max_empty = 3;

    loop {
        let batch = consumer.poll(Duration::from_millis(10)).unwrap();
        if batch.is_empty() {
            empty_count += 1;
            if empty_count >= max_empty {
                break;
            }
        } else {
            empty_count = 0;
            all_records.extend(batch);
        }
    }

    assert_eq!(all_records.len(), 20);
}

/// Two independent consumer groups consuming the same topic.
#[test]
fn multiple_consumer_groups() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    for i in 0..5 {
        let pr = ProducerRecord::new("shared-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    // Group A consumes all
    let mut consumer_a = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "group-a".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer_a.subscribe(&["shared-topic"]).unwrap();
    let records_a = consumer_a.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(records_a.len(), 5);
    consumer_a.commit_sync().unwrap();

    // Group B also consumes all (independent offsets)
    let mut consumer_b = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "group-b".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer_b.subscribe(&["shared-topic"]).unwrap();
    let records_b = consumer_b.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(records_b.len(), 5);
}

/// Persistence: produce → consume → commit → drop broker → reopen → resume.
#[test]
fn persistence_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: produce and partially consume
    {
        let broker = setup_broker(dir.path());
        let producer = Broker::producer(&broker);
        for i in 0..10 {
            let pr = ProducerRecord::new("persist-topic", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }

        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "persist-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["persist-topic"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 10);
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
        // broker dropped here
    }

    // Phase 2: reopen broker, consumer should resume from committed offset
    {
        let broker = setup_broker(dir.path());

        // Produce 5 more
        let producer = Broker::producer(&broker);
        for i in 10..15 {
            let pr = ProducerRecord::new("persist-topic", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }

        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "persist-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["persist-topic"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 5, "should only get records 10-14");
        assert_eq!(records[0].value, "v10");
    }
}

/// Debezium envelope passthrough: produce actual CDC JSON, verify byte-for-byte preservation.
#[test]
fn debezium_envelope_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let cdc_json = r#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","field":"id"},{"type":"string","field":"name"}],"name":"Value","field":"after"}],"name":"Envelope"},"payload":{"after":{"id":1,"name":"Test Plant"}}}"#;

    let producer = Broker::producer(&broker);
    let pr = ProducerRecord::new("cdc.public.plants", Some("1".into()), cdc_json);
    producer.send(&pr).unwrap();

    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "cdc-consumer".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&["cdc.public.plants"]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].value, cdc_json,
        "CDC JSON must be preserved byte-for-byte"
    );
    assert_eq!(records[0].key, Some("1".into()));
    assert_eq!(records[0].topic, "cdc.public.plants");
}

/// Auto-create topics via producer.
#[test]
fn auto_create_topics() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let producer = Broker::producer(&broker);
    // Topic doesn't exist yet — auto-create
    let pr = ProducerRecord::new("new-topic", None, "first");
    let record = producer.send(&pr).unwrap();
    assert_eq!(record.topic, "new-topic");
    assert_eq!(record.offset, 0);
}

/// Earliest vs Latest offset reset.
#[test]
fn offset_reset_latest() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    // Produce some records
    let producer = Broker::producer(&broker);
    for i in 0..5 {
        let pr = ProducerRecord::new("reset-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    // Consumer with Latest should see nothing from before subscribe
    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "latest-group".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Latest,
        },
    );
    consumer.subscribe(&["reset-topic"]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(records.len(), 0, "Latest should skip existing records");
}
