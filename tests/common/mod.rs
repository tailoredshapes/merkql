#![allow(dead_code)]

use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::tree::MerkleTree;
use serde::Serialize;
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Broker helpers
// ---------------------------------------------------------------------------

pub fn setup_broker(dir: &Path, partitions: u32) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
    };
    Broker::open(config).unwrap()
}

pub fn produce_n(
    broker: &BrokerRef,
    topic: &str,
    n: usize,
    value_fn: impl Fn(usize) -> String,
    key_fn: impl Fn(usize) -> Option<String>,
) -> Vec<merkql::record::Record> {
    let producer = Broker::producer(broker);
    let mut records = Vec::with_capacity(n);
    for i in 0..n {
        let pr = ProducerRecord::new(topic, key_fn(i), value_fn(i));
        let record = producer.send(&pr).unwrap();
        records.push(record);
    }
    records
}

pub fn consume_all(
    broker: &BrokerRef,
    group_id: &str,
    topics: &[&str],
    reset: OffsetReset,
) -> Vec<merkql::record::Record> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: group_id.into(),
            auto_commit: false,
            offset_reset: reset,
        },
    );
    consumer.subscribe(topics).unwrap();
    consumer.poll(Duration::from_millis(100)).unwrap()
}

pub fn generate_payload(size: usize) -> String {
    if size == 0 {
        return String::new();
    }
    let base = r#"{"op":"c","ts_ms":1700000000000,"source":{"version":"2.4","connector":"postgresql","name":"legacy","ts_ms":1700000000000,"db":"legacy","schema":"public","table":"entity","txId":42,"lsn":12345},"after":{"id":1,"name":"test","data":""#;
    let suffix = r#""}}"#;
    let overhead = base.len() + suffix.len();
    if size <= overhead {
        "x".repeat(size)
    } else {
        let fill_len = size - overhead;
        format!("{}{}{}", base, "x".repeat(fill_len), suffix)
    }
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct JepsenReport {
    pub title: String,
    pub timestamp: String,
    pub properties: Vec<PropertyResult>,
    pub benchmarks: Vec<BenchmarkResult>,
}

#[derive(Debug, Serialize)]
pub struct PropertyResult {
    pub name: String,
    pub claim: String,
    pub methodology: String,
    pub sample_size: usize,
    pub passed: bool,
    pub details: String,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub name: String,
    pub unit: String,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub throughput: f64,
    pub sample_size: usize,
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

pub fn measure_latency(
    name: &str,
    n: usize,
    mut f: impl FnMut(usize),
) -> BenchmarkResult {
    let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    let overall_start = Instant::now();

    for i in 0..n {
        let start = Instant::now();
        f(i);
        let elapsed = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed);
    }

    let total_secs = overall_start.elapsed().as_secs_f64();

    BenchmarkResult {
        name: name.to_string(),
        unit: "microseconds".to_string(),
        p50: hist.value_at_quantile(0.50) as f64,
        p95: hist.value_at_quantile(0.95) as f64,
        p99: hist.value_at_quantile(0.99) as f64,
        throughput: if total_secs > 0.0 { n as f64 / total_secs } else { 0.0 },
        sample_size: n,
    }
}

// ---------------------------------------------------------------------------
// Checker functions (reusable from both tests and report generator)
// ---------------------------------------------------------------------------

/// Check 1: Total Order — partition offsets are monotonically increasing and gap-free.
pub fn check_total_order(dir: &Path, n: usize, partitions: u32) -> PropertyResult {
    let broker = setup_broker(dir, partitions);
    produce_n(&broker, "total-order", n, |i| format!("v{}", i), |_| None);

    let guard = broker.lock().unwrap();
    let topic = guard.topic("total-order").unwrap();
    let mut total_verified = 0;

    for pid in topic.partition_ids() {
        let partition = topic.partition(pid).unwrap();
        let count = partition.next_offset();
        for offset in 0..count {
            let record = partition.read(offset).unwrap();
            assert!(record.is_some(), "gap at partition {} offset {}", pid, offset);
            let record = record.unwrap();
            assert_eq!(record.offset, offset);
            assert_eq!(record.partition, pid);
            total_verified += 1;
        }
    }

    let details = format!(
        "All {} records verified across {} partitions",
        total_verified, partitions
    );

    PropertyResult {
        name: "Total Order".to_string(),
        claim: "Every partition's offsets are monotonically increasing and gap-free".to_string(),
        methodology: format!(
            "Produced {} records across {} partitions via round-robin, read all back and verified offset sequences 0..N-1",
            n, partitions
        ),
        sample_size: n,
        passed: total_verified == n,
        details,
    }
}

/// Check 2: Durability — all records survive broker close/reopen.
pub fn check_durability(dir: &Path, n: usize) -> PropertyResult {
    let records_per_cycle = n / 3;
    let mut all_values: Vec<String> = Vec::new();

    for cycle in 0..3 {
        let broker = setup_broker(dir, 1);
        let start = cycle * records_per_cycle;
        let produced = produce_n(
            &broker,
            "durability",
            records_per_cycle,
            |i| format!("v{}", start + i),
            |_| None,
        );
        all_values.extend(produced.iter().map(|r| r.value.clone()));
        // broker dropped here — simulates close
    }

    // Reopen and verify all records
    let broker = setup_broker(dir, 1);
    let consumed = consume_all(&broker, "durability-check", &["durability"], OffsetReset::Earliest);

    let consumed_values: Vec<String> = consumed.iter().map(|r| r.value.clone()).collect();
    let passed = consumed_values == all_values;

    PropertyResult {
        name: "Durability".to_string(),
        claim: "All records survive broker close/reopen cycles".to_string(),
        methodology: format!(
            "Produced {} records across 3 close/reopen cycles, verified all readable after final reopen",
            all_values.len()
        ),
        sample_size: all_values.len(),
        passed,
        details: if passed {
            format!("All {} records survived 3 reopen cycles", all_values.len())
        } else {
            format!(
                "Expected {} records, got {}",
                all_values.len(),
                consumed_values.len()
            )
        },
    }
}

/// Check 3: Exactly-Once — consumer groups deliver every record exactly once.
pub fn check_exactly_once(dir: &Path, n: usize) -> PropertyResult {
    let broker = setup_broker(dir, 1);
    produce_n(&broker, "exactly-once", n, |i| format!("v{}", i), |_| None);

    let mut all_consumed: Vec<String> = Vec::new();

    for _phase in 0..4 {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "eo-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["exactly-once"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        all_consumed.extend(records.iter().map(|r| r.value.clone()));
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    let expected: Vec<String> = (0..n).map(|i| format!("v{}", i)).collect();
    let unique: HashSet<&String> = all_consumed.iter().collect();
    let no_duplicates = unique.len() == all_consumed.len();
    let all_present = all_consumed == expected;
    let passed = no_duplicates && all_present;

    PropertyResult {
        name: "Exactly-Once".to_string(),
        claim: "Consumer groups deliver every record exactly once across commit/restart cycles"
            .to_string(),
        methodology: format!(
            "Produced {} records, consumed in 4 phases with commit_sync between each, verified union = all with no duplicates",
            n
        ),
        sample_size: n,
        passed,
        details: if passed {
            format!("All {} records consumed exactly once across 4 phases", n)
        } else {
            format!(
                "Consumed {} records, {} unique (expected {})",
                all_consumed.len(),
                unique.len(),
                n
            )
        },
    }
}

/// Check 4: Merkle Integrity — 100% of records have valid proofs.
pub fn check_merkle_integrity(dir: &Path, n: usize, partitions: u32) -> PropertyResult {
    let broker = setup_broker(dir, partitions);
    produce_n(&broker, "merkle-int", n, |i| format!("v{}", i), |_| None);

    let guard = broker.lock().unwrap();
    let topic = guard.topic("merkle-int").unwrap();
    let mut verified = 0;

    for pid in topic.partition_ids() {
        let partition = topic.partition(pid).unwrap();
        for offset in 0..partition.next_offset() {
            let proof = partition.proof(offset).unwrap();
            assert!(proof.is_some(), "no proof for partition {} offset {}", pid, offset);
            let proof = proof.unwrap();
            let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
            assert!(valid, "invalid proof for partition {} offset {}", pid, offset);
            verified += 1;
        }
    }

    PropertyResult {
        name: "Merkle Integrity".to_string(),
        claim: "100% of records have valid merkle inclusion proofs".to_string(),
        methodology: format!(
            "Produced {} records across {} partitions, generated and verified proof for every record",
            n, partitions
        ),
        sample_size: n,
        passed: verified == n,
        details: format!("All {} proofs verified across {} partitions", verified, partitions),
    }
}

/// Check 5: No Data Loss — every confirmed append is immediately readable.
pub fn check_no_data_loss(dir: &Path, n: usize) -> PropertyResult {
    let broker = setup_broker(dir, 1);
    let producer = Broker::producer(&broker);
    let mut failures = 0;

    for i in 0..n {
        let pr = ProducerRecord::new("no-loss", None, format!("v{}", i));
        let record = producer.send(&pr).unwrap();

        // Immediately read back
        let guard = broker.lock().unwrap();
        let topic = guard.topic("no-loss").unwrap();
        let partition = topic.partition(record.partition).unwrap();
        let read_back = partition.read(record.offset).unwrap();
        if read_back.is_none() || read_back.unwrap().value != format!("v{}", i) {
            failures += 1;
        }
    }

    PropertyResult {
        name: "No Data Loss".to_string(),
        claim: "Every confirmed append is immediately readable via the partition API".to_string(),
        methodology: format!(
            "Produced {} records, immediately read each back via partition.read() after send",
            n
        ),
        sample_size: n,
        passed: failures == 0,
        details: if failures == 0 {
            format!("All {} records readable immediately after append", n)
        } else {
            format!("{} of {} records not immediately readable", failures, n)
        },
    }
}

/// Check 6: Byte Fidelity — values preserved exactly for edge-case payloads.
pub fn check_byte_fidelity(dir: &Path) -> PropertyResult {
    let broker = setup_broker(dir, 1);
    let producer = Broker::producer(&broker);

    let payloads: Vec<String> = vec![
        // Empty
        String::new(),
        // Unicode
        "Hello \u{1F600} World \u{1F30D}".to_string(),
        // Large JSON
        generate_payload(64 * 1024),
        // Null bytes in string (valid UTF-8)
        "before\0after".to_string(),
        // Boundary lengths
        "x".to_string(),
        "xx".to_string(),
        generate_payload(255),
        generate_payload(256),
        generate_payload(257),
        generate_payload(1023),
        generate_payload(1024),
        generate_payload(1025),
        generate_payload(4096),
        // Nested JSON
        r#"{"a":{"b":{"c":{"d":{"e":"deep"}}}}}"#.to_string(),
        // Special chars
        "tab\there\nnewline\rcarriage".to_string(),
        // All printable ASCII
        (32u8..=126).map(|b| b as char).collect::<String>(),
    ];

    let n = payloads.len();
    for (i, payload) in payloads.iter().enumerate() {
        let pr = ProducerRecord::new("fidelity", Some(format!("k{}", i)), payload.clone());
        producer.send(&pr).unwrap();
    }

    let consumed = consume_all(&broker, "fidelity-check", &["fidelity"], OffsetReset::Earliest);
    let mut failures = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        if i >= consumed.len() {
            failures.push(format!("missing record {}", i));
            continue;
        }
        if consumed[i].value != *payload {
            failures.push(format!(
                "record {} mismatch: expected {} bytes, got {} bytes",
                i,
                payload.len(),
                consumed[i].value.len()
            ));
        }
    }

    let passed = failures.is_empty();
    PropertyResult {
        name: "Byte Fidelity".to_string(),
        claim: "Values are preserved exactly for edge-case payloads (empty, unicode, large, boundary lengths)".to_string(),
        methodology: format!(
            "Produced {} records with edge-case payloads, consumed and compared byte-for-byte",
            n
        ),
        sample_size: n,
        passed,
        details: if passed {
            format!("All {} edge-case payloads preserved exactly", n)
        } else {
            format!("Failures: {}", failures.join("; "))
        },
    }
}
