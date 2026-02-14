mod common;

use common::*;
use merkql::broker::Broker;
use merkql::record::ProducerRecord;
use std::fs;

/// Generates a comprehensive Jepsen-style verification report.
/// Runs all 6 checker functions and 4 benchmark measurements.
/// Outputs JSON to stdout and target/jepsen-report.json.
#[test]
fn generate_jepsen_report() {
    let mut properties = Vec::new();
    let mut benchmarks = Vec::new();

    // --- Property checks ---

    eprintln!("Running checker: Total Order...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_total_order(dir.path(), 10_000, 4));
    }

    eprintln!("Running checker: Durability...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_durability(dir.path(), 4998));
    }

    eprintln!("Running checker: Exactly-Once...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_exactly_once(dir.path(), 1000));
    }

    eprintln!("Running checker: Merkle Integrity...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_merkle_integrity(dir.path(), 10_000, 4));
    }

    eprintln!("Running checker: No Data Loss...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_no_data_loss(dir.path(), 5000));
    }

    eprintln!("Running checker: Byte Fidelity...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_byte_fidelity(dir.path()));
    }

    // --- Benchmark measurements ---

    eprintln!("Running benchmark: Append Latency (256B)...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);
        let payload = generate_payload(256);

        benchmarks.push(measure_latency("Append Latency (256B payload)", 1000, |_| {
            let pr = ProducerRecord::new("bench-append", None, payload.clone());
            producer.send(&pr).unwrap();
        }));
    }

    eprintln!("Running benchmark: Read Latency...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(&broker, "bench-read", 10_000, |i| format!("v{}", i), |_| None);

        // Sequential read benchmark
        benchmarks.push(measure_latency("Read Latency (sequential)", 10_000, |i| {
            let guard = broker.lock().unwrap();
            let topic = guard.topic("bench-read").unwrap();
            let partition = topic.partition(0).unwrap();
            let _ = partition.read(i as u64).unwrap();
        }));
    }

    eprintln!("Running benchmark: Proof Generation...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(&broker, "bench-proof", 10_000, |i| format!("v{}", i), |_| None);

        benchmarks.push(measure_latency("Proof Generate + Verify (10K log)", 1000, |i| {
            let guard = broker.lock().unwrap();
            let topic = guard.topic("bench-proof").unwrap();
            let partition = topic.partition(0).unwrap();
            let offset = (i * 7) as u64 % partition.next_offset(); // prime stride
            let proof = partition.proof(offset).unwrap().unwrap();
            let _ = merkql::tree::MerkleTree::verify_proof(&proof, partition.store()).unwrap();
        }));
    }

    eprintln!("Running benchmark: Broker Reopen...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(&broker, "bench-reopen", 10_000, |i| format!("v{}", i), |_| None);
        drop(broker);

        benchmarks.push(measure_latency("Broker Reopen (10K records)", 20, |_| {
            let _ = setup_broker(dir.path(), 1);
        }));
    }

    // --- Assemble and output report ---

    let report = JepsenReport {
        title: "merkql Jepsen-Style Verification Report".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        properties,
        benchmarks,
    };

    let json = serde_json::to_string_pretty(&report).unwrap();
    println!("{}", json);

    // Write to target/
    let target_dir = std::path::Path::new("target");
    fs::create_dir_all(target_dir).unwrap();
    fs::write(target_dir.join("jepsen-report.json"), &json).unwrap();
    eprintln!("Report written to target/jepsen-report.json");

    // Assert all properties passed
    for prop in &report.properties {
        assert!(prop.passed, "Property '{}' failed: {}", prop.name, prop.details);
    }
}
