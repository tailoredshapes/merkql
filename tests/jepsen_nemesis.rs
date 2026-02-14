mod common;

use common::*;
use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::OffsetReset;
use merkql::record::ProducerRecord;
use std::fs;

fn broker_config(dir: &std::path::Path) -> BrokerConfig {
    BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
    }
}

/// Nemesis 1: Crash simulation — drop broker without close across 10 cycles.
/// Documents: does merkql recover all records after ungraceful drops?
#[test]
fn crash_drop_without_close() {
    let dir = tempfile::tempdir().unwrap();
    let records_per_cycle = 100;
    let cycles = 10;
    let mut expected_total = 0;

    for cycle in 0..cycles {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        let producer = Broker::producer(&broker);
        for i in 0..records_per_cycle {
            let pr = ProducerRecord::new(
                "crash-topic",
                None,
                format!("cycle{}-rec{}", cycle, i),
            );
            producer.send(&pr).unwrap();
        }
        expected_total += records_per_cycle;
        // broker is dropped here without explicit close — simulates crash
        drop(broker);
    }

    // Reopen and count recovered records
    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let consumed = consume_all(&broker, "crash-verify", &["crash-topic"], OffsetReset::Earliest);

    eprintln!(
        "[nemesis::crash] Expected {} records, recovered {}",
        expected_total,
        consumed.len()
    );
    assert_eq!(
        consumed.len(),
        expected_total,
        "All records should survive ungraceful broker drops"
    );
}

/// Nemesis 2: Truncated tree.snapshot — halve the snapshot file and reopen.
/// Documents: does merkql error or silently diverge?
#[test]
fn truncated_tree_snapshot() {
    let dir = tempfile::tempdir().unwrap();

    // Produce records
    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "trunc-snap", 100, |i| format!("v{}", i), |_| None);
    }

    // Find and truncate tree.snapshot
    let snapshot_path = find_file_recursive(dir.path(), "tree.snapshot");
    assert!(
        snapshot_path.is_some(),
        "tree.snapshot should exist after producing records"
    );
    let snapshot_path = snapshot_path.unwrap();
    let original_len = fs::metadata(&snapshot_path).unwrap().len();
    let truncated_len = original_len / 2;

    // Read, truncate, write back
    let data = fs::read(&snapshot_path).unwrap();
    fs::write(&snapshot_path, &data[..truncated_len as usize]).unwrap();

    eprintln!(
        "[nemesis::truncated_snapshot] Truncated tree.snapshot from {} to {} bytes",
        original_len, truncated_len
    );

    // Attempt to reopen
    let result = Broker::open(broker_config(dir.path()));
    match result {
        Ok(broker) => {
            // Try to read records
            let guard = broker.lock().unwrap();
            let topic = guard.topic("trunc-snap");
            match topic {
                Some(t) => {
                    let partition = t.partition(0).unwrap();
                    eprintln!(
                        "[nemesis::truncated_snapshot] Broker reopened. Partition reports next_offset={}",
                        partition.next_offset()
                    );
                    // Try reading records
                    let mut readable = 0;
                    for offset in 0..partition.next_offset() {
                        if partition.read(offset).is_ok() {
                            readable += 1;
                        }
                    }
                    eprintln!(
                        "[nemesis::truncated_snapshot] {} of {} records readable after snapshot truncation",
                        readable,
                        partition.next_offset()
                    );
                }
                None => {
                    eprintln!("[nemesis::truncated_snapshot] Topic not found after reopen");
                }
            }
        }
        Err(e) => {
            eprintln!(
                "[nemesis::truncated_snapshot] Broker failed to reopen: {}",
                e
            );
        }
    }
    // This test is observational — it documents behavior, not asserts correctness
}

/// Nemesis 3: Truncated offsets.idx — remove 16 bytes (half an entry) from the index.
/// Documents: does merkql lose a record or error?
#[test]
fn truncated_offsets_index() {
    let dir = tempfile::tempdir().unwrap();

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "trunc-idx", 100, |i| format!("v{}", i), |_| None);
    }

    // Find and truncate offsets.idx
    let index_path = find_file_recursive(dir.path(), "offsets.idx");
    assert!(
        index_path.is_some(),
        "offsets.idx should exist after producing records"
    );
    let index_path = index_path.unwrap();
    let original_len = fs::metadata(&index_path).unwrap().len();
    let truncated_len = original_len - 16; // Remove half an entry (entry = 32 bytes)

    let data = fs::read(&index_path).unwrap();
    fs::write(&index_path, &data[..truncated_len as usize]).unwrap();

    eprintln!(
        "[nemesis::truncated_index] Truncated offsets.idx from {} to {} bytes ({} -> {} entries)",
        original_len,
        truncated_len,
        original_len / 32,
        truncated_len / 32
    );

    // Reopen and check
    let result = Broker::open(broker_config(dir.path()));
    match result {
        Ok(broker) => {
            let guard = broker.lock().unwrap();
            let topic = guard.topic("trunc-idx").unwrap();
            let partition = topic.partition(0).unwrap();
            let reported_offset = partition.next_offset();
            eprintln!(
                "[nemesis::truncated_index] Partition reports next_offset={} (expected 100, index has {} complete entries)",
                reported_offset,
                truncated_len / 32
            );

            // Try reading all records up to reported offset
            let mut readable = 0;
            let mut errors = 0;
            for offset in 0..reported_offset {
                match partition.read(offset) {
                    Ok(Some(_)) => readable += 1,
                    Ok(None) => {}
                    Err(_) => errors += 1,
                }
            }
            eprintln!(
                "[nemesis::truncated_index] {} readable, {} errors out of {} attempted",
                readable, errors, reported_offset
            );
        }
        Err(e) => {
            eprintln!("[nemesis::truncated_index] Broker failed to reopen: {}", e);
        }
    }
}

/// Nemesis 4: Missing HEAD — delete HEAD file and reopen.
/// Documents: recovery or failure?
#[test]
fn missing_head() {
    let dir = tempfile::tempdir().unwrap();

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "no-head", 100, |i| format!("v{}", i), |_| None);
    }

    // Delete HEAD
    let head_path = find_file_recursive(dir.path(), "HEAD");
    assert!(head_path.is_some(), "HEAD should exist after producing records");
    let head_path = head_path.unwrap();
    fs::remove_file(&head_path).unwrap();
    eprintln!("[nemesis::missing_head] Deleted HEAD file");

    // Reopen and check
    let result = Broker::open(broker_config(dir.path()));
    match result {
        Ok(broker) => {
            let guard = broker.lock().unwrap();
            let topic = guard.topic("no-head").unwrap();
            let partition = topic.partition(0).unwrap();
            eprintln!(
                "[nemesis::missing_head] Broker reopened. next_offset={}",
                partition.next_offset()
            );

            // Verify records are still readable
            let mut readable = 0;
            for offset in 0..partition.next_offset() {
                if partition.read(offset).unwrap().is_some() {
                    readable += 1;
                }
            }
            eprintln!(
                "[nemesis::missing_head] {} of {} records readable without HEAD",
                readable,
                partition.next_offset()
            );

            // Try appending new records
            drop(guard);
            let producer = Broker::producer(&broker);
            let pr = ProducerRecord::new("no-head", None, "after-head-delete");
            match producer.send(&pr) {
                Ok(record) => {
                    eprintln!(
                        "[nemesis::missing_head] Append after HEAD delete succeeded at offset {}",
                        record.offset
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[nemesis::missing_head] Append after HEAD delete failed: {}",
                        e
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("[nemesis::missing_head] Broker failed to reopen: {}", e);
        }
    }
}

/// Nemesis 5: Index ahead of snapshot — simulates crash between index write (line 101)
/// and snapshot write (line 117) in partition.rs.
/// The index has an entry for record N but the tree.snapshot is from state N-1.
#[test]
fn index_ahead_of_snapshot() {
    let dir = tempfile::tempdir().unwrap();

    // Produce 100 records normally
    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "idx-ahead", 100, |i| format!("v{}", i), |_| None);
    }

    // Save snapshot state after 99 records by:
    // 1. Read current snapshot
    // 2. Produce record 100
    // 3. Revert snapshot to the saved state (but index now has 101 entries)
    let snapshot_path = find_file_recursive(dir.path(), "tree.snapshot").unwrap();
    let snapshot_before = fs::read(&snapshot_path).unwrap();

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "idx-ahead", 1, |_| "extra".to_string(), |_| None);
    }

    // Revert snapshot to the pre-101 state
    fs::write(&snapshot_path, &snapshot_before).unwrap();
    eprintln!(
        "[nemesis::index_ahead] Reverted tree.snapshot to state with 100 records, index has 101 entries"
    );

    // Reopen and check
    let result = Broker::open(broker_config(dir.path()));
    match result {
        Ok(broker) => {
            let guard = broker.lock().unwrap();
            let topic = guard.topic("idx-ahead").unwrap();
            let partition = topic.partition(0).unwrap();
            eprintln!(
                "[nemesis::index_ahead] Broker reopened. next_offset={}",
                partition.next_offset()
            );

            // Check if all records including the last one are readable
            let mut readable = 0;
            for offset in 0..partition.next_offset() {
                if partition.read(offset).unwrap().is_some() {
                    readable += 1;
                }
            }
            eprintln!(
                "[nemesis::index_ahead] {} of {} records readable",
                readable,
                partition.next_offset()
            );

            // Check if proofs still work
            let mut valid_proofs = 0;
            let mut invalid_proofs = 0;
            let mut proof_errors = 0;
            for offset in 0..partition.next_offset() {
                match partition.proof(offset) {
                    Ok(Some(proof)) => {
                        match merkql::tree::MerkleTree::verify_proof(&proof, partition.store()) {
                            Ok(true) => valid_proofs += 1,
                            Ok(false) => invalid_proofs += 1,
                            Err(_) => proof_errors += 1,
                        }
                    }
                    Ok(None) => proof_errors += 1,
                    Err(_) => proof_errors += 1,
                }
            }
            eprintln!(
                "[nemesis::index_ahead] Proofs: {} valid, {} invalid, {} errors",
                valid_proofs, invalid_proofs, proof_errors
            );
        }
        Err(e) => {
            eprintln!("[nemesis::index_ahead] Broker failed to reopen: {}", e);
        }
    }
}

// Helper: recursively find a file by name
fn find_file_recursive(dir: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(found) = find_file_recursive(&path, name) {
                    return Some(found);
                }
            } else if path.file_name().map(|f| f == name).unwrap_or(false) {
                return Some(path);
            }
        }
    }
    None
}
