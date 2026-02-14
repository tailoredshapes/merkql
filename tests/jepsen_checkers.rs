mod common;

use common::*;

#[test]
fn total_order_10k_across_4_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_total_order(dir.path(), 10_000, 4);
    assert!(result.passed, "Total Order: {}", result.details);
}

#[test]
fn durability_across_3_reopen_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_durability(dir.path(), 5000, 3);
    assert!(result.passed, "Durability: {}", result.details);
}

#[test]
fn exactly_once_with_commit_restart() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_exactly_once(dir.path(), 1000);
    assert!(result.passed, "Exactly-Once: {}", result.details);
}

#[test]
fn merkle_integrity_10k_across_4_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_merkle_integrity(dir.path(), 10_000, 4);
    assert!(result.passed, "Merkle Integrity: {}", result.details);
}

#[test]
fn no_data_loss_5k() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_no_data_loss(dir.path(), 5_000);
    assert!(result.passed, "No Data Loss: {}", result.details);
}

#[test]
fn byte_fidelity_500_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_byte_fidelity(dir.path());
    assert!(result.passed, "Byte Fidelity: {}", result.details);
    assert!(
        result.sample_size >= 500,
        "Expected at least 500 payloads, got {}",
        result.sample_size
    );
}
