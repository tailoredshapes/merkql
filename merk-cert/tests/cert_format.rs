//! Contract: wire-format stability across implementations.
//!
//! These pins are what make cross-implementation portability real: a segment
//! written by merk-aws must be verifiable by local merkql, which requires the
//! record serialization and hash to be byte-identical everywhere.

use chrono::{DateTime, Utc};
use merk_cert::subject::hash::Hash;
use merk_cert::subject::record::Record;

fn fixed_record() -> Record {
    Record {
        key: Some("golden-key".into()),
        value: r#"{"n":42}"#.into(),
        topic: "golden".into(),
        partition: 1,
        offset: 7,
        timestamp: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
    }
}

#[test]
fn record_serialization_round_trips() {
    let record = fixed_record();
    let bytes = record.serialize();
    let back = Record::deserialize(&bytes).unwrap();

    assert_eq!(back.key, record.key);
    assert_eq!(back.value, record.value);
    assert_eq!(back.topic, record.topic);
    assert_eq!(back.partition, record.partition);
    assert_eq!(back.offset, record.offset);
    assert_eq!(back.timestamp, record.timestamp);
}

#[test]
fn record_hash_is_pinned() {
    // Golden value — computed from the reference implementation. If this test
    // fails, the wire format changed and cross-implementation verification
    // (and every existing log on disk) breaks. That is a breaking release.
    let expected = "1acc6b28eef646fde3348f1eea22a50895605e60a49d929333abdca695b16787";
    assert_eq!(fixed_record().hash().to_hex(), expected);
}

#[test]
fn hash_hex_round_trips() {
    let h = Hash::digest(b"merk");
    let hex = h.to_hex();
    assert_eq!(hex.len(), 64);
    let back = Hash::from_hex(&hex).unwrap();
    assert_eq!(back.to_hex(), hex);
}

#[test]
fn hash_digest_is_pinned() {
    // SHA-256 of the literal bytes "merk" (verified against sha256sum) —
    // pins the hash algorithm itself.
    assert_eq!(
        Hash::digest(b"merk").to_hex(),
        "357d3a1111b12e15ea855ff09a8bc0b20a814667dd56d15ab90fe6667f17e9da"
    );
}
