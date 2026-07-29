//! Reproducer: multiple **processes** writing one merkql partition lose data.
//!
//! `cargo run --release --example multi_process_writers -- <dir> <writers> <each>`
//!
//! Observed on 2026-07-29 (2/4/8 writers, 50 records each):
//!   2 writers -> half the records unreadable
//!   4 writers -> the object pack is corrupt ("unknown compression marker")
//!   8 writers -> 396 of 400 records gone
//!
//! Why: `flock` serializes the critical section, but every process caches
//! `next_offset`, the object store's `write_pos`, the merkle tree and the index
//! writer in memory, and never re-reads them inside the lock. So two writers
//! assign the same offset and write at the same byte position, each overwriting
//! the other. Mutual exclusion is not enough when the state being protected
//! lives in memory rather than on disk.
//!
//! In-process concurrency is fine — `Topic` holds each partition behind an
//! `RwLock` and `append` takes `&mut self`.
use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const TOPIC: &str = "concurrent";

fn write(dir: &str, tag: &str, count: usize) -> anyhow::Result<()> {
    let broker = Broker::open(BrokerConfig::new(dir))?;
    broker.ensure_topic(TOPIC)?;
    let producer = Broker::producer(&broker);
    for i in 0..count {
        producer.send(&ProducerRecord::new(TOPIC, None, format!("{tag}-{i}")))?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("write") {
        return write(&args[2], &args[3], args[4].parse()?);
    }

    let dir = &args[1];
    let writers: usize = args[2].parse()?;
    let each: usize = args[3].parse()?;

    // Create the topic up front so every writer opens an existing one.
    {
        let broker = Broker::open(BrokerConfig::new(dir.as_str()))?;
        broker.create_topic(TOPIC, 1)?;
    }

    let exe = std::env::current_exe()?;
    let mut kids = Vec::new();
    for w in 0..writers {
        kids.push(
            std::process::Command::new(&exe)
                .args(["write", dir, &format!("w{w}"), &each.to_string()])
                .spawn()?,
        );
    }
    let mut failed = 0;
    for mut k in kids {
        if !k.wait()?.success() {
            failed += 1;
        }
    }

    let expected: HashSet<String> = (0..writers)
        .flat_map(|w| (0..each).map(move |i| format!("w{w}-{i}")))
        .collect();

    let broker = Broker::open(BrokerConfig::new(dir.as_str()))?;
    let mut consumer = Broker::consumer(&broker, ConsumerConfig {
        group_id: "probe".into(),
        auto_commit: false,
        offset_reset: OffsetReset::Earliest,
    });
    consumer.subscribe(&[TOPIC])?;
    let records = consumer.poll(Duration::from_millis(500))?;

    let mut by_offset: HashMap<u64, Vec<String>> = HashMap::new();
    for r in &records {
        by_offset.entry(r.offset).or_default().push(r.value.clone());
    }
    let found: HashSet<String> = records.iter().map(|r| r.value.clone()).collect();
    let dup_offsets: Vec<_> = by_offset.iter().filter(|(_, v)| v.len() > 1).collect();
    let missing: Vec<_> = expected.difference(&found).cloned().collect();

    println!("writers={writers} each={each}  child failures={failed}");
    println!("expected {} distinct records", expected.len());
    println!("readable {} records, {} distinct values", records.len(), found.len());
    println!("offsets with >1 record: {}", dup_offsets.len());
    println!("LOST records: {}", missing.len());
    if !missing.is_empty() {
        let mut sample: Vec<_> = missing.iter().take(5).collect();
        sample.sort();
        println!("  e.g. {sample:?}");
    }
    println!(
        "VERDICT: {}",
        if missing.is_empty() && dup_offsets.is_empty() {
            "no loss"
        } else {
            "DATA LOSS"
        }
    );
    Ok(())
}
