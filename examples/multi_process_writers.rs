//! Demonstrates that merkql refuses a second writing process rather than
//! corrupting the log.
//!
//! `cargo run --release --example multi_process_writers -- <dir> <writers> <each>`
//!
//! Expect one writer to succeed and the rest to fail with a clear message. The
//! records written by the winner are all present; nothing is silently lost.
//!
//! It was not always so. Before the writer claim existed, this same program
//! measured (2/4/8 writers, 50 records each): two writers lost half the
//! records, four corrupted the object pack outright, and eight lost 396 of 400
//! — all silently, with every process reporting success.
//!
//! The cause is worth remembering: `flock` serialized the critical section, but
//! every process cached `next_offset`, the object store's `write_pos`, the
//! merkle tree and the index writer in memory and never re-read them inside the
//! lock. Mutual exclusion is not enough when the state being protected lives in
//! memory rather than on disk. merkql now claims the lock for the writer's
//! lifetime and refuses anyone else.
//!
//! Concurrent *readers* are unaffected, and in-process concurrency is fine —
//! `Topic` holds each partition behind an `RwLock` and `append` takes
//! `&mut self`.
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
    let mut wrote = 0usize;
    for i in 0..count {
        match producer.send(&ProducerRecord::new(TOPIC, None, format!("{tag}-{i}"))) {
            Ok(_) => wrote += 1,
            Err(e) => {
                eprintln!("[{tag}] refused after {wrote} records: {}", 
                    e.to_string().lines().next().unwrap_or(""));
                return Err(e);
            }
        }
    }
    eprintln!("[{tag}] wrote {wrote}");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("write") {
        return write(&args[2], &args[3], args[4].parse()?);
    }
    // Open only: never appends. Isolates whether merely opening a partition
    // while another process writes is enough to damage the log.
    if args.get(1).map(|s| s.as_str()) == Some("open") {
        for _ in 0..20 {
            let _broker = Broker::open(BrokerConfig::new(args[2].as_str()))?;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        return Ok(());
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
    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "probe".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&[TOPIC])?;
    let records = consumer.poll(Duration::from_millis(500))?;

    let mut by_offset: HashMap<u64, Vec<String>> = HashMap::new();
    for r in &records {
        by_offset.entry(r.offset).or_default().push(r.value.clone());
    }
    let found: HashSet<String> = records.iter().map(|r| r.value.clone()).collect();
    let dup_offsets: Vec<_> = by_offset.iter().filter(|(_, v)| v.len() > 1).collect();
    let missing: Vec<_> = expected.difference(&found).cloned().collect();

    let refused = failed;
    let accepted = writers - refused;
    let expected_from_accepted = accepted * each;

    println!("writers={writers} each={each}");
    println!("  {accepted} accepted, {refused} refused");
    println!(
        "  readable {} records, {} distinct",
        records.len(),
        found.len()
    );
    println!("  offsets holding more than one record: {}", dup_offsets.len());
    println!("  expected from accepted writers: {expected_from_accepted}");

    // Records belonging to a refused writer were never written — that is the
    // point. What must not happen is losing a record an accepted writer wrote,
    // or two records sharing an offset.
    let duplicated = !dup_offsets.is_empty() || records.len() != found.len();
    let lost = records.len() < expected_from_accepted;

    println!(
        "VERDICT: {}",
        if duplicated {
            "CORRUPTION"
        } else if lost {
            "DATA LOSS"
        } else if refused > 0 {
            "refused loudly, nothing lost"
        } else {
            "single writer, nothing lost"
        }
    );
    if lost {
        let mut sample: Vec<_> = missing.iter().take(5).collect();
        sample.sort();
        println!("  missing from an accepted writer, e.g. {sample:?}");
    }
    Ok(())
}
