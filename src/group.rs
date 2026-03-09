use crate::utils::{atomic_read, atomic_write};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Identifies a specific topic-partition pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

/// Persists per-TopicPartition committed offsets for a consumer group.
pub struct ConsumerGroup {
    group_id: String,
    dir: PathBuf,
    offsets: HashMap<TopicPartition, u64>,
}

impl ConsumerGroup {
    pub fn open(group_id: &str, dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).context("creating group dir")?;

        let offsets_path = dir.join("offsets.bin");
        let offsets = if offsets_path.exists() {
            match atomic_read(&offsets_path) {
                Ok(Some(data)) => bincode::deserialize(&data).unwrap_or_else(|_| HashMap::new()),
                Ok(None) => HashMap::new(), // CRC mismatch → re-consume from beginning
                Err(_) => HashMap::new(),   // read error → re-consume from beginning
            }
        } else {
            HashMap::new()
        };

        Ok(ConsumerGroup {
            group_id: group_id.to_string(),
            dir,
            offsets,
        })
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get the committed offset for a topic-partition, if any.
    pub fn committed_offset(&self, tp: &TopicPartition) -> Option<u64> {
        self.offsets.get(tp).copied()
    }

    /// Commit offsets for multiple topic-partitions at once.
    pub fn commit(&mut self, offsets: &HashMap<TopicPartition, u64>) -> Result<()> {
        for (tp, offset) in offsets {
            self.offsets.insert(tp.clone(), *offset);
        }
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        // Acquire flock for writer exclusion
        let lock_path = self.dir.join("group.lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .context("opening group lock file")?;
        fs2::FileExt::lock_exclusive(&lock_file).context("acquiring group lock")?;

        let data = bincode::serialize(&self.offsets).context("serializing offsets")?;
        atomic_write(&self.dir.join("offsets.bin"), &data).context("writing offsets")?;

        // Lock released on drop of lock_file
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let group_dir = dir.path().join("group");

        let tp = TopicPartition {
            topic: "t1".into(),
            partition: 0,
        };

        {
            let mut group = ConsumerGroup::open("g1", &group_dir).unwrap();
            let mut offsets = HashMap::new();
            offsets.insert(tp.clone(), 42);
            group.commit(&offsets).unwrap();
        }

        // Reopen and verify
        let group = ConsumerGroup::open("g1", &group_dir).unwrap();
        assert_eq!(group.committed_offset(&tp), Some(42));
    }

    #[test]
    fn multiple_topic_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let mut group = ConsumerGroup::open("g1", dir.path().join("group")).unwrap();

        let tp1 = TopicPartition {
            topic: "t1".into(),
            partition: 0,
        };
        let tp2 = TopicPartition {
            topic: "t1".into(),
            partition: 1,
        };
        let tp3 = TopicPartition {
            topic: "t2".into(),
            partition: 0,
        };

        let mut offsets = HashMap::new();
        offsets.insert(tp1.clone(), 10);
        offsets.insert(tp2.clone(), 20);
        offsets.insert(tp3.clone(), 30);
        group.commit(&offsets).unwrap();

        assert_eq!(group.committed_offset(&tp1), Some(10));
        assert_eq!(group.committed_offset(&tp2), Some(20));
        assert_eq!(group.committed_offset(&tp3), Some(30));
    }

    #[test]
    fn corrupt_offsets_crc_defaults_to_empty() {
        let dir = tempfile::tempdir().unwrap();
        let group_dir = dir.path().join("group");

        let tp = TopicPartition {
            topic: "t1".into(),
            partition: 0,
        };

        {
            let mut group = ConsumerGroup::open("g1", &group_dir).unwrap();
            let mut offsets = HashMap::new();
            offsets.insert(tp.clone(), 42);
            group.commit(&offsets).unwrap();
        }

        // Corrupt the offsets.bin CRC
        let offsets_path = group_dir.join("offsets.bin");
        let mut data = fs::read(&offsets_path).unwrap();
        data[0] ^= 0xFF;
        fs::write(&offsets_path, &data).unwrap();

        // Reopen — should default to empty offsets
        let group = ConsumerGroup::open("g1", &group_dir).unwrap();
        assert_eq!(group.committed_offset(&tp), None);
    }
}
