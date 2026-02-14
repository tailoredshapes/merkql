use crate::commit::Commit;
use crate::hash::Hash;
use crate::record::Record;
use crate::store::ObjectStore;
use crate::tree::{MerkleTree, TreeSnapshot};
use anyhow::{Context, Result};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

/// Fixed-width index entry: 32 bytes (SHA-256 hash of the record object).
const INDEX_ENTRY_SIZE: usize = 32;

/// A single append-only partition backed by a merkle tree.
pub struct Partition {
    id: u32,
    dir: PathBuf,
    store: ObjectStore,
    tree: MerkleTree,
    next_offset: u64,
    head: Option<Hash>,
}

impl Partition {
    /// Open or create a partition at the given directory.
    pub fn open(id: u32, dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let objects_dir = dir.join("objects");
        fs::create_dir_all(&objects_dir).context("creating partition objects dir")?;

        let store = ObjectStore::new(objects_dir);

        // Restore tree snapshot if it exists
        let snapshot_path = dir.join("tree.snapshot");
        let tree = if snapshot_path.exists() {
            let data = fs::read(&snapshot_path).context("reading tree snapshot")?;
            let snap: TreeSnapshot =
                bincode::deserialize(&data).context("deserializing snapshot")?;
            MerkleTree::from_snapshot(snap)
        } else {
            MerkleTree::new()
        };

        // Restore HEAD
        let head_path = dir.join("HEAD");
        let head = if head_path.exists() {
            let hex = fs::read_to_string(&head_path).context("reading HEAD")?;
            Some(Hash::from_hex(hex.trim())?)
        } else {
            None
        };

        // Determine next offset from index size
        let index_path = dir.join("offsets.idx");
        let next_offset = if index_path.exists() {
            let meta = fs::metadata(&index_path).context("reading index metadata")?;
            meta.len() / INDEX_ENTRY_SIZE as u64
        } else {
            0
        };

        Ok(Partition {
            id,
            dir,
            store,
            tree,
            next_offset,
            head,
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Append a record to the partition, assigning the next sequential offset.
    /// Returns the assigned offset.
    pub fn append(&mut self, record: &mut Record) -> Result<u64> {
        let offset = self.next_offset;
        record.offset = offset;
        record.partition = self.id;

        // Store the record
        let record_bytes = record.serialize();
        let record_hash = self.store.put(&record_bytes)?;

        // Append to merkle tree
        let tree_hash = self.tree.append(record_hash, offset, &self.store)?;

        // Append to offset index
        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .context("opening offset index")?;
        file.write_all(&record_hash.0)
            .context("writing index entry")?;

        // Create commit
        let tree_root = self.tree.root(&self.store)?.unwrap_or(tree_hash);
        let commit = Commit::new(self.head, tree_root, (offset, offset));
        let commit_bytes = commit.serialize();
        let commit_hash = self.store.put(&commit_bytes)?;
        self.head = Some(commit_hash);

        // Persist HEAD
        fs::write(self.dir.join("HEAD"), commit_hash.to_hex()).context("writing HEAD")?;

        // Persist tree snapshot
        let snap = self.tree.snapshot();
        let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
        fs::write(self.dir.join("tree.snapshot"), &snap_bytes).context("writing tree snapshot")?;

        self.next_offset += 1;
        Ok(offset)
    }

    /// Read a single record by offset. O(1) via the index.
    pub fn read(&self, offset: u64) -> Result<Option<Record>> {
        if offset >= self.next_offset {
            return Ok(None);
        }

        let record_hash = self.read_index_entry(offset)?;
        let data = self.store.get(&record_hash)?;
        let record = Record::deserialize(&data)?;
        Ok(Some(record))
    }

    /// Read a range of records [from, to) (exclusive end).
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<Record>> {
        let end = to.min(self.next_offset);
        let mut records = Vec::new();
        for offset in from..end {
            if let Some(record) = self.read(offset)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Get the current merkle root hash.
    pub fn merkle_root(&self) -> Result<Option<Hash>> {
        self.tree.root(&self.store)
    }

    /// Generate a merkle inclusion proof for a given offset.
    pub fn proof(&self, offset: u64) -> Result<Option<crate::tree::Proof>> {
        self.tree.proof(offset, &self.store)
    }

    /// Verify a merkle proof.
    pub fn verify_proof(&self, proof: &crate::tree::Proof) -> Result<bool> {
        MerkleTree::verify_proof(proof, &self.store)
    }

    pub fn store(&self) -> &ObjectStore {
        &self.store
    }

    fn read_index_entry(&self, offset: u64) -> Result<Hash> {
        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::File::open(&index_path).context("opening offset index")?;
        let seek_pos = offset * INDEX_ENTRY_SIZE as u64;
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(seek_pos))
            .context("seeking in index")?;
        let mut buf = [0u8; 32];
        file.read_exact(&mut buf).context("reading index entry")?;
        Ok(Hash(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_record(topic: &str, value: &str) -> Record {
        Record {
            key: None,
            value: value.into(),
            topic: topic.into(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn sequential_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0")).unwrap();

        for i in 0..5 {
            let mut rec = make_record("t", &format!("val-{}", i));
            let offset = part.append(&mut rec).unwrap();
            assert_eq!(offset, i);
        }
        assert_eq!(part.next_offset(), 5);
    }

    #[test]
    fn read_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0")).unwrap();

        let mut rec = make_record("t", "hello");
        part.append(&mut rec).unwrap();

        let read_back = part.read(0).unwrap().unwrap();
        assert_eq!(read_back.value, "hello");
        assert_eq!(read_back.offset, 0);
    }

    #[test]
    fn read_range_works() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0")).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        let range = part.read_range(3, 7).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].value, "v3");
        assert_eq!(range[3].value, "v6");
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Write some records
        {
            let mut part = Partition::open(0, &part_dir).unwrap();
            for i in 0..3 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        // Reopen and verify
        let mut part = Partition::open(0, &part_dir).unwrap();
        assert_eq!(part.next_offset(), 3);

        let r = part.read(1).unwrap().unwrap();
        assert_eq!(r.value, "v1");

        // Can continue appending
        let mut rec = make_record("t", "v3");
        let offset = part.append(&mut rec).unwrap();
        assert_eq!(offset, 3);
    }

    #[test]
    fn merkle_root_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0")).unwrap();

        let mut rec = make_record("t", "first");
        part.append(&mut rec).unwrap();
        let root1 = part.merkle_root().unwrap().unwrap();

        let mut rec = make_record("t", "second");
        part.append(&mut rec).unwrap();
        let root2 = part.merkle_root().unwrap().unwrap();

        assert_ne!(root1, root2);
    }

    #[test]
    fn read_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let part = Partition::open(0, dir.path().join("p0")).unwrap();
        assert!(part.read(0).unwrap().is_none());
    }
}
