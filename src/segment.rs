//! Segment management for partitions.
//!
//! A segment represents a contiguous range of offsets within a partition.
//! Each segment has its own:
//! - Object store (pack file)
//! - Offset index
//! - Merkle tree
//!
//! Segments can be:
//! - Active: Currently accepting writes
//! - Sealed: Read-only, no longer accepting writes

use crate::compression::Compression;
use crate::hash::Hash;
use crate::record::Record;
use crate::store::ObjectStore;
use crate::tree::{MerkleTree, TreeSnapshot};
use anyhow::{Context, Result};
use std::fs;
use std::io::{BufWriter, Read as IoRead, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};

/// Fixed-width index entry: 4 bytes CRC32 + 32 bytes SHA-256 hash = 36 bytes.
const INDEX_ENTRY_SIZE: usize = 36;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Segment metadata stored in offset_range.bin
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegmentMeta {
    /// First offset in this segment (inclusive)
    pub start_offset: u64,
    /// Last offset in this segment (inclusive), None if segment is empty
    pub end_offset: Option<u64>,
    /// Number of records in this segment
    pub record_count: u64,
    /// Whether this segment is sealed (read-only)
    pub sealed: bool,
}

/// A single segment within a partition.
pub struct Segment {
    /// Segment ID (monotonically increasing)
    pub id: u32,
    /// Directory containing this segment's files
    dir: PathBuf,
    /// Object store for this segment
    store: ObjectStore,
    /// Merkle tree for this segment
    tree: MerkleTree,
    /// First offset in this segment (inclusive)
    start_offset: u64,
    /// Next offset to write (start_offset + record_count)
    next_offset: u64,
    /// Index writer (None if sealed)
    index_writer: Option<BufWriter<fs::File>>,
    /// Whether this segment is sealed
    sealed: bool,
}

impl Segment {
    /// Create a new empty segment.
    pub fn create(
        id: u32,
        dir: impl Into<PathBuf>,
        start_offset: u64,
        compression: Compression,
    ) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).context("creating segment dir")?;

        let objects_dir = dir.join("objects");
        fs::create_dir_all(&objects_dir).context("creating objects dir")?;

        let pack_path = objects_dir.join("objects.pack");
        let legacy_dir = objects_dir.join("legacy"); // Won't exist, but needed for API
        let store = ObjectStore::open(pack_path, legacy_dir, compression)?;

        let index_path = dir.join("offsets.idx");
        let index_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .context("creating offset index")?;
        let index_writer = BufWriter::new(index_file);

        // Save initial metadata
        let meta = SegmentMeta {
            start_offset,
            end_offset: None,
            record_count: 0,
            sealed: false,
        };
        Self::save_meta(&dir, &meta)?;

        Ok(Segment {
            id,
            dir,
            store,
            tree: MerkleTree::new(),
            start_offset,
            next_offset: start_offset,
            index_writer: Some(index_writer),
            sealed: false,
        })
    }

    /// Open an existing segment.
    pub fn open(id: u32, dir: impl Into<PathBuf>, compression: Compression) -> Result<Self> {
        let dir = dir.into();

        // Load metadata
        let meta = Self::load_meta(&dir)?;

        let objects_dir = dir.join("objects");
        let pack_path = objects_dir.join("objects.pack");
        let legacy_dir = objects_dir.join("legacy");
        let store = ObjectStore::open(pack_path, legacy_dir, compression)?;

        // Load tree snapshot if it exists
        let snapshot_path = dir.join("tree.snapshot");
        let tree = if snapshot_path.exists() {
            match Self::atomic_read(&snapshot_path) {
                Ok(Some(data)) => {
                    match bincode::deserialize::<TreeSnapshot>(&data) {
                        Ok(snap) => MerkleTree::from_snapshot(snap),
                        Err(_) => MerkleTree::new(),
                    }
                }
                _ => MerkleTree::new(),
            }
        } else {
            MerkleTree::new()
        };

        // Determine actual record count from index file
        let index_path = dir.join("offsets.idx");
        let record_count = if index_path.exists() {
            let f = fs::File::open(&index_path).context("opening index")?;
            let len = f.metadata()?.len();
            len / INDEX_ENTRY_SIZE as u64
        } else {
            0
        };

        let next_offset = meta.start_offset + record_count;

        // Open index writer if not sealed
        let index_writer = if meta.sealed {
            None
        } else {
            let index_file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&index_path)
                .context("opening offset index for append")?;
            Some(BufWriter::new(index_file))
        };

        Ok(Segment {
            id,
            dir,
            store,
            tree,
            start_offset: meta.start_offset,
            next_offset,
            index_writer,
            sealed: meta.sealed,
        })
    }

    /// Open an existing segment, loading only metadata (for cold start optimization).
    /// The full index is NOT loaded - only the metadata file is read.
    pub fn open_sealed_metadata_only(
        id: u32,
        dir: impl Into<PathBuf>,
        compression: Compression,
    ) -> Result<Self> {
        let dir = dir.into();

        // Load metadata
        let meta = Self::load_meta(&dir)?;

        if !meta.sealed {
            // Fall back to full open for non-sealed segments
            return Self::open(id, dir, compression);
        }

        // For sealed segments, we can defer loading the actual data
        let objects_dir = dir.join("objects");
        let pack_path = objects_dir.join("objects.pack");
        let legacy_dir = objects_dir.join("legacy");
        let store = ObjectStore::open(pack_path, legacy_dir, compression)?;

        Ok(Segment {
            id,
            dir,
            store,
            tree: MerkleTree::new(), // Empty - will load on demand if needed
            start_offset: meta.start_offset,
            next_offset: meta.start_offset + meta.record_count,
            index_writer: None,
            sealed: true,
        })
    }

    /// Get the segment ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the start offset of this segment.
    pub fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Get the next offset (one past the last record).
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Get the number of records in this segment.
    pub fn record_count(&self) -> u64 {
        self.next_offset - self.start_offset
    }

    /// Check if this segment is sealed.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Check if this segment contains a given offset.
    pub fn contains(&self, offset: u64) -> bool {
        offset >= self.start_offset && offset < self.next_offset
    }

    /// Check if this segment overlaps with a given range [from, to).
    pub fn overlaps(&self, from: u64, to: u64) -> bool {
        self.start_offset < to && self.next_offset > from
    }

    /// Append a record to this segment.
    /// Returns an error if the segment is sealed.
    pub fn append(&mut self, record: &mut Record) -> Result<u64> {
        if self.sealed {
            anyhow::bail!("cannot append to sealed segment");
        }

        let index_writer = self
            .index_writer
            .as_mut()
            .expect("unsealed segment must have index writer");

        let offset = self.next_offset;
        record.offset = offset;

        // Store the record
        let record_bytes = record.serialize();
        let record_hash = self.store.put(&record_bytes)?;

        // Append to merkle tree
        self.tree.append(record_hash, offset, &self.store)?;

        // Append to offset index: [4 bytes CRC32][32 bytes hash]
        let entry_crc = crc32fast::hash(&record_hash.0);
        index_writer
            .write_all(&entry_crc.to_le_bytes())
            .context("writing index entry CRC")?;
        index_writer
            .write_all(&record_hash.0)
            .context("writing index entry hash")?;

        self.next_offset += 1;

        Ok(offset)
    }

    /// Flush all pending writes.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.index_writer {
            writer.flush().context("flushing index")?;
            writer.get_ref().sync_all().context("syncing index")?;
        }
        self.store.flush()?;

        // Persist tree snapshot
        let snap = self.tree.snapshot();
        let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
        Self::atomic_write(&self.dir.join("tree.snapshot"), &snap_bytes)?;

        // Update metadata
        let meta = SegmentMeta {
            start_offset: self.start_offset,
            end_offset: if self.next_offset > self.start_offset {
                Some(self.next_offset - 1)
            } else {
                None
            },
            record_count: self.record_count(),
            sealed: self.sealed,
        };
        Self::save_meta(&self.dir, &meta)?;

        Ok(())
    }

    /// Seal this segment, making it read-only.
    pub fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Ok(());
        }

        self.flush()?;

        // Close the index writer
        self.index_writer = None;
        self.sealed = true;

        // Update metadata
        let meta = SegmentMeta {
            start_offset: self.start_offset,
            end_offset: if self.next_offset > self.start_offset {
                Some(self.next_offset - 1)
            } else {
                None
            },
            record_count: self.record_count(),
            sealed: true,
        };
        Self::save_meta(&self.dir, &meta)?;

        Ok(())
    }

    /// Read a single record by offset.
    pub fn read(&self, offset: u64) -> Result<Option<Record>> {
        if !self.contains(offset) {
            return Ok(None);
        }

        let local_offset = offset - self.start_offset;
        let hash = self.read_index_entry(local_offset)?;
        let data = self.store.get(&hash)?;
        let record = Record::deserialize(&data)?;
        Ok(Some(record))
    }

    /// Read a range of records [from, to) that fall within this segment.
    /// Clamps to segment bounds.
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<Record>> {
        let start = from.max(self.start_offset);
        let end = to.min(self.next_offset);

        if start >= end {
            return Ok(Vec::new());
        }

        // Read index entries
        let local_start = start - self.start_offset;
        let local_end = end - self.start_offset;
        let entries = self.read_index_entries_batch(local_start, local_end)?;

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Batch fetch objects
        let hashes: Vec<Hash> = entries.iter().map(|(_, h)| *h).collect();
        let objects = self.store.get_batch(&hashes)?;

        // Deserialize records
        let mut records = Vec::with_capacity(objects.len());
        for (_hash, data) in objects {
            let record = Record::deserialize(&data)?;
            records.push(record);
        }

        Ok(records)
    }

    /// Get the merkle root for this segment.
    pub fn merkle_root(&self) -> Result<Option<Hash>> {
        self.tree.root(&self.store)
    }

    /// Get the object store.
    pub fn store(&self) -> &ObjectStore {
        &self.store
    }

    /// Get the segment directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Delete this segment's files from disk.
    pub fn delete(self) -> Result<()> {
        fs::remove_dir_all(&self.dir).context("removing segment directory")?;
        Ok(())
    }

    // --- Private helpers ---

    fn read_index_entry(&self, local_offset: u64) -> Result<Hash> {
        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::File::open(&index_path).context("opening offset index")?;
        let seek_pos = local_offset * INDEX_ENTRY_SIZE as u64;
        file.seek(SeekFrom::Start(seek_pos))
            .context("seeking in index")?;

        let mut crc_buf = [0u8; CRC_SIZE];
        file.read_exact(&mut crc_buf)
            .context("reading index entry CRC")?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        let mut hash_buf = [0u8; 32];
        file.read_exact(&mut hash_buf)
            .context("reading index entry hash")?;

        let computed_crc = crc32fast::hash(&hash_buf);
        if stored_crc != computed_crc {
            anyhow::bail!(
                "index entry CRC mismatch at local offset {}: stored={:#010x} computed={:#010x}",
                local_offset,
                stored_crc,
                computed_crc
            );
        }

        Ok(Hash(hash_buf))
    }

    fn read_index_entries_batch(&self, from: u64, to: u64) -> Result<Vec<(u64, Hash)>> {
        if from >= to {
            return Ok(Vec::new());
        }

        let index_path = self.dir.join("offsets.idx");
        let mut file =
            fs::File::open(&index_path).context("opening offset index for batch read")?;

        let seek_pos = from * INDEX_ENTRY_SIZE as u64;
        file.seek(SeekFrom::Start(seek_pos))
            .context("seeking in index for batch read")?;

        let mut entries = Vec::with_capacity((to - from) as usize);

        for local_offset in from..to {
            let mut crc_buf = [0u8; CRC_SIZE];
            file.read_exact(&mut crc_buf)
                .with_context(|| format!("reading index entry CRC at local offset {}", local_offset))?;
            let stored_crc = u32::from_le_bytes(crc_buf);

            let mut hash_buf = [0u8; 32];
            file.read_exact(&mut hash_buf)
                .with_context(|| format!("reading index entry hash at local offset {}", local_offset))?;

            let computed_crc = crc32fast::hash(&hash_buf);
            if stored_crc != computed_crc {
                anyhow::bail!(
                    "index entry CRC mismatch at local offset {}: stored={:#010x} computed={:#010x}",
                    local_offset,
                    stored_crc,
                    computed_crc
                );
            }

            entries.push((self.start_offset + local_offset, Hash(hash_buf)));
        }

        Ok(entries)
    }

    fn save_meta(dir: &Path, meta: &SegmentMeta) -> Result<()> {
        let data = bincode::serialize(meta).context("serializing segment meta")?;
        Self::atomic_write(&dir.join("segment.meta"), &data)
    }

    fn load_meta(dir: &Path) -> Result<SegmentMeta> {
        let meta_path = dir.join("segment.meta");
        let data = Self::atomic_read(&meta_path)?
            .with_context(|| format!("CRC mismatch reading segment metadata at {}", meta_path.display()))?;
        bincode::deserialize(&data).context("deserializing segment meta")
    }

    fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
        let crc = crc32fast::hash(data);
        let tmp = path.with_extension("tmp");
        let mut f = fs::File::create(&tmp).context("creating temp file for atomic write")?;
        f.write_all(&crc.to_le_bytes())
            .context("writing CRC32 checksum")?;
        f.write_all(data).context("writing atomic data")?;
        f.sync_all().context("syncing atomic write")?;
        fs::rename(&tmp, path).context("renaming atomic write")?;

        if let Some(parent) = path.parent()
            && let Ok(dir) = fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }

        Ok(())
    }

    fn atomic_read(path: &Path) -> Result<Option<Vec<u8>>> {
        let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        if raw.len() < CRC_SIZE {
            return Ok(None);
        }
        let stored_crc = u32::from_le_bytes(raw[..CRC_SIZE].try_into().unwrap());
        let data = &raw[CRC_SIZE..];
        let computed_crc = crc32fast::hash(data);
        if stored_crc != computed_crc {
            return Ok(None);
        }
        Ok(Some(data.to_vec()))
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
    fn segment_create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");
        let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();

        assert_eq!(seg.start_offset(), 0);
        assert_eq!(seg.next_offset(), 0);
        assert_eq!(seg.record_count(), 0);
        assert!(!seg.is_sealed());

        let mut rec = make_record("t", "hello");
        let offset = seg.append(&mut rec).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(seg.next_offset(), 1);
        assert_eq!(seg.record_count(), 1);

        seg.flush().unwrap();
    }

    #[test]
    fn segment_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");
        let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("value-{}", i));
            seg.append(&mut rec).unwrap();
        }
        seg.flush().unwrap();

        for i in 0..10 {
            let record = seg.read(i).unwrap().unwrap();
            assert_eq!(record.value, format!("value-{}", i));
            assert_eq!(record.offset, i);
        }

        // Out of bounds
        assert!(seg.read(10).unwrap().is_none());
    }

    #[test]
    fn segment_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");
        let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();

        for i in 0..20 {
            let mut rec = make_record("t", &format!("v{}", i));
            seg.append(&mut rec).unwrap();
        }
        seg.flush().unwrap();

        let range = seg.read_range(5, 15).unwrap();
        assert_eq!(range.len(), 10);
        assert_eq!(range[0].value, "v5");
        assert_eq!(range[9].value, "v14");
    }

    #[test]
    fn segment_with_nonzero_start() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000001");
        let mut seg = Segment::create(1, &seg_dir, 100, Compression::None).unwrap();

        assert_eq!(seg.start_offset(), 100);
        assert_eq!(seg.next_offset(), 100);

        let mut rec = make_record("t", "first");
        let offset = seg.append(&mut rec).unwrap();
        assert_eq!(offset, 100);

        let mut rec = make_record("t", "second");
        let offset = seg.append(&mut rec).unwrap();
        assert_eq!(offset, 101);

        seg.flush().unwrap();

        assert!(seg.read(99).unwrap().is_none());
        assert_eq!(seg.read(100).unwrap().unwrap().value, "first");
        assert_eq!(seg.read(101).unwrap().unwrap().value, "second");
        assert!(seg.read(102).unwrap().is_none());
    }

    #[test]
    fn segment_seal() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");
        let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();

        let mut rec = make_record("t", "value");
        seg.append(&mut rec).unwrap();
        seg.seal().unwrap();

        assert!(seg.is_sealed());

        // Cannot append to sealed segment
        let mut rec = make_record("t", "more");
        assert!(seg.append(&mut rec).is_err());

        // Can still read
        assert_eq!(seg.read(0).unwrap().unwrap().value, "value");
    }

    #[test]
    fn segment_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");

        {
            let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();
            for i in 0..5 {
                let mut rec = make_record("t", &format!("v{}", i));
                seg.append(&mut rec).unwrap();
            }
            seg.flush().unwrap();
        }

        // Reopen
        let seg = Segment::open(0, &seg_dir, Compression::None).unwrap();
        assert_eq!(seg.start_offset(), 0);
        assert_eq!(seg.next_offset(), 5);
        assert_eq!(seg.record_count(), 5);
        assert!(!seg.is_sealed());

        for i in 0..5 {
            assert_eq!(seg.read(i).unwrap().unwrap().value, format!("v{}", i));
        }
    }

    #[test]
    fn segment_reopen_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");

        {
            let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();
            for i in 0..5 {
                let mut rec = make_record("t", &format!("v{}", i));
                seg.append(&mut rec).unwrap();
            }
            seg.seal().unwrap();
        }

        // Reopen
        let seg = Segment::open(0, &seg_dir, Compression::None).unwrap();
        assert!(seg.is_sealed());
        assert_eq!(seg.record_count(), 5);
    }

    #[test]
    fn segment_contains() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000001");
        let mut seg = Segment::create(1, &seg_dir, 100, Compression::None).unwrap();

        for _ in 0..10 {
            let mut rec = make_record("t", "v");
            seg.append(&mut rec).unwrap();
        }

        assert!(!seg.contains(99));
        assert!(seg.contains(100));
        assert!(seg.contains(105));
        assert!(seg.contains(109));
        assert!(!seg.contains(110));
    }

    #[test]
    fn segment_overlaps() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000001");
        let mut seg = Segment::create(1, &seg_dir, 100, Compression::None).unwrap();

        for _ in 0..10 {
            let mut rec = make_record("t", "v");
            seg.append(&mut rec).unwrap();
        }
        // Segment covers [100, 110)

        assert!(seg.overlaps(95, 105)); // Partial overlap
        assert!(seg.overlaps(100, 110)); // Exact match
        assert!(seg.overlaps(105, 115)); // Partial overlap
        assert!(seg.overlaps(90, 120)); // Contains segment
        assert!(seg.overlaps(102, 105)); // Within segment
        assert!(!seg.overlaps(90, 100)); // Before
        assert!(!seg.overlaps(110, 120)); // After
    }

    #[test]
    fn segment_delete() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg-000000");

        let mut seg = Segment::create(0, &seg_dir, 0, Compression::None).unwrap();
        let mut rec = make_record("t", "value");
        seg.append(&mut rec).unwrap();
        seg.flush().unwrap();

        assert!(seg_dir.exists());
        seg.delete().unwrap();
        assert!(!seg_dir.exists());
    }
}
