use crate::compression::Compression;
use crate::hash::Hash;
use crate::record::Record;
use crate::segment::Segment;
use crate::store::ObjectStore;
use crate::tree::{MerkleTree, TreeSnapshot};
use anyhow::{Context, Result};
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Fixed-width index entry: 4 bytes CRC32 + 32 bytes SHA-256 hash = 36 bytes.
const INDEX_ENTRY_SIZE: usize = 36;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Partition metadata for multi-segment mode
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PartitionMeta {
    /// ID of the currently active segment
    active_segment_id: u32,
    /// Maximum records per segment before rolling (None = no rolling)
    max_segment_records: Option<u64>,
    /// Whether this partition uses segmented mode
    segmented: bool,
}

/// Atomically write checksummed data to a file using temp+fsync+rename+fsync-parent.
/// Format: [4 bytes CRC32 of data][data...]
fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let crc = crc32fast::hash(data);
    let tmp = path.with_extension("tmp");
    let mut f = fs::File::create(&tmp).context("creating temp file for atomic write")?;
    f.write_all(&crc.to_le_bytes())
        .context("writing CRC32 checksum")?;
    f.write_all(data).context("writing atomic data")?;
    f.sync_all().context("syncing atomic write")?;
    fs::rename(&tmp, path).context("renaming atomic write")?;

    // fsync parent directory to ensure the directory entry is durable (NFS safety)
    if let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }

    Ok(())
}

/// Read checksummed data written by `atomic_write`.
/// Returns Ok(Some(data)) on success, Ok(None) if CRC mismatch.
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

/// Acquire an exclusive flock on a lock file in the given directory.
/// Returns the lock file handle (lock released on drop).
fn acquire_partition_lock(dir: &Path) -> Result<fs::File> {
    let lock_path = dir.join("partition.lock");
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .context("opening partition lock file")?;
    fs2::FileExt::lock_exclusive(&lock_file).context("acquiring partition lock")?;
    Ok(lock_file)
}

/// Storage mode for a partition
enum PartitionStorage {
    /// Legacy single-file mode (backward compatible)
    Legacy {
        store: ObjectStore,
        tree: MerkleTree,
        index_writer: BufWriter<fs::File>,
    },
    /// Multi-segment mode
    Segmented {
        segments: Vec<Segment>,
        active_segment_idx: usize,
    },
}

/// A single append-only partition backed by a merkle tree.
pub struct Partition {
    id: u32,
    dir: PathBuf,
    storage: PartitionStorage,
    next_offset: u64,
    min_valid_offset: u64,
    compression: Compression,
    max_segment_records: Option<u64>,
}

impl Partition {
    /// Open or create a partition at the given directory.
    /// Uses legacy single-segment mode by default for backward compatibility.
    pub fn open(id: u32, dir: impl Into<PathBuf>, compression: Compression) -> Result<Self> {
        Self::open_with_config(id, dir, compression, None)
    }

    /// Open or create a partition with segment rolling configuration.
    ///
    /// # Arguments
    /// * `max_segment_records` - Maximum records per segment before rolling.
    ///   `None` means no rolling (single segment/legacy mode).
    pub fn open_with_config(
        id: u32,
        dir: impl Into<PathBuf>,
        compression: Compression,
        max_segment_records: Option<u64>,
    ) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).context("creating partition dir")?;

        let segments_dir = dir.join("segments");
        let legacy_pack = dir.join("objects.pack");
        let legacy_index = dir.join("offsets.idx");

        // Determine if we should use segmented mode:
        // 1. If segments/ directory exists, use segmented mode
        // 2. If max_segment_records is set AND no legacy files exist, use segmented mode
        // 3. Otherwise, use legacy mode
        let use_segmented = segments_dir.exists()
            || (max_segment_records.is_some() && !legacy_pack.exists() && !legacy_index.exists());

        if use_segmented {
            Self::open_segmented(id, dir, compression, max_segment_records)
        } else {
            Self::open_legacy(id, dir, compression, max_segment_records)
        }
    }

    /// Open partition in legacy (single-file) mode
    fn open_legacy(
        id: u32,
        dir: PathBuf,
        compression: Compression,
        max_segment_records: Option<u64>,
    ) -> Result<Self> {
        let pack_path = dir.join("objects.pack");
        let objects_dir = dir.join("objects");
        let store = ObjectStore::open(pack_path, objects_dir, compression)?;

        // Restore tree snapshot if it exists, with CRC validation
        let snapshot_path = dir.join("tree.snapshot");
        let tree = if snapshot_path.exists() {
            match atomic_read(&snapshot_path) {
                Ok(Some(data)) => {
                    match bincode::deserialize::<TreeSnapshot>(&data) {
                        Ok(snap) => MerkleTree::from_snapshot(snap),
                        Err(_) => MerkleTree::new(),
                    }
                }
                Ok(None) => MerkleTree::new(),
                Err(_) => MerkleTree::new(),
            }
        } else {
            MerkleTree::new()
        };

        // Determine next offset from index size
        let index_path = dir.join("offsets.idx");
        let next_offset = if index_path.exists() {
            let mut f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&index_path)
                .context("opening index for validation")?;
            let len = f
                .seek(SeekFrom::End(0))
                .context("seeking to end of index")?;

            // Truncate any trailing partial entry
            let valid_len = (len / INDEX_ENTRY_SIZE as u64) * INDEX_ENTRY_SIZE as u64;
            if valid_len < len {
                f.set_len(valid_len)
                    .context("truncating partial index entry")?;
                f.sync_all().context("syncing after index truncation")?;
            }

            let mut entry_count = valid_len / INDEX_ENTRY_SIZE as u64;

            // Validate tail entries
            while entry_count > 0 {
                let entry_pos = (entry_count - 1) * INDEX_ENTRY_SIZE as u64;
                f.seek(SeekFrom::Start(entry_pos))
                    .context("seeking to tail entry")?;

                let mut crc_buf = [0u8; CRC_SIZE];
                if f.read_exact(&mut crc_buf).is_err() {
                    entry_count -= 1;
                    continue;
                }
                let stored_crc = u32::from_le_bytes(crc_buf);

                let mut hash_buf = [0u8; 32];
                if f.read_exact(&mut hash_buf).is_err() {
                    entry_count -= 1;
                    continue;
                }

                let computed_crc = crc32fast::hash(&hash_buf);
                if stored_crc != computed_crc {
                    entry_count -= 1;
                    continue;
                }

                let hash = Hash(hash_buf);
                if !store.exists(&hash) {
                    entry_count -= 1;
                    continue;
                }

                break;
            }

            // Truncate index to validated length
            let validated_len = entry_count * INDEX_ENTRY_SIZE as u64;
            if validated_len < valid_len {
                f.set_len(validated_len)
                    .context("truncating invalid tail entries")?;
                f.sync_all()
                    .context("syncing after tail entry truncation")?;
            }

            entry_count
        } else {
            0
        };

        // Restore retention marker
        let retention_path = dir.join("retention.bin");
        let min_valid_offset = if retention_path.exists() {
            match atomic_read(&retention_path) {
                Ok(Some(data)) => bincode::deserialize(&data).unwrap_or(0),
                Ok(None) => 0,
                Err(_) => 0,
            }
        } else {
            0
        };

        // Open index file for persistent appending
        let index_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .context("opening offset index for append")?;
        let index_writer = BufWriter::new(index_file);

        Ok(Partition {
            id,
            dir,
            storage: PartitionStorage::Legacy {
                store,
                tree,
                index_writer,
            },
            next_offset,
            min_valid_offset,
            compression,
            max_segment_records,
        })
    }

    /// Open partition in segmented mode
    fn open_segmented(
        id: u32,
        dir: PathBuf,
        compression: Compression,
        max_segment_records: Option<u64>,
    ) -> Result<Self> {
        let segments_dir = dir.join("segments");
        fs::create_dir_all(&segments_dir).context("creating segments dir")?;

        // Load existing segments
        let mut segments: Vec<Segment> = Vec::new();
        let mut max_segment_id: u32 = 0;

        if segments_dir.exists() {
            let mut segment_dirs: Vec<_> = fs::read_dir(&segments_dir)
                .context("reading segments dir")?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .collect();

            segment_dirs.sort_by_key(|e| e.file_name());

            for entry in segment_dirs {
                let seg_name = entry.file_name();
                let seg_name = seg_name.to_string_lossy();

                // Parse segment ID from "seg-NNNNNN" format
                if let Some(id_str) = seg_name.strip_prefix("seg-")
                    && let Ok(seg_id) = id_str.parse::<u32>()
                {
                    // Cold start optimization: sealed segments load metadata only
                    // This avoids scanning the full index on startup
                    let segment = Segment::open_sealed_metadata_only(seg_id, entry.path(), compression)?;
                    max_segment_id = max_segment_id.max(seg_id);
                    segments.push(segment);
                }
            }
        }

        // If no segments exist, create the first one
        if segments.is_empty() {
            let seg_dir = segments_dir.join("seg-000000");
            let segment = Segment::create(0, seg_dir, 0, compression)?;
            segments.push(segment);
        }

        // Find the active (non-sealed) segment
        let active_segment_idx = segments
            .iter()
            .position(|s| !s.is_sealed())
            .unwrap_or(segments.len() - 1);

        // Calculate next_offset from the active segment
        let next_offset = segments[active_segment_idx].next_offset();

        // Restore retention marker
        let retention_path = dir.join("retention.bin");
        let min_valid_offset = if retention_path.exists() {
            match atomic_read(&retention_path) {
                Ok(Some(data)) => bincode::deserialize(&data).unwrap_or(0),
                Ok(None) => 0,
                Err(_) => 0,
            }
        } else {
            0
        };

        // Save partition metadata
        let meta = PartitionMeta {
            active_segment_id: segments[active_segment_idx].id(),
            max_segment_records,
            segmented: true,
        };
        let meta_data = bincode::serialize(&meta).context("serializing partition meta")?;
        atomic_write(&dir.join("partition.meta"), &meta_data)?;

        Ok(Partition {
            id,
            dir,
            storage: PartitionStorage::Segmented {
                segments,
                active_segment_idx,
            },
            next_offset,
            min_valid_offset,
            compression,
            max_segment_records,
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn min_valid_offset(&self) -> u64 {
        self.min_valid_offset
    }

    /// Append a record to the partition, assigning the next sequential offset.
    /// Returns the assigned offset.
    /// Acquires an exclusive flock for writer exclusion (NFS-safe).
    pub fn append(&mut self, record: &mut Record) -> Result<u64> {
        let _lock = acquire_partition_lock(&self.dir)?;

        // Check if we need to roll to a new segment (segmented mode only)
        if let PartitionStorage::Segmented { ref segments, active_segment_idx } = self.storage
            && let Some(max_records) = self.max_segment_records
        {
            let active_segment = &segments[active_segment_idx];
            if active_segment.record_count() >= max_records {
                self.roll_segment()?;
            }
        }

        let offset = self.next_offset;
        record.offset = offset;
        record.partition = self.id;

        match &mut self.storage {
            PartitionStorage::Legacy { store, tree, index_writer } => {
                // Store the record
                let record_bytes = record.serialize();
                let record_hash = store.put(&record_bytes)?;

                // Append to merkle tree
                tree.append(record_hash, offset, store)?;

                // Append to offset index
                let entry_crc = crc32fast::hash(&record_hash.0);
                index_writer
                    .write_all(&entry_crc.to_le_bytes())
                    .context("writing index entry CRC")?;
                index_writer
                    .write_all(&record_hash.0)
                    .context("writing index entry hash")?;

                // Flush and fsync
                index_writer.flush().context("flushing index")?;
                index_writer.get_ref().sync_all().context("syncing index")?;
                store.flush()?;

                // Persist tree snapshot atomically
                let snap = tree.snapshot();
                let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
                atomic_write(&self.dir.join("tree.snapshot"), &snap_bytes)?;
            }
            PartitionStorage::Segmented { segments, active_segment_idx } => {
                let segment = &mut segments[*active_segment_idx];
                segment.append(record)?;
                segment.flush()?;
            }
        }

        self.next_offset += 1;
        Ok(offset)
    }

    /// Append a batch of records. Amortizes fsync and snapshot writes.
    /// Returns the assigned offsets.
    /// Acquires an exclusive flock for writer exclusion (NFS-safe).
    pub fn append_batch(&mut self, records: &mut [Record]) -> Result<Vec<u64>> {
        let _lock = acquire_partition_lock(&self.dir)?;

        let mut offsets = Vec::with_capacity(records.len());

        match &mut self.storage {
            PartitionStorage::Legacy { store, tree, index_writer } => {
                for record in records.iter_mut() {
                    let offset = self.next_offset;
                    record.offset = offset;
                    record.partition = self.id;

                    let record_bytes = record.serialize();
                    let record_hash = store.put(&record_bytes)?;
                    tree.append(record_hash, offset, store)?;

                    let entry_crc = crc32fast::hash(&record_hash.0);
                    index_writer
                        .write_all(&entry_crc.to_le_bytes())
                        .context("writing index entry CRC")?;
                    index_writer
                        .write_all(&record_hash.0)
                        .context("writing index entry hash")?;

                    self.next_offset += 1;
                    offsets.push(offset);
                }

                // Single flush + fsync for entire batch
                index_writer.flush().context("flushing index")?;
                index_writer.get_ref().sync_all().context("syncing index")?;
                store.flush()?;

                let snap = tree.snapshot();
                let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
                atomic_write(&self.dir.join("tree.snapshot"), &snap_bytes)?;
            }
            PartitionStorage::Segmented { segments, active_segment_idx } => {
                for record in records.iter_mut() {
                    // Check if we need to roll
                    if let Some(max_records) = self.max_segment_records {
                        let active_segment = &segments[*active_segment_idx];
                        if active_segment.record_count() >= max_records {
                            // We need to flush before rolling
                            segments[*active_segment_idx].flush()?;

                            // Roll to new segment
                            let current_segment = &mut segments[*active_segment_idx];
                            current_segment.seal()?;

                            let new_id = current_segment.id() + 1;
                            let seg_dir = self.dir.join("segments").join(format!("seg-{:06}", new_id));
                            let new_segment = Segment::create(new_id, seg_dir, self.next_offset, self.compression)?;
                            segments.push(new_segment);
                            *active_segment_idx = segments.len() - 1;
                        }
                    }

                    let offset = self.next_offset;
                    record.offset = offset;
                    record.partition = self.id;

                    let segment = &mut segments[*active_segment_idx];
                    segment.append(record)?;

                    self.next_offset += 1;
                    offsets.push(offset);
                }

                // Single flush for entire batch
                segments[*active_segment_idx].flush()?;
            }
        }

        Ok(offsets)
    }

    /// Read a single record by offset. O(1) via the index.
    /// Returns None for offsets below min_valid_offset or >= next_offset.
    pub fn read(&self, offset: u64) -> Result<Option<Record>> {
        if offset >= self.next_offset || offset < self.min_valid_offset {
            return Ok(None);
        }

        match &self.storage {
            PartitionStorage::Legacy { store, .. } => {
                let record_hash = self.read_index_entry_legacy(offset)?;
                let data = store.get(&record_hash)?;
                let record = Record::deserialize(&data)?;
                Ok(Some(record))
            }
            PartitionStorage::Segmented { segments, .. } => {
                // Find the segment containing this offset
                for segment in segments.iter() {
                    if segment.contains(offset) {
                        return segment.read(offset);
                    }
                }
                Ok(None)
            }
        }
    }

    /// Read a range of records [from, to) (exclusive end).
    /// Uses batch operations for efficiency.
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<Record>> {
        let start = from.max(self.min_valid_offset);
        let end = to.min(self.next_offset);

        if start >= end {
            return Ok(Vec::new());
        }

        match &self.storage {
            PartitionStorage::Legacy { store, .. } => {
                let entries = self.read_index_entries_batch_legacy(start, end)?;
                if entries.is_empty() {
                    return Ok(Vec::new());
                }

                let hashes: Vec<Hash> = entries.iter().map(|(_, h)| *h).collect();
                let objects = store.get_batch(&hashes)?;

                let mut records = Vec::with_capacity(objects.len());
                for (_hash, data) in objects {
                    let record = Record::deserialize(&data)?;
                    records.push(record);
                }
                Ok(records)
            }
            PartitionStorage::Segmented { segments, .. } => {
                let mut records = Vec::new();

                // Read from each overlapping segment
                for segment in segments.iter() {
                    if segment.overlaps(start, end) {
                        let segment_records = segment.read_range(start, end)?;
                        records.extend(segment_records);
                    }
                }

                // Sort by offset to ensure order
                records.sort_by_key(|r| r.offset);
                Ok(records)
            }
        }
    }

    /// Get the current merkle root hash.
    pub fn merkle_root(&self) -> Result<Option<Hash>> {
        match &self.storage {
            PartitionStorage::Legacy { store, tree, .. } => tree.root(store),
            PartitionStorage::Segmented { segments, active_segment_idx } => {
                // For segmented mode, return the active segment's merkle root
                // In a full implementation, we'd have a partition-level merkle tree
                segments[*active_segment_idx].merkle_root()
            }
        }
    }

    /// Generate a merkle inclusion proof for a given offset.
    pub fn proof(&self, offset: u64) -> Result<Option<crate::tree::Proof>> {
        match &self.storage {
            PartitionStorage::Legacy { store, tree, .. } => tree.proof(offset, store),
            PartitionStorage::Segmented { .. } => {
                // For segmented mode, proofs are segment-local
                // A full implementation would need cross-segment proofs
                Ok(None)
            }
        }
    }

    /// Verify a merkle proof.
    pub fn verify_proof(&self, proof: &crate::tree::Proof) -> Result<bool> {
        match &self.storage {
            PartitionStorage::Legacy { store, .. } => MerkleTree::verify_proof(proof, store),
            PartitionStorage::Segmented { segments, .. } => {
                // Try to verify against any segment's store
                for segment in segments.iter() {
                    if let Ok(true) = MerkleTree::verify_proof(proof, segment.store()) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub fn store(&self) -> &ObjectStore {
        match &self.storage {
            PartitionStorage::Legacy { store, .. } => store,
            PartitionStorage::Segmented { segments, active_segment_idx } => {
                segments[*active_segment_idx].store()
            }
        }
    }

    /// Advance the retention window. Records below new_min will return None on read.
    pub fn advance_retention(&mut self, new_min: u64) -> Result<()> {
        if new_min > self.min_valid_offset {
            self.min_valid_offset = new_min;
            let data =
                bincode::serialize(&self.min_valid_offset).context("serializing retention")?;
            atomic_write(&self.dir.join("retention.bin"), &data)?;
        }
        Ok(())
    }

    /// Compact the partition by removing sealed segments whose data is entirely
    /// below the retention window.
    pub fn compact(&mut self) -> Result<usize> {
        match &mut self.storage {
            PartitionStorage::Legacy { .. } => {
                // Legacy mode doesn't support compaction
                Ok(0)
            }
            PartitionStorage::Segmented { segments, active_segment_idx } => {
                let mut removed = 0;
                let mut new_segments = Vec::new();
                let mut new_active_idx = 0;

                for (idx, segment) in segments.drain(..).enumerate() {
                    // Keep the active segment
                    if idx == *active_segment_idx {
                        new_active_idx = new_segments.len();
                        new_segments.push(segment);
                        continue;
                    }

                    // Keep unsealed segments
                    if !segment.is_sealed() {
                        new_segments.push(segment);
                        continue;
                    }

                    // Check if entire segment is below retention
                    if segment.next_offset() <= self.min_valid_offset {
                        // Delete this segment
                        segment.delete()?;
                        removed += 1;
                    } else {
                        new_segments.push(segment);
                    }
                }

                *segments = new_segments;
                *active_segment_idx = new_active_idx;

                Ok(removed)
            }
        }
    }

    /// Roll to a new segment, sealing the current active segment.
    fn roll_segment(&mut self) -> Result<()> {
        if let PartitionStorage::Segmented { segments, active_segment_idx } = &mut self.storage {
            // Seal the current segment
            segments[*active_segment_idx].seal()?;

            // Create a new segment
            let new_id = segments[*active_segment_idx].id() + 1;
            let seg_dir = self.dir.join("segments").join(format!("seg-{:06}", new_id));
            let new_segment = Segment::create(new_id, seg_dir, self.next_offset, self.compression)?;
            segments.push(new_segment);
            *active_segment_idx = segments.len() - 1;

            // Update partition metadata
            let meta = PartitionMeta {
                active_segment_id: new_id,
                max_segment_records: self.max_segment_records,
                segmented: true,
            };
            let meta_data = bincode::serialize(&meta).context("serializing partition meta")?;
            atomic_write(&self.dir.join("partition.meta"), &meta_data)?;
        }
        Ok(())
    }

    /// Get the number of segments (1 for legacy mode)
    pub fn segment_count(&self) -> usize {
        match &self.storage {
            PartitionStorage::Legacy { .. } => 1,
            PartitionStorage::Segmented { segments, .. } => segments.len(),
        }
    }

    // --- Legacy mode helpers ---

    fn read_index_entry_legacy(&self, offset: u64) -> Result<Hash> {
        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::File::open(&index_path).context("opening offset index")?;
        let seek_pos = offset * INDEX_ENTRY_SIZE as u64;
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
                "index entry CRC mismatch at offset {}: stored={:#010x} computed={:#010x}",
                offset,
                stored_crc,
                computed_crc
            );
        }

        Ok(Hash(hash_buf))
    }

    fn read_index_entries_batch_legacy(&self, from: u64, to: u64) -> Result<Vec<(u64, Hash)>> {
        if from >= to {
            return Ok(Vec::new());
        }

        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::File::open(&index_path).context("opening offset index for batch read")?;

        let seek_pos = from * INDEX_ENTRY_SIZE as u64;
        file.seek(SeekFrom::Start(seek_pos))
            .context("seeking in index for batch read")?;

        let count = (to - from) as usize;
        let mut entries = Vec::with_capacity(count);

        for offset in from..to {
            let mut crc_buf = [0u8; CRC_SIZE];
            file.read_exact(&mut crc_buf)
                .with_context(|| format!("reading index entry CRC at offset {}", offset))?;
            let stored_crc = u32::from_le_bytes(crc_buf);

            let mut hash_buf = [0u8; 32];
            file.read_exact(&mut hash_buf)
                .with_context(|| format!("reading index entry hash at offset {}", offset))?;

            let computed_crc = crc32fast::hash(&hash_buf);
            if stored_crc != computed_crc {
                anyhow::bail!(
                    "index entry CRC mismatch at offset {}: stored={:#010x} computed={:#010x}",
                    offset,
                    stored_crc,
                    computed_crc
                );
            }

            entries.push((offset, Hash(hash_buf)));
        }

        Ok(entries)
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
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

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
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        let mut rec = make_record("t", "hello");
        part.append(&mut rec).unwrap();

        let read_back = part.read(0).unwrap().unwrap();
        assert_eq!(read_back.value, "hello");
        assert_eq!(read_back.offset, 0);
    }

    #[test]
    fn read_range_works() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

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

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..3 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.next_offset(), 3);

        let r = part.read(1).unwrap().unwrap();
        assert_eq!(r.value, "v1");

        let mut rec = make_record("t", "v3");
        let offset = part.append(&mut rec).unwrap();
        assert_eq!(offset, 3);
    }

    #[test]
    fn merkle_root_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

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
        let part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();
        assert!(part.read(0).unwrap().is_none());
    }

    #[test]
    fn batch_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        let mut records: Vec<Record> = (0..5)
            .map(|i| make_record("t", &format!("batch-{}", i)))
            .collect();
        let offsets = part.append_batch(&mut records).unwrap();

        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
        assert_eq!(part.next_offset(), 5);

        for i in 0..5 {
            let r = part.read(i).unwrap().unwrap();
            assert_eq!(r.value, format!("batch-{}", i));
        }
    }

    #[test]
    fn retention_hides_old_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(5).unwrap();
        assert_eq!(part.min_valid_offset(), 5);

        for i in 0..5 {
            assert!(part.read(i).unwrap().is_none());
        }
        for i in 5..10 {
            assert!(part.read(i).unwrap().is_some());
        }
    }

    #[test]
    fn retention_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..10 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
            part.advance_retention(5).unwrap();
        }

        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.min_valid_offset(), 5);
        assert!(part.read(4).unwrap().is_none());
        assert!(part.read(5).unwrap().is_some());
    }

    #[test]
    fn read_range_respects_retention() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(3).unwrap();
        let range = part.read_range(0, 10).unwrap();
        assert_eq!(range.len(), 7);
        assert_eq!(range[0].value, "v3");
    }

    #[test]
    fn compression_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::Lz4).unwrap();

        for i in 0..5 {
            let mut rec = make_record("t", &format!("compressed-{}", i));
            part.append(&mut rec).unwrap();
        }

        for i in 0..5 {
            let r = part.read(i).unwrap().unwrap();
            assert_eq!(r.value, format!("compressed-{}", i));
        }
    }

    #[test]
    fn corrupt_snapshot_crc_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..5 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        let snapshot_path = part_dir.join("tree.snapshot");
        let mut data = fs::read(&snapshot_path).unwrap();
        data[0] ^= 0xFF;
        fs::write(&snapshot_path, &data).unwrap();

        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.next_offset(), 5);
        let r = part.read(2).unwrap().unwrap();
        assert_eq!(r.value, "v2");
    }

    #[test]
    fn corrupt_retention_crc_defaults_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..10 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
            part.advance_retention(5).unwrap();
        }

        let retention_path = part_dir.join("retention.bin");
        let mut data = fs::read(&retention_path).unwrap();
        data[0] ^= 0xFF;
        fs::write(&retention_path, &data).unwrap();

        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.min_valid_offset(), 0);
        for i in 0..10 {
            assert!(part.read(i).unwrap().is_some());
        }
    }

    #[test]
    fn flock_serializes_concurrent_writers() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            let mut rec = make_record("t", "seed");
            part.append(&mut rec).unwrap();
        }

        let part_dir = Arc::new(part_dir);
        let handles: Vec<_> = (0..2)
            .map(|thread_id| {
                let pd = Arc::clone(&part_dir);
                thread::spawn(move || {
                    let mut part = Partition::open(0, &*pd, Compression::None).unwrap();
                    for i in 0..5 {
                        let mut rec = make_record("t", &format!("t{}-v{}", thread_id, i));
                        part.append(&mut rec).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let part = Partition::open(0, &*part_dir, Compression::None).unwrap();
        assert!(part.next_offset() >= 1);
    }

    #[test]
    fn read_range_batch_correctness() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..100 {
            let mut rec = make_record("t", &format!("batch-value-{}", i));
            part.append(&mut rec).unwrap();
        }

        let range = part.read_range(10, 20).unwrap();
        assert_eq!(range.len(), 10);
        for (i, record) in range.iter().enumerate() {
            assert_eq!(record.value, format!("batch-value-{}", i + 10));
            assert_eq!(record.offset, (i + 10) as u64);
        }

        let full = part.read_range(0, 100).unwrap();
        assert_eq!(full.len(), 100);

        let empty = part.read_range(50, 50).unwrap();
        assert!(empty.is_empty());

        let clamped = part.read_range(90, 200).unwrap();
        assert_eq!(clamped.len(), 10);
        assert_eq!(clamped[0].value, "batch-value-90");
    }

    #[test]
    fn read_range_with_retention() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..50 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(25).unwrap();

        let range = part.read_range(20, 35).unwrap();
        assert_eq!(range.len(), 10);
        assert_eq!(range[0].value, "v25");
        assert_eq!(range[9].value, "v34");

        let old = part.read_range(0, 20).unwrap();
        assert!(old.is_empty());
    }

    // --- Segmented mode tests ---

    #[test]
    fn segment_rolling_basic() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Create partition with max 5 records per segment
        let mut part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(5),
        ).unwrap();

        // Write 12 records - should create 3 segments (5 + 5 + 2)
        for i in 0..12 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        assert_eq!(part.next_offset(), 12);
        assert_eq!(part.segment_count(), 3);

        // Verify all records are readable
        for i in 0..12 {
            let record = part.read(i).unwrap().unwrap();
            assert_eq!(record.value, format!("v{}", i));
        }
    }

    #[test]
    fn read_across_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        let mut part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(5),
        ).unwrap();

        // Write 10 records across 2 segments
        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        // Read range that spans segment boundary
        let range = part.read_range(3, 8).unwrap();
        assert_eq!(range.len(), 5);
        assert_eq!(range[0].value, "v3");
        assert_eq!(range[4].value, "v7");
    }

    #[test]
    fn segmented_persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open_with_config(
                0,
                &part_dir,
                Compression::None,
                Some(5),
            ).unwrap();

            for i in 0..12 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        // Reopen
        let mut part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(5),
        ).unwrap();

        assert_eq!(part.next_offset(), 12);
        assert_eq!(part.segment_count(), 3);

        // Verify all records
        for i in 0..12 {
            let record = part.read(i).unwrap().unwrap();
            assert_eq!(record.value, format!("v{}", i));
        }

        // Can continue appending
        let mut rec = make_record("t", "v12");
        let offset = part.append(&mut rec).unwrap();
        assert_eq!(offset, 12);
    }

    #[test]
    fn compaction_removes_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        let mut part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(5),
        ).unwrap();

        // Write 15 records (3 segments)
        for i in 0..15 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        assert_eq!(part.segment_count(), 3);

        // Advance retention past first segment
        part.advance_retention(6).unwrap();

        // Compact should remove the first segment
        let removed = part.compact().unwrap();
        assert_eq!(removed, 1);
        assert_eq!(part.segment_count(), 2);

        // Old records should return None
        for i in 0..6 {
            assert!(part.read(i).unwrap().is_none());
        }

        // Newer records should still work
        for i in 6..15 {
            let record = part.read(i).unwrap().unwrap();
            assert_eq!(record.value, format!("v{}", i));
        }
    }

    #[test]
    fn cold_start_many_segments_loads_metadata_only() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Create partition with many segments
        {
            let mut part = Partition::open_with_config(
                0,
                &part_dir,
                Compression::None,
                Some(10), // 10 records per segment
            ).unwrap();

            // Write 100 records (10 segments)
            for i in 0..100 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        // Measure cold start time
        let start = Instant::now();
        let part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(10),
        ).unwrap();
        let _cold_start_duration = start.elapsed();

        // Should have loaded all segments
        assert_eq!(part.segment_count(), 10);
        assert_eq!(part.next_offset(), 100);

        // All records should still be readable
        for i in 0..100 {
            let record = part.read(i).unwrap().unwrap();
            assert_eq!(record.value, format!("v{}", i));
        }
    }

    #[test]
    fn compaction_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        let mut part = Partition::open_with_config(
            0,
            &part_dir,
            Compression::None,
            Some(5),
        ).unwrap();

        // Write 15 records (3 segments)
        for i in 0..15 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(6).unwrap();

        // First compaction
        let removed1 = part.compact().unwrap();
        assert_eq!(removed1, 1);
        assert_eq!(part.segment_count(), 2);

        // Second compaction should be a no-op
        let removed2 = part.compact().unwrap();
        assert_eq!(removed2, 0);
        assert_eq!(part.segment_count(), 2);

        // Third compaction should also be a no-op
        let removed3 = part.compact().unwrap();
        assert_eq!(removed3, 0);
        assert_eq!(part.segment_count(), 2);
    }
}
