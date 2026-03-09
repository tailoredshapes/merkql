use crate::compression::Compression;
use crate::hash::Hash;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

/// Header size per pack entry: 4 bytes (data_length) + 32 bytes (hash).
const ENTRY_HEADER_SIZE: u64 = 36;

/// Size of one index entry: 32 (hash) + 8 (data_offset) + 4 (data_length).
const IDX_ENTRY_SIZE: usize = 44;

/// Size of the index file header: 8 bytes (pack_size as u64 LE).
const IDX_HEADER_SIZE: usize = 8;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Write-only state protected by a mutex.
struct WriteState {
    file: File,
    write_pos: u64,
}

/// Content-addressed pack-file store using SHA-256 hashes.
///
/// All objects are stored in a single append-only file (`objects.pack`) with format:
/// `[4 bytes: data_length as u32 LE][32 bytes: SHA-256 hash][data_length bytes: compressed data]`
///
/// Objects are hashed BEFORE compression so merkle proofs remain valid
/// regardless of compression setting.
///
/// Uses RwLock for the index (read-heavy) and Mutex for writes, allowing
/// concurrent reads without blocking.
pub struct ObjectStore {
    index: RwLock<HashMap<Hash, (u64, u32)>>, // hash → (data_offset, data_length)
    writer: Mutex<WriteState>,
    compression: Compression,
    pack_path: PathBuf,
    idx_path: PathBuf,
}

impl ObjectStore {
    /// Open or create a pack-file object store.
    ///
    /// If `pack_path` exists, its entries are scanned to rebuild the in-memory index.
    /// If `legacy_dir` exists (old per-file layout), objects are migrated into the
    /// pack file and the directory is removed.
    pub fn open(
        pack_path: impl Into<PathBuf>,
        legacy_dir: impl Into<PathBuf>,
        compression: Compression,
    ) -> Result<Self> {
        let pack_path = pack_path.into();
        let legacy_dir = legacy_dir.into();
        let idx_path = pack_path.with_extension("idx");

        let pack_exists = pack_path.exists();
        let legacy_exists = legacy_dir.is_dir();

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&pack_path)
            .context("opening pack file")?;

        let mut index = HashMap::new();
        let mut write_pos: u64 = 0;

        if pack_exists {
            // Use file handle to get length (not fs::metadata on path — NFS stale cache safe)
            let pack_len = file
                .seek(SeekFrom::End(0))
                .context("seeking to end of pack file")?;
            file.seek(SeekFrom::Start(0))
                .context("seeking back to start")?;

            // Try loading persisted index first
            if let Some(loaded) = Self::load_index(&idx_path, pack_len)? {
                index = loaded.0;
                write_pos = loaded.1;
            } else {
                // Index missing or stale — fall back to full scan
                write_pos = Self::scan_pack(&mut file, &mut index)?;
            }
        }

        if legacy_exists && !pack_exists {
            // Migrate from old per-file layout
            write_pos = Self::migrate_legacy(&legacy_dir, &mut file, &mut index, write_pos)?;
            file.sync_all().context("syncing after migration")?;
            fs::remove_dir_all(&legacy_dir).context("removing legacy objects dir")?;
        }

        Ok(ObjectStore {
            index: RwLock::new(index),
            writer: Mutex::new(WriteState { file, write_pos }),
            compression,
            pack_path,
            idx_path,
        })
    }

    /// Store bytes, returning their content hash. Idempotent — if the object
    /// already exists, this is a no-op.
    /// Hash is computed on uncompressed data; storage uses compressed form.
    pub fn put(&self, data: &[u8]) -> Result<Hash> {
        let hash = Hash::digest(data);
        let compressed = self.compression.compress(data);

        // First check with read lock (fast path for existing objects)
        {
            let index = self.index.read().unwrap();
            if index.contains_key(&hash) {
                return Ok(hash);
            }
        }

        // Need to write - acquire writer lock
        let mut writer = self.writer.lock().unwrap();

        // Double-check after acquiring write lock (another thread may have written)
        {
            let index = self.index.read().unwrap();
            if index.contains_key(&hash) {
                return Ok(hash);
            }
        }

        let data_len = compressed.len() as u32;
        let pos = writer.write_pos;

        writer
            .file
            .seek(SeekFrom::Start(pos))
            .context("seeking to write position")?;
        writer
            .file
            .write_all(&data_len.to_le_bytes())
            .context("writing data length")?;
        writer.file.write_all(&hash.0).context("writing hash")?;
        writer
            .file
            .write_all(&compressed)
            .context("writing compressed data")?;

        let data_offset = pos + ENTRY_HEADER_SIZE;
        writer.write_pos = data_offset + data_len as u64;

        // Update index with write lock
        {
            let mut index = self.index.write().unwrap();
            index.insert(hash, (data_offset, data_len));
        }

        Ok(hash)
    }

    /// Retrieve bytes by hash. Returns decompressed data.
    /// Uses a fresh file handle to avoid blocking other readers/writers.
    pub fn get(&self, hash: &Hash) -> Result<Vec<u8>> {
        let (data_offset, data_len) = {
            let index = self.index.read().unwrap();
            *index
                .get(hash)
                .with_context(|| format!("object not found: {}", hash))?
        };

        // Open a fresh file handle for reading (doesn't block other readers/writers)
        let mut file = File::open(&self.pack_path)
            .with_context(|| format!("opening pack file for read: {}", hash))?;

        let mut buf = vec![0u8; data_len as usize];
        file.seek(SeekFrom::Start(data_offset))
            .with_context(|| format!("seeking to object {}", hash))?;
        file.read_exact(&mut buf)
            .with_context(|| format!("reading object {}", hash))?;

        Compression::decompress(&buf).with_context(|| format!("decompressing object {}", hash))
    }

    /// Retrieve multiple objects by hash in a single operation.
    /// Opens the pack file once and reads all requested objects.
    /// Returns a vector of (hash, data) pairs in the same order as input.
    /// Missing hashes are skipped (not included in output).
    pub fn get_batch(&self, hashes: &[Hash]) -> Result<Vec<(Hash, Vec<u8>)>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        // Collect all offsets under a single read lock
        let locations: Vec<(Hash, u64, u32)> = {
            let index = self.index.read().unwrap();
            hashes
                .iter()
                .filter_map(|h| index.get(h).map(|&(off, len)| (*h, off, len)))
                .collect()
        };

        if locations.is_empty() {
            return Ok(Vec::new());
        }

        // Open a single file handle for all reads
        let mut file = File::open(&self.pack_path).context("opening pack file for batch read")?;

        let mut results = Vec::with_capacity(locations.len());
        for (hash, data_offset, data_len) in locations {
            let mut buf = vec![0u8; data_len as usize];
            file.seek(SeekFrom::Start(data_offset))
                .with_context(|| format!("seeking to object {}", hash))?;
            file.read_exact(&mut buf)
                .with_context(|| format!("reading object {}", hash))?;

            let decompressed = Compression::decompress(&buf)
                .with_context(|| format!("decompressing object {}", hash))?;
            results.push((hash, decompressed));
        }

        Ok(results)
    }

    /// Check if an object exists.
    pub fn exists(&self, hash: &Hash) -> bool {
        let index = self.index.read().unwrap();
        index.contains_key(hash)
    }

    /// Flush the pack file and persist the index to disk.
    pub fn flush(&self) -> Result<()> {
        let writer = self.writer.lock().unwrap();
        let index = self.index.read().unwrap();
        writer.file.sync_all().context("syncing pack file")?;
        Self::save_index(&self.idx_path, &index, writer.write_pos)?;
        Ok(())
    }

    /// Scan a pack file sequentially, rebuilding the in-memory index.
    /// Returns the write position (end of last valid entry).
    /// Truncates the file at the last valid entry if a partial write is detected.
    fn scan_pack(file: &mut File, index: &mut HashMap<Hash, (u64, u32)>) -> Result<u64> {
        // Use file handle to get length (NFS stale cache safe)
        let file_len = file
            .seek(SeekFrom::End(0))
            .context("seeking to end for pack length")?;
        let mut pos: u64 = 0;
        let mut last_good_pos: u64 = 0;

        file.seek(SeekFrom::Start(0))
            .context("seeking to start of pack")?;

        loop {
            if pos >= file_len {
                break;
            }

            // Need at least 4 bytes for data_length
            if pos + 4 > file_len {
                break;
            }

            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let data_len = u32::from_le_bytes(len_buf);

            // Need 32 bytes for hash
            if pos + 4 + 32 > file_len {
                break;
            }

            let mut hash_buf = [0u8; 32];
            if file.read_exact(&mut hash_buf).is_err() {
                break;
            }

            // Need data_len bytes for data
            let entry_end = pos + ENTRY_HEADER_SIZE + data_len as u64;
            if entry_end > file_len {
                break;
            }

            // Skip over the data
            if file.seek(SeekFrom::Start(entry_end)).is_err() {
                break;
            }

            let hash = Hash(hash_buf);
            let data_offset = pos + ENTRY_HEADER_SIZE;
            index.insert(hash, (data_offset, data_len));

            last_good_pos = entry_end;
            pos = entry_end;
        }

        // Truncate any partial entry at the end
        if last_good_pos < file_len {
            file.set_len(last_good_pos)
                .context("truncating partial entry")?;
        }

        Ok(last_good_pos)
    }

    /// Load the persisted index file with CRC32 validation.
    /// Returns None if the file doesn't exist, CRC mismatch, is too small,
    /// or if pack_size doesn't match the actual pack file length.
    #[allow(clippy::type_complexity)]
    fn load_index(
        idx_path: &Path,
        pack_len: u64,
    ) -> Result<Option<(HashMap<Hash, (u64, u32)>, u64)>> {
        let raw = match fs::read(idx_path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context("reading index file"),
        };

        // Validate CRC32 envelope
        if raw.len() < CRC_SIZE {
            return Ok(None);
        }
        let stored_crc = u32::from_le_bytes(raw[..CRC_SIZE].try_into().unwrap());
        let payload = &raw[CRC_SIZE..];
        let computed_crc = crc32fast::hash(payload);
        if stored_crc != computed_crc {
            return Ok(None); // CRC mismatch → fall back to scan_pack
        }

        if payload.len() < IDX_HEADER_SIZE {
            return Ok(None);
        }

        let stored_pack_size = u64::from_le_bytes(payload[..8].try_into().unwrap());
        if stored_pack_size != pack_len {
            // Pack file changed since index was written — stale
            return Ok(None);
        }

        let entry_bytes = &payload[IDX_HEADER_SIZE..];
        if entry_bytes.len() % IDX_ENTRY_SIZE != 0 {
            return Ok(None);
        }

        let n = entry_bytes.len() / IDX_ENTRY_SIZE;
        let mut index = HashMap::with_capacity(n);

        for i in 0..n {
            let base = i * IDX_ENTRY_SIZE;
            let mut hash_buf = [0u8; 32];
            hash_buf.copy_from_slice(&entry_bytes[base..base + 32]);
            let data_offset =
                u64::from_le_bytes(entry_bytes[base + 32..base + 40].try_into().unwrap());
            let data_len =
                u32::from_le_bytes(entry_bytes[base + 40..base + 44].try_into().unwrap());
            index.insert(Hash(hash_buf), (data_offset, data_len));
        }

        Ok(Some((index, stored_pack_size)))
    }

    /// Atomically write the checksummed index file (CRC32 + temp + fsync + rename + fsync-parent).
    fn save_index(
        idx_path: &Path,
        index: &HashMap<Hash, (u64, u32)>,
        pack_size: u64,
    ) -> Result<()> {
        let tmp = idx_path.with_extension("idx.tmp");

        // Build payload: [8 bytes pack_size][entries...]
        let mut payload = Vec::with_capacity(IDX_HEADER_SIZE + index.len() * IDX_ENTRY_SIZE);
        payload.extend_from_slice(&pack_size.to_le_bytes());
        for (hash, &(data_offset, data_len)) in index {
            payload.extend_from_slice(&hash.0);
            payload.extend_from_slice(&data_offset.to_le_bytes());
            payload.extend_from_slice(&data_len.to_le_bytes());
        }

        // Prepend CRC32
        let crc = crc32fast::hash(&payload);

        let mut f = File::create(&tmp).context("creating temp index file")?;
        f.write_all(&crc.to_le_bytes())
            .context("writing index CRC32")?;
        f.write_all(&payload).context("writing index")?;
        f.sync_all().context("syncing index file")?;
        fs::rename(&tmp, idx_path).context("renaming index file")?;

        // fsync parent directory (NFS safety)
        if let Some(parent) = idx_path.parent()
            && let Ok(dir) = fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }

        Ok(())
    }

    /// Migrate objects from the legacy per-file layout into the pack file.
    fn migrate_legacy(
        legacy_dir: &Path,
        file: &mut File,
        index: &mut HashMap<Hash, (u64, u32)>,
        mut write_pos: u64,
    ) -> Result<u64> {
        // Walk objects/{prefix}/{suffix}
        let entries = fs::read_dir(legacy_dir).context("reading legacy objects dir")?;
        for prefix_entry in entries {
            let prefix_entry = prefix_entry.context("reading prefix dir entry")?;
            let prefix_path = prefix_entry.path();
            if !prefix_path.is_dir() {
                continue;
            }
            let prefix_name = match prefix_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let suffix_entries =
                fs::read_dir(&prefix_path).context("reading suffix dir entries")?;
            for suffix_entry in suffix_entries {
                let suffix_entry = suffix_entry.context("reading suffix entry")?;
                let suffix_path = suffix_entry.path();
                if !suffix_path.is_file() {
                    continue;
                }
                let suffix_name = match suffix_path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                // Skip temp files
                if suffix_name.starts_with(".tmp-") {
                    continue;
                }

                let hex_str = format!("{}{}", prefix_name, suffix_name);
                let hash = match Hash::from_hex(&hex_str) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                if index.contains_key(&hash) {
                    continue;
                }

                let compressed = fs::read(&suffix_path)
                    .with_context(|| format!("reading legacy object {}", hex_str))?;
                let data_len = compressed.len() as u32;

                file.seek(SeekFrom::Start(write_pos))
                    .context("seeking for migration write")?;
                file.write_all(&data_len.to_le_bytes())
                    .context("writing migrated data length")?;
                file.write_all(&hash.0).context("writing migrated hash")?;
                file.write_all(&compressed)
                    .context("writing migrated data")?;

                let data_offset = write_pos + ENTRY_HEADER_SIZE;
                index.insert(hash, (data_offset, data_len));
                write_pos = data_offset + data_len as u64;
            }
        }

        Ok(write_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn open_store(dir: &Path, compression: Compression) -> ObjectStore {
        let pack_path = dir.join("objects.pack");
        let legacy_dir = dir.join("objects");
        ObjectStore::open(pack_path, legacy_dir, compression).unwrap()
    }

    #[test]
    fn put_get_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let data = b"hello merkql";
        let hash = store.put(data).unwrap();
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn put_get_round_trip_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::Lz4);

        let data = b"hello merkql compressed";
        let hash = store.put(data).unwrap();
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn idempotent_put() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let data = b"same content";
        let h1 = store.put(data).unwrap();
        let h2 = store.put(data).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn get_nonexistent_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let hash = Hash::digest(b"not stored");
        assert!(store.get(&hash).is_err());
    }

    #[test]
    fn exists_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let hash = Hash::digest(b"check me");
        assert!(!store.exists(&hash));

        store.put(b"check me").unwrap();
        assert!(store.exists(&hash));
    }

    #[test]
    fn hash_same_regardless_of_compression() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("none")).unwrap();
        fs::create_dir_all(dir.path().join("lz4")).unwrap();
        let store_none = open_store(&dir.path().join("none"), Compression::None);
        let store_lz4 = open_store(&dir.path().join("lz4"), Compression::Lz4);

        let data = b"content to hash";
        let h1 = store_none.put(data).unwrap();
        let h2 = store_lz4.put(data).unwrap();
        assert_eq!(h1, h2, "hash must be computed on uncompressed data");
    }

    #[test]
    fn mixed_compression_read() {
        let dir = tempfile::tempdir().unwrap();

        // Write with None
        {
            let store = open_store(dir.path(), Compression::None);
            store.put(b"written uncompressed").unwrap();
        }

        // Read with Lz4 store — should still work because decompress reads the marker
        let store = open_store(dir.path(), Compression::Lz4);
        let hash = Hash::digest(b"written uncompressed");
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"written uncompressed");
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let hash;

        {
            let store = open_store(dir.path(), Compression::None);
            hash = store.put(b"persistent data").unwrap();
        }

        // Reopen and verify
        let store = open_store(dir.path(), Compression::None);
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"persistent data");
    }

    #[test]
    fn crash_recovery_truncates_partial() {
        let dir = tempfile::tempdir().unwrap();
        let pack_path = dir.path().join("objects.pack");
        let hash;

        {
            let store = open_store(dir.path(), Compression::None);
            hash = store.put(b"good data").unwrap();
            store.flush().unwrap();
        }

        // Append garbage (simulating a partial write / crash)
        {
            let mut f = OpenOptions::new().append(true).open(&pack_path).unwrap();
            f.write_all(&[0xFF; 10]).unwrap(); // partial entry
        }

        // Reopen — should recover the good data and truncate the garbage
        let store = open_store(dir.path(), Compression::None);
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"good data");
    }

    #[test]
    fn legacy_migration() {
        let dir = tempfile::tempdir().unwrap();
        let objects_dir = dir.path().join("objects");

        // Create legacy layout: objects/{prefix}/{suffix}
        let data = b"legacy object data";
        let compressed = Compression::None.compress(data);
        let hash = Hash::digest(data);

        let prefix_dir = objects_dir.join(hash.prefix());
        fs::create_dir_all(&prefix_dir).unwrap();
        fs::write(prefix_dir.join(hash.suffix()), &compressed).unwrap();

        // Open should migrate
        let store = open_store(dir.path(), Compression::None);

        // Legacy dir should be removed
        assert!(!objects_dir.exists());

        // Data should be accessible
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn corrupt_index_crc_triggers_scan_pack_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let hash;

        {
            let store = open_store(dir.path(), Compression::None);
            hash = store.put(b"important data").unwrap();
            store.flush().unwrap();
        }

        // Corrupt the index file CRC
        let idx_path = dir.path().join("objects.idx");
        let mut data = fs::read(&idx_path).unwrap();
        data[0] ^= 0xFF; // flip a CRC byte
        fs::write(&idx_path, &data).unwrap();

        // Reopen — should fall back to scan_pack and recover
        let store = open_store(dir.path(), Compression::None);
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"important data");
    }

    #[test]
    fn get_batch_returns_all_objects() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let data1 = b"batch item 1";
        let data2 = b"batch item 2";
        let data3 = b"batch item 3";

        let h1 = store.put(data1).unwrap();
        let h2 = store.put(data2).unwrap();
        let h3 = store.put(data3).unwrap();

        let results = store.get_batch(&[h1, h2, h3]).unwrap();
        assert_eq!(results.len(), 3);

        // Results should be in order
        assert_eq!(results[0].0, h1);
        assert_eq!(results[0].1, data1.to_vec());
        assert_eq!(results[1].0, h2);
        assert_eq!(results[1].1, data2.to_vec());
        assert_eq!(results[2].0, h3);
        assert_eq!(results[2].1, data3.to_vec());
    }

    #[test]
    fn get_batch_skips_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let h1 = store.put(b"exists").unwrap();
        let missing = Hash::digest(b"not stored");

        let results = store.get_batch(&[h1, missing]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, h1);
    }

    #[test]
    fn get_batch_empty_input() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let results = store.get_batch(&[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn concurrent_reads_same_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open_store(dir.path(), Compression::None));

        let data = b"shared data for concurrent reads";
        let hash = store.put(data).unwrap();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                let hash = hash;
                thread::spawn(move || {
                    for _ in 0..100 {
                        let retrieved = store.get(&hash).unwrap();
                        assert_eq!(retrieved, b"shared data for concurrent reads");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_reads_different_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open_store(dir.path(), Compression::None));

        // Store multiple objects
        let mut hashes = Vec::new();
        for i in 0..10 {
            let data = format!("object-{}", i);
            let hash = store.put(data.as_bytes()).unwrap();
            hashes.push((hash, data));
        }

        let hashes = Arc::new(hashes);

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let store = Arc::clone(&store);
                let hashes = Arc::clone(&hashes);
                thread::spawn(move || {
                    for _ in 0..50 {
                        let idx = (thread_id * 17) % hashes.len();
                        let (hash, expected) = &hashes[idx];
                        let retrieved = store.get(hash).unwrap();
                        assert_eq!(retrieved, expected.as_bytes());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_read_during_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open_store(dir.path(), Compression::None));

        // Store initial object
        let initial_data = b"initial object";
        let initial_hash = store.put(initial_data).unwrap();

        let reader_store = Arc::clone(&store);
        let writer_store = Arc::clone(&store);

        // Reader thread: continuously reads the initial object
        let reader = thread::spawn(move || {
            for _ in 0..500 {
                let retrieved = reader_store.get(&initial_hash).unwrap();
                assert_eq!(retrieved, b"initial object");
            }
        });

        // Writer thread: continuously writes new objects
        let writer = thread::spawn(move || {
            for i in 0..100 {
                let data = format!("new object {}", i);
                writer_store.put(data.as_bytes()).unwrap();
            }
        });

        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn reads_do_not_block_writers() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open_store(dir.path(), Compression::None));

        // Store initial object
        let initial_hash = store.put(b"test object").unwrap();

        let done = Arc::new(AtomicBool::new(false));

        // Reader threads: continuously read
        let reader_handles: Vec<_> = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                let done = Arc::clone(&done);
                let hash = initial_hash;
                thread::spawn(move || {
                    while !done.load(Ordering::Relaxed) {
                        let _ = store.get(&hash);
                    }
                })
            })
            .collect();

        // Give readers time to start
        thread::sleep(Duration::from_millis(10));

        // Writer: should be able to write quickly even with readers
        let start = Instant::now();
        for i in 0..50 {
            let data = format!("write during reads {}", i);
            store.put(data.as_bytes()).unwrap();
        }
        let write_duration = start.elapsed();

        done.store(true, Ordering::Relaxed);
        for h in reader_handles {
            h.join().unwrap();
        }

        // Writes should complete in reasonable time (< 1 second for 50 writes)
        // This would timeout if readers were blocking writers
        assert!(
            write_duration < Duration::from_secs(1),
            "Writes took too long: {:?}",
            write_duration
        );
    }
}
