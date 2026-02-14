use crate::hash::Hash;
use anyhow::{Context, Result};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Content-addressed file store using SHA-256 hashes.
/// Objects are stored at `{root}/{prefix}/{suffix}` where prefix is the first
/// byte (2 hex chars) and suffix is the remaining 31 bytes (62 hex chars).
pub struct ObjectStore {
    root: PathBuf,
}

impl ObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        ObjectStore { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Store bytes, returning their content hash. Idempotent — if the object
    /// already exists, this is a no-op.
    pub fn put(&self, data: &[u8]) -> Result<Hash> {
        let hash = Hash::digest(data);
        let path = self.object_file(&hash);

        if path.exists() {
            return Ok(hash);
        }

        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).context("creating object dir")?;

        // Atomic write: temp file + rename
        let tmp_path = dir.join(format!(".tmp-{}", std::process::id()));
        let mut file = fs::File::create(&tmp_path).context("creating temp file")?;
        file.write_all(data).context("writing object")?;
        file.sync_all().context("syncing object")?;
        fs::rename(&tmp_path, &path).context("renaming object")?;

        Ok(hash)
    }

    /// Retrieve bytes by hash.
    pub fn get(&self, hash: &Hash) -> Result<Vec<u8>> {
        let path = self.object_file(hash);
        fs::read(&path).with_context(|| format!("reading object {}", hash))
    }

    /// Check if an object exists.
    pub fn exists(&self, hash: &Hash) -> bool {
        self.object_file(hash).exists()
    }

    fn object_file(&self, hash: &Hash) -> PathBuf {
        self.root.join(hash.object_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::new(dir.path().join("objects"));

        let data = b"hello merkql";
        let hash = store.put(data).unwrap();
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn idempotent_put() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::new(dir.path().join("objects"));

        let data = b"same content";
        let h1 = store.put(data).unwrap();
        let h2 = store.put(data).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn get_nonexistent_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::new(dir.path().join("objects"));

        let hash = Hash::digest(b"not stored");
        assert!(store.get(&hash).is_err());
    }

    #[test]
    fn exists_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::new(dir.path().join("objects"));

        let hash = Hash::digest(b"check me");
        assert!(!store.exists(&hash));

        store.put(b"check me").unwrap();
        assert!(store.exists(&hash));
    }
}
