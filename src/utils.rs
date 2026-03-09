//! Shared utility functions.

use anyhow::{Context, Result};
use std::fs;
use std::io::Write;
use std::path::Path;

/// Atomically write checksummed data to a file using temp+fsync+rename+fsync-parent.
/// Format: [4 bytes CRC32 of data][data...]
pub fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let crc = crc32fast::hash(data);
    let mut f = fs::File::create(&tmp).context("creating temp file for atomic write")?;
    f.write_all(&crc.to_le_bytes()).context("writing CRC")?;
    f.write_all(data).context("writing data")?;
    f.sync_all().context("syncing temp file")?;
    fs::rename(&tmp, path).context("renaming atomic write")?;

    // fsync parent directory to ensure the directory entry is durable (NFS safety)
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

/// Atomically read checksummed data from a file.
/// Returns Ok(None) if CRC doesn't match (corrupt data).
/// Returns Err if file can't be read.
pub fn atomic_read(path: &Path) -> Result<Option<Vec<u8>>> {
    let bytes = fs::read(path).context("reading atomic file")?;
    if bytes.len() < 4 {
        return Ok(None);
    }
    let stored_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let data = &bytes[4..];
    let computed_crc = crc32fast::hash(data);
    if stored_crc != computed_crc {
        return Ok(None);
    }
    Ok(Some(data.to_vec()))
}
