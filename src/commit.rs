use crate::hash::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A commit links a merkle tree root to its parent commit, forming a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub parent: Option<Hash>,
    pub tree_root: Hash,
    pub offset_range: (u64, u64),
    pub timestamp: DateTime<Utc>,
}

impl Commit {
    pub fn new(parent: Option<Hash>, tree_root: Hash, offset_range: (u64, u64)) -> Self {
        Commit {
            parent,
            tree_root,
            offset_range,
            timestamp: Utc::now(),
        }
    }

    pub fn hash(&self) -> Hash {
        let encoded = bincode::serialize(self).expect("commit serialization");
        Hash::digest(&encoded)
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("commit serialization")
    }

    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_chain_integrity() {
        let root1 = Hash::digest(b"tree-root-1");
        let c1 = Commit::new(None, root1, (0, 9));
        let h1 = c1.hash();

        let root2 = Hash::digest(b"tree-root-2");
        let c2 = Commit::new(Some(h1), root2, (10, 19));
        let h2 = c2.hash();

        assert_ne!(h1, h2);
        assert_eq!(c2.parent, Some(h1));
    }

    #[test]
    fn commit_serialize_round_trip() {
        let root = Hash::digest(b"root");
        let commit = Commit::new(None, root, (0, 4));
        let bytes = commit.serialize();
        let restored = Commit::deserialize(&bytes).unwrap();
        assert_eq!(commit.tree_root, restored.tree_root);
        assert_eq!(commit.offset_range, restored.offset_range);
        assert_eq!(commit.parent, restored.parent);
    }
}
