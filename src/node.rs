use crate::hash::Hash;
use serde::{Deserialize, Serialize};

/// A merkle tree node: either a leaf referencing a record, or an internal branch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    Leaf {
        record_hash: Hash,
        offset: u64,
    },
    Branch {
        left: Hash,
        right: Hash,
        offset_range: (u64, u64),
    },
}

impl Node {
    /// Compute the deterministic hash of this node.
    pub fn hash(&self) -> Hash {
        let encoded = bincode::serialize(self).expect("node serialization");
        Hash::digest(&encoded)
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("node serialization")
    }

    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_hash_determinism() {
        let record_hash = Hash::digest(b"record data");
        let leaf1 = Node::Leaf {
            record_hash,
            offset: 0,
        };
        let leaf2 = Node::Leaf {
            record_hash,
            offset: 0,
        };
        assert_eq!(leaf1.hash(), leaf2.hash());
    }

    #[test]
    fn different_offsets_different_hash() {
        let record_hash = Hash::digest(b"record data");
        let leaf1 = Node::Leaf {
            record_hash,
            offset: 0,
        };
        let leaf2 = Node::Leaf {
            record_hash,
            offset: 1,
        };
        assert_ne!(leaf1.hash(), leaf2.hash());
    }

    #[test]
    fn branch_hash_determinism() {
        let left = Hash::digest(b"left");
        let right = Hash::digest(b"right");
        let b1 = Node::Branch {
            left,
            right,
            offset_range: (0, 3),
        };
        let b2 = Node::Branch {
            left,
            right,
            offset_range: (0, 3),
        };
        assert_eq!(b1.hash(), b2.hash());
    }

    #[test]
    fn serialize_round_trip() {
        let record_hash = Hash::digest(b"data");
        let node = Node::Leaf {
            record_hash,
            offset: 42,
        };
        let bytes = node.serialize();
        let restored = Node::deserialize(&bytes).unwrap();
        assert_eq!(node.hash(), restored.hash());
    }
}
