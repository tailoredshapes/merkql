use crate::hash::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A consumed record — mirrors Kafka's `ConsumerRecord<String, String>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub key: Option<String>,
    pub value: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

impl Record {
    pub fn hash(&self) -> Hash {
        let encoded = bincode::serialize(self).expect("record serialization");
        Hash::digest(&encoded)
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("record serialization")
    }

    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

/// A record to be produced — the producer assigns topic/partition/offset/timestamp.
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub key: Option<String>,
    pub value: String,
    pub topic: String,
}

impl ProducerRecord {
    pub fn new(topic: impl Into<String>, key: Option<String>, value: impl Into<String>) -> Self {
        ProducerRecord {
            key,
            value: value.into(),
            topic: topic.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_serialize_round_trip() {
        let record = Record {
            key: Some("key1".into()),
            value: r#"{"id": 1}"#.into(),
            topic: "test.public.users".into(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        };

        let bytes = record.serialize();
        let restored = Record::deserialize(&bytes).unwrap();
        assert_eq!(record.key, restored.key);
        assert_eq!(record.value, restored.value);
        assert_eq!(record.topic, restored.topic);
        assert_eq!(record.partition, restored.partition);
        assert_eq!(record.offset, restored.offset);
    }

    #[test]
    fn record_hash_determinism() {
        let ts = Utc::now();
        let r1 = Record {
            key: None,
            value: "data".into(),
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
        let r2 = Record {
            key: None,
            value: "data".into(),
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
        assert_eq!(r1.hash(), r2.hash());
    }

    #[test]
    fn producer_record_creation() {
        let pr = ProducerRecord::new("my-topic", Some("k".into()), "v");
        assert_eq!(pr.topic, "my-topic");
        assert_eq!(pr.key, Some("k".into()));
        assert_eq!(pr.value, "v");
    }
}
