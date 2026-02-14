use crate::broker::BrokerRef;
use crate::record::{ProducerRecord, Record};
use anyhow::Result;
use chrono::Utc;

/// Kafka-compatible producer. Routes records through the broker's topics.
pub struct Producer {
    broker: BrokerRef,
}

impl Producer {
    pub(crate) fn new(broker: BrokerRef) -> Self {
        Producer { broker }
    }

    /// Send a record. Auto-creates the topic if the broker is configured for it.
    pub fn send(&self, producer_record: &ProducerRecord) -> Result<Record> {
        let mut broker = self
            .broker
            .lock()
            .map_err(|e| anyhow::anyhow!("lock: {}", e))?;

        // Ensure topic exists (auto-create if configured)
        broker.ensure_topic(&producer_record.topic)?;

        let mut record = Record {
            key: producer_record.key.clone(),
            value: producer_record.value.clone(),
            topic: producer_record.topic.clone(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        };

        let topic = broker
            .topic_mut(&producer_record.topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found: {}", producer_record.topic))?;

        topic.append(&mut record)?;
        Ok(record)
    }
}
