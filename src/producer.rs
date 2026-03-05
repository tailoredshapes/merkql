use crate::broker::BrokerRef;
use crate::record::{ProducerRecord, Record};
use anyhow::Result;
use chrono::Utc;

#[cfg(feature = "notify")]
use crate::notify::AppendNotification;

/// Kafka-compatible producer. Routes records through the broker's topics.
pub struct Producer {
    broker: BrokerRef,
}

impl Producer {
    pub(crate) fn new(broker: BrokerRef) -> Self {
        Producer { broker }
    }

    /// Send a record. Auto-creates the topic if the broker is configured for it.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// let record = producer.send(&ProducerRecord::new("events", None, "hello")).unwrap();
    /// assert_eq!(record.value, "hello");
    /// ```
    pub fn send(&self, producer_record: &ProducerRecord) -> Result<Record> {
        // Ensure topic exists (auto-create if configured)
        self.broker.ensure_topic(&producer_record.topic)?;

        let mut record = Record {
            key: producer_record.key.clone(),
            value: producer_record.value.clone(),
            topic: producer_record.topic.clone(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        };

        let topic = self
            .broker
            .topic(&producer_record.topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found: {}", producer_record.topic))?;

        topic.append(&mut record)?;

        // Fire notification after successful append
        #[cfg(feature = "notify")]
        {
            let root_hash = if let Some(partition) = topic.partition(record.partition) {
                if let Ok(guard) = partition.read() {
                    guard
                        .merkle_root()
                        .ok()
                        .flatten()
                        .map(|h| hex::encode(h.0))
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            let notification = AppendNotification {
                topic: record.topic.clone(),
                partition: record.partition,
                offset: record.offset,
                count: 1,
                root_hash,
                timestamp_ms: record.timestamp.timestamp_millis() as u64,
            };
            self.broker.notify_append(notification);
        }

        Ok(record)
    }

    /// Send a batch of records to the same topic.
    /// Returns all records with their assigned offsets and partitions.
    /// Fires a single batch notification (not per-record).
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// let batch: Vec<ProducerRecord> = (0..3)
    ///     .map(|i| ProducerRecord::new("events", None, format!("msg-{}", i)))
    ///     .collect();
    /// let results = producer.send_batch(&batch).unwrap();
    /// assert_eq!(results.len(), 3);
    /// ```
    pub fn send_batch(&self, producer_records: &[ProducerRecord]) -> Result<Vec<Record>> {
        if producer_records.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(producer_records.len());
        #[cfg(feature = "notify")]
        let mut first_offset = 0u64;
        #[cfg(feature = "notify")]
        let mut last_partition = 0u32;
        #[cfg(feature = "notify")]
        let mut topic_name = String::new();

        #[allow(unused_variables)]
        for (i, pr) in producer_records.iter().enumerate() {
            // Ensure topic exists (auto-create if configured)
            self.broker.ensure_topic(&pr.topic)?;

            let mut record = Record {
                key: pr.key.clone(),
                value: pr.value.clone(),
                topic: pr.topic.clone(),
                partition: 0,
                offset: 0,
                timestamp: Utc::now(),
            };

            let topic = self
                .broker
                .topic(&pr.topic)
                .ok_or_else(|| anyhow::anyhow!("topic not found: {}", pr.topic))?;

            topic.append(&mut record)?;

            #[cfg(feature = "notify")]
            if i == 0 {
                first_offset = record.offset;
                topic_name = record.topic.clone();
            }
            #[cfg(feature = "notify")]
            {
                last_partition = record.partition;
            }

            results.push(record);
        }

        // Fire single batch notification after all records appended
        #[cfg(feature = "notify")]
        if !results.is_empty() {
            let root_hash = if let Some(topic) = self.broker.topic(&topic_name) {
                if let Some(partition) = topic.partition(last_partition) {
                    if let Ok(guard) = partition.read() {
                        guard
                            .merkle_root()
                            .ok()
                            .flatten()
                            .map(|h| hex::encode(h.0))
                            .unwrap_or_default()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            let last_record = results.last().unwrap();
            let notification = AppendNotification {
                topic: topic_name,
                partition: last_partition,
                offset: first_offset,
                count: results.len() as u64,
                root_hash,
                timestamp_ms: last_record.timestamp.timestamp_millis() as u64,
            };
            self.broker.notify_batch(notification);
        }

        Ok(results)
    }
}
