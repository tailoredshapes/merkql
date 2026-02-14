use crate::consumer::{Consumer, ConsumerConfig};
use crate::group::{ConsumerGroup, TopicPartition};
use crate::producer::Producer;
use crate::topic::Topic;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub type BrokerRef = Arc<Mutex<Broker>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub default_partitions: u32,
    pub auto_create_topics: bool,
}

impl BrokerConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        BrokerConfig {
            data_dir: data_dir.into(),
            default_partitions: 1,
            auto_create_topics: true,
        }
    }
}

/// Top-level coordinator. Manages topics, consumer groups, and provides
/// factory methods for consumers and producers.
pub struct Broker {
    config: BrokerConfig,
    topics: HashMap<String, Topic>,
    groups: HashMap<String, ConsumerGroup>,
}

impl Broker {
    /// Open or create a broker, scanning for existing topics.
    pub fn open(config: BrokerConfig) -> Result<BrokerRef> {
        let merkql_dir = config.data_dir.join(".merkql");
        fs::create_dir_all(&merkql_dir).context("creating .merkql dir")?;

        // Persist config
        let config_path = merkql_dir.join("config.bin");
        let config_bytes = bincode::serialize(&config).context("serializing config")?;
        fs::write(&config_path, &config_bytes).context("writing config")?;

        let mut topics = HashMap::new();
        let mut groups = HashMap::new();

        // Scan existing topics
        let topics_dir = merkql_dir.join("topics");
        if topics_dir.exists() {
            for entry in fs::read_dir(&topics_dir).context("reading topics dir")? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let topic_dir = entry.path();
                    if topic_dir.join("meta.bin").exists() {
                        let topic = Topic::reopen(&topic_dir)?;
                        topics.insert(topic.name().to_string(), topic);
                    }
                }
            }
        }

        // Scan existing groups
        let groups_dir = merkql_dir.join("groups");
        if groups_dir.exists() {
            for entry in fs::read_dir(&groups_dir).context("reading groups dir")? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let group_dir = entry.path();
                    let group_id = entry.file_name().to_string_lossy().to_string();
                    let group = ConsumerGroup::open(&group_id, &group_dir)?;
                    groups.insert(group_id, group);
                }
            }
        }

        Ok(Arc::new(Mutex::new(Broker {
            config,
            topics,
            groups,
        })))
    }

    /// Create a consumer for this broker.
    pub fn consumer(broker: &BrokerRef, config: ConsumerConfig) -> Consumer {
        Consumer::new(Arc::clone(broker), config)
    }

    /// Create a producer for this broker.
    pub fn producer(broker: &BrokerRef) -> Producer {
        Producer::new(Arc::clone(broker))
    }

    /// Get a topic by name (immutable).
    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.topics.get(name)
    }

    /// Get a topic by name (mutable).
    pub fn topic_mut(&mut self, name: &str) -> Option<&mut Topic> {
        self.topics.get_mut(name)
    }

    /// Get a consumer group by ID.
    pub fn group(&self, group_id: &str) -> Option<&ConsumerGroup> {
        self.groups.get(group_id)
    }

    /// Ensure a topic exists, creating it if auto_create_topics is enabled.
    pub fn ensure_topic(&mut self, name: &str) -> Result<()> {
        if self.topics.contains_key(name) {
            return Ok(());
        }

        if !self.config.auto_create_topics {
            anyhow::bail!(
                "topic '{}' does not exist and auto_create_topics is disabled",
                name
            );
        }

        let topics_dir = self.config.data_dir.join(".merkql").join("topics");
        let topic_dir = topics_dir.join(name);
        let topic = Topic::open(name, self.config.default_partitions, &topic_dir)?;
        self.topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Create a topic explicitly with a given number of partitions.
    pub fn create_topic(&mut self, name: &str, num_partitions: u32) -> Result<()> {
        if self.topics.contains_key(name) {
            return Ok(());
        }
        let topics_dir = self.config.data_dir.join(".merkql").join("topics");
        let topic_dir = topics_dir.join(name);
        let topic = Topic::open(name, num_partitions, &topic_dir)?;
        self.topics.insert(name.to_string(), topic);
        Ok(())
    }

    /// Commit offsets for a consumer group.
    pub fn commit_offsets(
        &mut self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, u64>,
    ) -> Result<()> {
        if !self.groups.contains_key(group_id) {
            let groups_dir = self.config.data_dir.join(".merkql").join("groups");
            let group_dir = groups_dir.join(group_id);
            let group = ConsumerGroup::open(group_id, &group_dir)?;
            self.groups.insert(group_id.to_string(), group);
        }

        let group = self.groups.get_mut(group_id).unwrap();
        group.commit(offsets)
    }
}
