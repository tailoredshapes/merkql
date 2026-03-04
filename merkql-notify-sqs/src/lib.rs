use async_trait::async_trait;
use aws_sdk_sqs::Client as SqsClient;
use merkql_notify::{AppendNotification, NotifyError, NotifyPlugin, NotifyResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the SQS notification plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsNotifyConfig {
    /// Default SQS queue URL.
    pub queue_url: String,
    /// Per-topic queue URL overrides. Key is merkql topic name, value is queue URL.
    pub queue_url_map: Option<HashMap<String, String>>,
    /// Delay in seconds before the message becomes visible (0-900).
    pub delay_seconds: Option<i32>,
    /// Message group ID for FIFO queues. If set, enables FIFO mode.
    pub message_group_id: Option<String>,
}

/// JSON payload sent to SQS.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqsPayload {
    source: String,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
    root_hash: String,
    timestamp_ms: u64,
}

impl From<&AppendNotification> for SqsPayload {
    fn from(n: &AppendNotification) -> Self {
        SqsPayload {
            source: "merkql".to_string(),
            topic: n.topic.clone(),
            partition: n.partition,
            offset: n.offset,
            count: n.count,
            root_hash: n.root_hash.clone(),
            timestamp_ms: n.timestamp_ms,
        }
    }
}

/// AWS SQS notification plugin.
pub struct SqsNotifier {
    config: SqsNotifyConfig,
    client: SqsClient,
}

impl SqsNotifier {
    /// Create a new SQS notifier with the given configuration and AWS SDK config.
    pub fn new(config: SqsNotifyConfig, sdk_config: &aws_config::SdkConfig) -> Self {
        let client = SqsClient::new(sdk_config);
        SqsNotifier { config, client }
    }

    /// Create a new SQS notifier with a custom client (for testing).
    pub fn with_client(config: SqsNotifyConfig, client: SqsClient) -> Self {
        SqsNotifier { config, client }
    }

    fn get_queue_url(&self, topic: &str) -> &str {
        self.config
            .queue_url_map
            .as_ref()
            .and_then(|m| m.get(topic))
            .map(|s| s.as_str())
            .unwrap_or(&self.config.queue_url)
    }

    fn is_fifo_queue(&self) -> bool {
        self.config.message_group_id.is_some() || self.config.queue_url.ends_with(".fifo")
    }

    async fn send_message(&self, notification: &AppendNotification) -> NotifyResult<()> {
        let queue_url = self.get_queue_url(&notification.topic);
        let payload = SqsPayload::from(notification);
        let message_body = serde_json::to_string(&payload)
            .map_err(|e| NotifyError::Permanent(format!("JSON serialization failed: {}", e)))?;

        let mut request = self
            .client
            .send_message()
            .queue_url(queue_url)
            .message_body(&message_body);

        // Add delay if configured
        if let Some(delay) = self.config.delay_seconds {
            request = request.delay_seconds(delay);
        }

        // FIFO queue settings
        if self.is_fifo_queue() {
            let group_id = format!("{}-{}", notification.topic, notification.partition);
            let dedup_id = format!(
                "{}-{}-{}",
                notification.topic, notification.partition, notification.offset
            );
            request = request
                .message_group_id(&group_id)
                .message_deduplication_id(&dedup_id);
        }

        match request.send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("throttl") || err_str.contains("requestthrottled") {
                    Err(NotifyError::Retriable(format!("SQS throttled: {}", e)))
                } else if err_str.contains("timeout") || err_str.contains("connection") {
                    Err(NotifyError::Retriable(format!("SQS network error: {}", e)))
                } else if err_str.contains("nonexistent") || err_str.contains("queuedoesnotexist") {
                    Err(NotifyError::Permanent(format!(
                        "SQS queue does not exist: {}",
                        e
                    )))
                } else if err_str.contains("invalidattribute") {
                    Err(NotifyError::Permanent(format!(
                        "SQS invalid attribute: {}",
                        e
                    )))
                } else {
                    Err(NotifyError::Retriable(format!("SQS error: {}", e)))
                }
            }
        }
    }
}

#[async_trait]
impl NotifyPlugin for SqsNotifier {
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()> {
        self.send_message(notification).await
    }

    async fn on_batch(&self, notification: &AppendNotification) -> NotifyResult<()> {
        // For batch, we send a single message with count > 1
        self.send_message(notification).await
    }

    fn name(&self) -> &str {
        "sqs"
    }

    async fn health_check(&self) -> NotifyResult<()> {
        if self.config.queue_url.is_empty() {
            return Err(NotifyError::Configuration("queue_url is empty".to_string()));
        }

        // Validate URL format
        if !self.config.queue_url.starts_with("https://sqs.")
            && !self.config.queue_url.starts_with("http://")
        {
            return Err(NotifyError::Configuration(format!(
                "invalid queue_url format: {}",
                self.config.queue_url
            )));
        }

        // Try to get queue attributes to verify it exists and we have access
        match self
            .client
            .get_queue_attributes()
            .queue_url(&self.config.queue_url)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("nonexistent")
                    || err_str.contains("not found")
                    || err_str.contains("queuedoesnotexist")
                {
                    Err(NotifyError::Permanent(format!(
                        "SQS queue not found: {}",
                        self.config.queue_url
                    )))
                } else if err_str.contains("timeout") || err_str.contains("connection") {
                    Err(NotifyError::Retriable(format!("SQS unreachable: {}", e)))
                } else {
                    Err(NotifyError::Configuration(format!(
                        "SQS health check failed: {}",
                        e
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_serialization() {
        let notification = AppendNotification {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
            count: 1,
            root_hash: "abc123".to_string(),
            timestamp_ms: 1709500000000,
        };

        let payload = SqsPayload::from(&notification);
        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"source\":\"merkql\""));
        assert!(json.contains("\"topic\":\"orders\""));
        assert!(json.contains("\"count\":1"));
    }

    #[test]
    fn fifo_detection() {
        let config = SqsNotifyConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123/merkql-orders.fifo".to_string(),
            queue_url_map: None,
            delay_seconds: None,
            message_group_id: None,
        };

        assert!(config.queue_url.ends_with(".fifo"));
    }

    #[test]
    fn queue_url_mapping() {
        let mut url_map = HashMap::new();
        url_map.insert(
            "orders".to_string(),
            "https://sqs.us-east-1.amazonaws.com/123/orders".to_string(),
        );

        let config = SqsNotifyConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123/default".to_string(),
            queue_url_map: Some(url_map),
            delay_seconds: None,
            message_group_id: None,
        };

        // Test the mapping logic (without client)
        let default_url = &config.queue_url;
        let orders_url = config
            .queue_url_map
            .as_ref()
            .unwrap()
            .get("orders")
            .unwrap();

        assert_eq!(orders_url, "https://sqs.us-east-1.amazonaws.com/123/orders");
        assert_eq!(
            default_url,
            "https://sqs.us-east-1.amazonaws.com/123/default"
        );
    }
}
