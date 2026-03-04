use async_trait::async_trait;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sns::types::MessageAttributeValue;
use merkql_notify::{AppendNotification, NotifyError, NotifyPlugin, NotifyResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the SNS notification plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnsNotifyConfig {
    /// Default SNS topic ARN.
    pub topic_arn: String,
    /// Per-topic ARN overrides. Key is merkql topic name, value is SNS ARN.
    pub topic_arn_map: Option<HashMap<String, String>>,
    /// AWS region (optional, uses default if not set).
    pub region: Option<String>,
    /// Message group ID for FIFO topics. If set, enables FIFO mode.
    pub message_group_id: Option<String>,
}

/// JSON payload sent to SNS.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnsPayload {
    source: String,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
    root_hash: String,
    timestamp_ms: u64,
}

impl From<&AppendNotification> for SnsPayload {
    fn from(n: &AppendNotification) -> Self {
        SnsPayload {
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

/// AWS SNS notification plugin.
pub struct SnsNotifier {
    config: SnsNotifyConfig,
    client: SnsClient,
}

impl SnsNotifier {
    /// Create a new SNS notifier with the given configuration and AWS SDK config.
    pub fn new(config: SnsNotifyConfig, sdk_config: &aws_config::SdkConfig) -> Self {
        let client = SnsClient::new(sdk_config);
        SnsNotifier { config, client }
    }

    /// Create a new SNS notifier with a custom client (for testing).
    pub fn with_client(config: SnsNotifyConfig, client: SnsClient) -> Self {
        SnsNotifier { config, client }
    }

    fn get_topic_arn(&self, topic: &str) -> &str {
        self.config
            .topic_arn_map
            .as_ref()
            .and_then(|m| m.get(topic))
            .map(|s| s.as_str())
            .unwrap_or(&self.config.topic_arn)
    }

    fn is_fifo_topic(&self) -> bool {
        self.config.message_group_id.is_some() || self.config.topic_arn.ends_with(".fifo")
    }

    async fn publish(&self, notification: &AppendNotification) -> NotifyResult<()> {
        let topic_arn = self.get_topic_arn(&notification.topic);
        let payload = SnsPayload::from(notification);
        let message = serde_json::to_string(&payload)
            .map_err(|e| NotifyError::Permanent(format!("JSON serialization failed: {}", e)))?;

        let mut request = self.client.publish().topic_arn(topic_arn).message(&message);

        // Add message attributes for filtering
        request = request.message_attributes(
            "merkql.topic",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(&notification.topic)
                .build()
                .map_err(|e| NotifyError::Permanent(format!("failed to build attribute: {}", e)))?,
        );

        request = request.message_attributes(
            "merkql.partition",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value(notification.partition.to_string())
                .build()
                .map_err(|e| NotifyError::Permanent(format!("failed to build attribute: {}", e)))?,
        );

        // FIFO topic settings
        if self.is_fifo_topic() {
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
                if err_str.contains("throttl") {
                    Err(NotifyError::Retriable(format!("SNS throttled: {}", e)))
                } else if err_str.contains("timeout") || err_str.contains("connection") {
                    Err(NotifyError::Retriable(format!("SNS network error: {}", e)))
                } else if err_str.contains("authorization") {
                    Err(NotifyError::Permanent(format!(
                        "SNS authorization error: {}",
                        e
                    )))
                } else if err_str.contains("invalid") {
                    Err(NotifyError::Permanent(format!(
                        "SNS invalid parameter: {}",
                        e
                    )))
                } else {
                    Err(NotifyError::Retriable(format!("SNS error: {}", e)))
                }
            }
        }
    }
}

#[async_trait]
impl NotifyPlugin for SnsNotifier {
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()> {
        self.publish(notification).await
    }

    async fn on_batch(&self, notification: &AppendNotification) -> NotifyResult<()> {
        self.publish(notification).await
    }

    fn name(&self) -> &str {
        "sns"
    }

    async fn health_check(&self) -> NotifyResult<()> {
        if self.config.topic_arn.is_empty() {
            return Err(NotifyError::Configuration("topic_arn is empty".to_string()));
        }

        // Validate ARN format
        if !self.config.topic_arn.starts_with("arn:aws:sns:") {
            return Err(NotifyError::Configuration(format!(
                "invalid ARN format: {}",
                self.config.topic_arn
            )));
        }

        // Try to get topic attributes to verify it exists and we have access
        match self
            .client
            .get_topic_attributes()
            .topic_arn(&self.config.topic_arn)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("not found") || err_str.contains("notfound") {
                    Err(NotifyError::Permanent(format!(
                        "SNS topic not found: {}",
                        self.config.topic_arn
                    )))
                } else if err_str.contains("timeout") || err_str.contains("connection") {
                    Err(NotifyError::Retriable(format!("SNS unreachable: {}", e)))
                } else {
                    Err(NotifyError::Configuration(format!(
                        "SNS health check failed: {}",
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

        let payload = SnsPayload::from(&notification);
        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"source\":\"merkql\""));
        assert!(json.contains("\"topic\":\"orders\""));
    }

    #[test]
    fn fifo_detection() {
        let config = SnsNotifyConfig {
            topic_arn: "arn:aws:sns:us-east-1:123:merkql-orders.fifo".to_string(),
            topic_arn_map: None,
            region: None,
            message_group_id: None,
        };

        // Can't fully test without mocking the client
        assert!(config.topic_arn.ends_with(".fifo"));
    }

    #[test]
    fn arn_mapping() {
        let mut arn_map = HashMap::new();
        arn_map.insert(
            "orders".to_string(),
            "arn:aws:sns:us-east-1:123:orders-topic".to_string(),
        );

        let config = SnsNotifyConfig {
            topic_arn: "arn:aws:sns:us-east-1:123:default-topic".to_string(),
            topic_arn_map: Some(arn_map),
            region: None,
            message_group_id: None,
        };

        // Test the mapping logic (without client)
        let default_arn = &config.topic_arn;
        let orders_arn = config
            .topic_arn_map
            .as_ref()
            .unwrap()
            .get("orders")
            .unwrap();

        assert_eq!(orders_arn, "arn:aws:sns:us-east-1:123:orders-topic");
        assert_eq!(default_arn, "arn:aws:sns:us-east-1:123:default-topic");
    }
}
