use async_trait::async_trait;
use hmac::{Hmac, Mac};
use merkql_notify::{AppendNotification, NotifyError, NotifyPlugin, NotifyResult};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Configuration for the webhook notification plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookNotifyConfig {
    /// Default URL to POST notifications to.
    pub url: String,
    /// Per-topic URL overrides. Key is topic name, value is URL.
    pub url_map: Option<HashMap<String, String>>,
    /// HTTP method (default: POST).
    pub method: Option<String>,
    /// Custom headers to include in every request.
    pub headers: Option<HashMap<String, String>>,
    /// Request timeout in milliseconds (default: 5000).
    pub timeout_ms: Option<u64>,
    /// Maximum number of retries for retriable errors (default: 3).
    pub max_retries: Option<u32>,
    /// HMAC-SHA256 signing secret. If set, adds X-MerkQL-Signature header.
    pub signing_secret: Option<String>,
}

/// JSON payload sent to the webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebhookPayload {
    source: String,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
    root_hash: String,
    timestamp_ms: u64,
}

impl From<&AppendNotification> for WebhookPayload {
    fn from(n: &AppendNotification) -> Self {
        WebhookPayload {
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

/// HTTP webhook notification plugin.
pub struct WebhookNotifier {
    config: WebhookNotifyConfig,
    client: Client,
}

impl std::fmt::Debug for WebhookNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookNotifier")
            .field("config", &self.config)
            .finish()
    }
}

impl WebhookNotifier {
    /// Create a new webhook notifier with the given configuration.
    pub fn new(config: WebhookNotifyConfig) -> Result<Self, NotifyError> {
        let timeout = Duration::from_millis(config.timeout_ms.unwrap_or(5000));
        let client = Client::builder().timeout(timeout).build().map_err(|e| {
            NotifyError::Configuration(format!("failed to create HTTP client: {}", e))
        })?;

        Ok(WebhookNotifier { config, client })
    }

    fn get_url(&self, topic: &str) -> &str {
        self.config
            .url_map
            .as_ref()
            .and_then(|m| m.get(topic))
            .map(|s| s.as_str())
            .unwrap_or(&self.config.url)
    }

    fn sign_payload(&self, body: &[u8]) -> Option<String> {
        self.config.signing_secret.as_ref().map(|secret| {
            let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                .expect("HMAC can take key of any size");
            mac.update(body);
            hex::encode(mac.finalize().into_bytes())
        })
    }

    async fn send_with_retries(
        &self,
        url: &str,
        body: Vec<u8>,
        signature: Option<String>,
    ) -> NotifyResult<()> {
        let max_retries = self.config.max_retries.unwrap_or(3);
        let mut last_error = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                // Exponential backoff: 100ms, 200ms, 400ms, ...
                let delay = Duration::from_millis(100 * (1 << (attempt - 1)));
                tokio::time::sleep(delay).await;
            }

            let mut request = self
                .client
                .post(url)
                .header("Content-Type", "application/json")
                .body(body.clone());

            // Add custom headers
            if let Some(headers) = &self.config.headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }

            // Add signature header if configured
            if let Some(ref sig) = signature {
                request = request.header("X-MerkQL-Signature", sig);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return Ok(());
                    } else if status.is_client_error() {
                        // 4xx errors are permanent - don't retry
                        return Err(NotifyError::Permanent(format!(
                            "HTTP {} from {}",
                            status, url
                        )));
                    } else {
                        // 5xx errors are retriable
                        last_error = Some(NotifyError::Retriable(format!(
                            "HTTP {} from {}",
                            status, url
                        )));
                    }
                }
                Err(e) => {
                    if e.is_timeout() || e.is_connect() {
                        last_error =
                            Some(NotifyError::Retriable(format!("connection error: {}", e)));
                    } else {
                        last_error = Some(NotifyError::Retriable(format!("request error: {}", e)));
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| NotifyError::Retriable("unknown error".to_string())))
    }
}

#[async_trait]
impl NotifyPlugin for WebhookNotifier {
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()> {
        let url = self.get_url(&notification.topic);
        let payload = WebhookPayload::from(notification);
        let body = serde_json::to_vec(&payload)
            .map_err(|e| NotifyError::Permanent(format!("JSON serialization failed: {}", e)))?;
        let signature = self.sign_payload(&body);

        self.send_with_retries(url, body, signature).await
    }

    fn name(&self) -> &str {
        "webhook"
    }

    async fn health_check(&self) -> NotifyResult<()> {
        if self.config.url.is_empty() {
            return Err(NotifyError::Configuration("url is empty".to_string()));
        }

        // Try to send a HEAD request to verify connectivity
        match self.client.head(&self.config.url).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.is_connect() || e.is_timeout() {
                    Err(NotifyError::Retriable(format!("server unreachable: {}", e)))
                } else {
                    Err(NotifyError::Configuration(format!("invalid URL: {}", e)))
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

        let payload = WebhookPayload::from(&notification);
        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"source\":\"merkql\""));
        assert!(json.contains("\"topic\":\"orders\""));
        assert!(json.contains("\"partition\":0"));
        assert!(json.contains("\"offset\":42"));
    }

    #[test]
    fn hmac_signature() {
        let config = WebhookNotifyConfig {
            url: "http://localhost".to_string(),
            url_map: None,
            method: None,
            headers: None,
            timeout_ms: None,
            max_retries: None,
            signing_secret: Some("s3cr3t".to_string()),
        };

        let notifier = WebhookNotifier::new(config).unwrap();
        let body = b"{\"test\":true}";
        let signature = notifier.sign_payload(body);

        assert!(signature.is_some());
        // HMAC-SHA256 produces 64 hex characters
        assert_eq!(signature.unwrap().len(), 64);
    }

    #[test]
    fn url_mapping() {
        let mut url_map = HashMap::new();
        url_map.insert(
            "orders".to_string(),
            "http://orders-server/notify".to_string(),
        );

        let config = WebhookNotifyConfig {
            url: "http://default-server/notify".to_string(),
            url_map: Some(url_map),
            method: None,
            headers: None,
            timeout_ms: None,
            max_retries: None,
            signing_secret: None,
        };

        let notifier = WebhookNotifier::new(config).unwrap();

        assert_eq!(notifier.get_url("orders"), "http://orders-server/notify");
        assert_eq!(notifier.get_url("users"), "http://default-server/notify");
    }
}
