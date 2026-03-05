# merkql Notification Plugins — Implementation Prompt

## Context

merkql is a Rust library providing Kafka-compatible event streaming semantics (topics, partitions, consumer groups, offset management) backed by a Merkle tree, designed for embedded use with zero infrastructure. It stores data on a local or shared filesystem (NFS, SAMBA, EFS, Filestore, etc.).

The deployment model is:
- **Single writer** per topic/partition (append-only)
- **Hundreds of concurrent readers** with no contention
- Cloud functions (Lambda, Cloud Functions, Azure Functions) or containers access merkql via a shared mounted filesystem

The missing piece: **consumer notification**. When a writer appends a record, downstream consumers need to be triggered. In a traditional server, you'd poll or use a filesystem watcher. In a serverless/function environment, you need an external trigger mechanism.

## Architecture

### Core Trait Crate: `merkql-notify`

This crate contains only the notification trait and shared types. It has **no cloud SDK dependencies**. Both the core `merkql` crate and all plugin crates depend on this.

```rust
use std::fmt;

#[derive(Debug, Clone)]
pub struct AppendNotification {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub count: u64,
    pub root_hash: String,
    pub timestamp_ms: u64,
}

#[derive(Debug)]
pub enum NotifyError {
    Retriable(String),
    Permanent(String),
    Configuration(String),
}

impl fmt::Display for NotifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyError::Retriable(msg) => write!(f, "retriable: {}", msg),
            NotifyError::Permanent(msg) => write!(f, "permanent: {}", msg),
            NotifyError::Configuration(msg) => write!(f, "config: {}", msg),
        }
    }
}

impl std::error::Error for NotifyError {}

pub type NotifyResult<T> = Result<T, NotifyError>;

#[async_trait::async_trait]
pub trait NotifyPlugin: Send + Sync {
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()>;
    async fn on_batch(&self, notification: &AppendNotification) -> NotifyResult<()> {
        self.on_append(notification).await
    }
    fn name(&self) -> &str;
    async fn health_check(&self) -> NotifyResult<()>;
}

pub struct CompositeNotifier {
    plugins: Vec<Box<dyn NotifyPlugin>>,
}

impl CompositeNotifier {
    pub fn new(plugins: Vec<Box<dyn NotifyPlugin>>) -> Self {
        Self { plugins }
    }
}

#[async_trait::async_trait]
impl NotifyPlugin for CompositeNotifier {
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()> {
        for plugin in &self.plugins {
            plugin.on_append(notification).await?;
        }
        Ok(())
    }
    async fn on_batch(&self, notification: &AppendNotification) -> NotifyResult<()> {
        for plugin in &self.plugins {
            plugin.on_batch(notification).await?;
        }
        Ok(())
    }
    fn name(&self) -> &str { "composite" }
    async fn health_check(&self) -> NotifyResult<()> {
        for plugin in &self.plugins {
            plugin.health_check().await?;
        }
        Ok(())
    }
}
```

### Integration with merkql core

After a successful append (once fsync'd and Merkle tree updated), call `on_append`/`on_batch`. **Notification failure must never fail the append.** Fire-and-forget, log errors only.

```rust
if let Some(ref notifier) = self.notifier {
    let notification = AppendNotification { ... };
    if let Err(e) = notifier.on_append(&notification).await {
        tracing::warn!(plugin = notifier.name(), error = %e, "notification failed");
    }
}
```

### Notification JSON Payload (all plugins use this)

```json
{
  "source": "merkql",
  "topic": "orders",
  "partition": 0,
  "offset": 42,
  "count": 1,
  "root_hash": "a1b2c3...",
  "timestamp_ms": 1709500000000
}
```

---

## Plugin Implementations (implement in this order)

### 1. `merkql-notify-webhook` — Generic HTTP Webhook (implement first)

Deps: `merkql-notify`, `reqwest`, `serde_json`, `tokio`, `async-trait`

```rust
pub struct WebhookNotifyConfig {
    pub url: String,
    pub url_map: Option<HashMap<String, String>>,
    pub method: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub signing_secret: Option<String>,
}
```

- HTTP POST with JSON payload
- If `signing_secret`: add `X-MerkQL-Signature` header (HMAC-SHA256 of body)
- Retry with exponential backoff on 5xx/timeout; 4xx = permanent failure
- This is the reference implementation and universal fallback

### 2. `merkql-notify-sns` — AWS SNS

Deps: `merkql-notify`, `aws-sdk-sns`, `aws-config`, `serde_json`, `tokio`, `async-trait`

```rust
pub struct SnsNotifyConfig {
    pub topic_arn: String,
    pub topic_arn_map: Option<HashMap<String, String>>,
    pub region: Option<String>,
    pub message_group_id: Option<String>, // FIFO topics
}
```

- Publish JSON payload to SNS
- MessageAttributes: `merkql.topic`, `merkql.partition` for filter policies
- FIFO: MessageGroupId = `{topic}-{partition}`, MessageDeduplicationId = `{topic}-{partition}-{offset}`
- IAM: `sns:Publish`

### 3. `merkql-notify-sqs` — AWS SQS

Deps: `merkql-notify`, `aws-sdk-sqs`, `aws-config`, `serde_json`, `tokio`, `async-trait`

```rust
pub struct SqsNotifyConfig {
    pub queue_url: String,
    pub queue_url_map: Option<HashMap<String, String>>,
    pub delay_seconds: Option<i32>,
    pub message_group_id: Option<String>, // FIFO queues
}
```

- Send JSON to SQS
- FIFO: MessageGroupId = `{topic}-{partition}`, MessageDeduplicationId = `{topic}-{partition}-{offset}`
- `on_batch` sends single message with count > 1
- IAM: `sqs:SendMessage`

---

## Crate Structure

These should be **workspace members** added to the existing merkql Cargo.toml workspace:

```
merkql-notify/          ← new crate, trait + types only
merkql-notify-webhook/  ← new crate
merkql-notify-sns/      ← new crate
merkql-notify-sqs/      ← new crate
```

## Testing

Each plugin needs:
1. Unit tests with mocked backends (mockito or wiremock for webhook, mock AWS client for SNS/SQS)
2. `merkql-notify` ships a test harness that all plugins run:
   - `on_append` called with correct payload after single append
   - `on_batch` called once (not per-record) after batch
   - notification failure does NOT fail the append
   - `health_check` returns meaningful errors for bad config
   - idempotency: same notification twice is safe

## Implementation Instructions

1. Create `merkql-notify` crate with the trait, types, CompositeNotifier, and test harness
2. Add it as a workspace member in root Cargo.toml
3. Wire `merkql` core to accept `Option<Box<dyn NotifyPlugin>>` in Producer/Broker
4. Create `merkql-notify-webhook` crate with tests
5. Create `merkql-notify-sns` crate with tests  
6. Create `merkql-notify-sqs` crate with tests
7. Run `cargo build --workspace` and `cargo test --workspace` — all must pass
8. Commit with message: "feat: add merkql-notify trait crate and webhook/SNS/SQS plugins"

When completely finished, run: openclaw system event --text "Done: merkql-notify trait + webhook + SNS + SQS plugins implemented and tested" --mode now
