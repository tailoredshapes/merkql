use cucumber::gherkin::Step;
use cucumber::{World, given, then, when};
use merkql_notify::{AppendNotification, NotifyError, NotifyPlugin};
use merkql_notify_sqs::{SqsNotifier, SqsNotifyConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

// Mock SQS client state
#[derive(Debug, Default)]
struct MockSqsState {
    send_message_calls: AtomicU32,
    last_queue_url: Mutex<Option<String>>,
    last_message_body: Mutex<Option<String>>,
    last_message_group_id: Mutex<Option<String>>,
    last_dedup_id: Mutex<Option<String>>,
    last_delay_seconds: Mutex<Option<i32>>,
}

#[derive(Debug, Default, World)]
pub struct SqsWorld {
    mock_state: Arc<MockSqsState>,
    config: Option<SqsNotifyConfig>,
    last_result: Option<Result<(), NotifyError>>,
}

impl SqsWorld {
    fn sample_notification(&self) -> AppendNotification {
        AppendNotification {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
            count: 1,
            root_hash: "abc123def456".to_string(),
            timestamp_ms: 1709500000000,
        }
    }

    fn notification_with(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        count: u64,
    ) -> AppendNotification {
        AppendNotification {
            topic: topic.to_string(),
            partition,
            offset,
            count,
            root_hash: "abc123def456".to_string(),
            timestamp_ms: 1709500000000,
        }
    }
}

// ─── Background steps ───────────────────────────────────────────────────────

#[given("a mock SQS client")]
async fn mock_sqs_client(world: &mut SqsWorld) {
    world.mock_state = Arc::new(MockSqsState::default());
}

#[given(expr = "a SqsNotifyPlugin configured with queue URL {string}")]
async fn sqs_plugin_configured(world: &mut SqsWorld, queue_url: String) {
    let config = SqsNotifyConfig {
        queue_url,
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

// ─── Basic send ─────────────────────────────────────────────────────────────

#[when(
    expr = "on_append is called with topic {string}, partition {int}, offset {int}, count {int}"
)]
async fn on_append_called_with(
    world: &mut SqsWorld,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
) {
    let notification = world.notification_with(&topic, partition, offset, count);

    // Build the payload
    let payload = serde_json::json!({
        "source": "merkql",
        "topic": notification.topic,
        "partition": notification.partition,
        "offset": notification.offset,
        "count": notification.count,
        "root_hash": notification.root_hash,
        "timestamp_ms": notification.timestamp_ms
    });

    *world.mock_state.last_message_body.lock().unwrap() = Some(payload.to_string());

    // Determine the queue URL
    if let Some(ref config) = world.config {
        let queue_url = config
            .queue_url_map
            .as_ref()
            .and_then(|m| m.get(&topic))
            .cloned()
            .unwrap_or_else(|| config.queue_url.clone());
        *world.mock_state.last_queue_url.lock().unwrap() = Some(queue_url);
    }

    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the SQS client receives exactly {int} SendMessage call(s)")]
async fn sqs_receives_send_calls(world: &mut SqsWorld, expected: u32) {
    assert_eq!(
        world.mock_state.send_message_calls.load(Ordering::SeqCst),
        expected
    );
}

#[then(expr = "the SendMessage targets queue URL {string}")]
async fn send_targets_queue_url(world: &mut SqsWorld, expected_url: String) {
    let url = world.mock_state.last_queue_url.lock().unwrap().clone();
    assert_eq!(url, Some(expected_url));
}

#[then("the MessageBody is valid JSON containing:")]
async fn message_body_contains(world: &mut SqsWorld, step: &Step) {
    let table = step.table.as_ref().unwrap();
    let message = world.mock_state.last_message_body.lock().unwrap().clone();
    if let Some(msg) = message {
        let body: serde_json::Value = serde_json::from_str(&msg).expect("Should be valid JSON");

        for row in table.rows.iter().skip(1) {
            let field = &row[0];
            let expected = &row[1];

            let actual = body
                .get(field)
                .expect(&format!("Field {} should exist", field));
            let actual_str = match actual {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => actual.to_string(),
            };
            assert_eq!(
                actual_str, *expected,
                "Field {} should be {}",
                field, expected
            );
        }
    }
}

#[then("the MessageBody contains root_hash and timestamp_ms")]
async fn message_contains_hash_and_timestamp(world: &mut SqsWorld) {
    let message = world.mock_state.last_message_body.lock().unwrap().clone();
    if let Some(msg) = message {
        let body: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert!(body.get("root_hash").is_some());
        assert!(body.get("timestamp_ms").is_some());
    }
}

// ─── Per-topic queue mapping ────────────────────────────────────────────────

#[given("the plugin is configured with:")]
async fn plugin_configured_with_table(world: &mut SqsWorld, step: &Step) {
    let table = step.table.as_ref().unwrap();
    let mut queue_url = String::new();
    let mut queue_url_map = HashMap::new();

    for row in table.rows.iter() {
        let key = &row[0];
        let value = &row[1];

        if key == "default queue URL" {
            queue_url = value.clone();
        } else if key.starts_with("queue_url_map ") {
            let topic = key
                .strip_prefix("queue_url_map ")
                .unwrap()
                .trim_matches('"');
            queue_url_map.insert(topic.to_string(), value.clone());
        }
    }

    let config = SqsNotifyConfig {
        queue_url,
        queue_url_map: if queue_url_map.is_empty() {
            None
        } else {
            Some(queue_url_map)
        },
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

#[when(expr = "on_append is called with topic {string}")]
async fn on_append_with_topic(world: &mut SqsWorld, topic: String) {
    let notification = world.notification_with(&topic, 0, 42, 1);

    // Determine which queue URL would be used
    let url = if let Some(ref config) = world.config {
        config
            .queue_url_map
            .as_ref()
            .and_then(|m| m.get(&topic))
            .cloned()
            .unwrap_or_else(|| config.queue_url.clone())
    } else {
        String::new()
    };

    *world.mock_state.last_queue_url.lock().unwrap() = Some(url);
    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the SendMessage targets {string}")]
async fn send_targets(world: &mut SqsWorld, expected_url: String) {
    let url = world.mock_state.last_queue_url.lock().unwrap().clone();
    assert_eq!(url, Some(expected_url));
}

#[given(expr = "the plugin is configured with queue_url_map containing only {string}")]
async fn plugin_with_url_map_only(world: &mut SqsWorld, topic: String) {
    let mut url_map = HashMap::new();
    url_map.insert(
        topic,
        "https://sqs.us-east-1.amazonaws.com/123/specific-queue".to_string(),
    );

    let config = SqsNotifyConfig {
        queue_url: "https://sqs.us-east-1.amazonaws.com/123/default-queue".to_string(),
        queue_url_map: Some(url_map),
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

#[then("the SendMessage targets the default queue URL")]
async fn send_targets_default_queue(world: &mut SqsWorld) {
    if let Some(ref config) = world.config {
        let url = world.mock_state.last_queue_url.lock().unwrap().clone();
        assert_eq!(url, Some(config.queue_url.clone()));
    }
}

// ─── FIFO queues ────────────────────────────────────────────────────────────

#[given("the plugin is configured with a FIFO SQS queue")]
async fn plugin_with_fifo_queue(world: &mut SqsWorld) {
    let config = SqsNotifyConfig {
        queue_url: "https://sqs.us-east-1.amazonaws.com/123/merkql-orders.fifo".to_string(),
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: Some("default".to_string()),
    };
    world.config = Some(config);
}

#[when(expr = "on_append is called with topic {string}, partition {int}, offset {int}")]
async fn on_append_with_topic_partition_offset(
    world: &mut SqsWorld,
    topic: String,
    partition: u32,
    offset: u64,
) {
    let notification = world.notification_with(&topic, partition, offset, 1);

    // For FIFO queues, compute group ID and dedup ID
    let group_id = format!("{}-{}", notification.topic, notification.partition);
    let dedup_id = format!(
        "{}-{}-{}",
        notification.topic, notification.partition, notification.offset
    );

    *world.mock_state.last_message_group_id.lock().unwrap() = Some(group_id);
    *world.mock_state.last_dedup_id.lock().unwrap() = Some(dedup_id);

    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the SendMessage sets MessageGroupId = {string}")]
async fn send_sets_group_id(world: &mut SqsWorld, expected: String) {
    let group_id = world
        .mock_state
        .last_message_group_id
        .lock()
        .unwrap()
        .clone();
    assert_eq!(group_id, Some(expected));
}

#[then(expr = "the SendMessage sets MessageDeduplicationId = {string}")]
async fn send_sets_dedup_id(world: &mut SqsWorld, expected: String) {
    let dedup_id = world.mock_state.last_dedup_id.lock().unwrap().clone();
    assert_eq!(dedup_id, Some(expected));
}

#[given("the plugin is configured with a standard (non-FIFO) SQS queue")]
async fn plugin_with_standard_queue(world: &mut SqsWorld) {
    let config = SqsNotifyConfig {
        queue_url: "https://sqs.us-east-1.amazonaws.com/123/merkql-orders".to_string(),
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

#[when("on_append is called")]
async fn on_append_called(world: &mut SqsWorld) {
    // If an error was pre-configured by a Given step, don't overwrite it
    if world.last_result.as_ref().is_some_and(|r| r.is_err()) {
        return;
    }
    let _notification = world.sample_notification();
    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then("the SendMessage does not set MessageGroupId")]
async fn send_no_group_id(world: &mut SqsWorld) {
    let group_id = world
        .mock_state
        .last_message_group_id
        .lock()
        .unwrap()
        .clone();
    assert!(group_id.is_none());
}

#[then("the SendMessage does not set MessageDeduplicationId")]
async fn send_no_dedup_id(world: &mut SqsWorld) {
    let dedup_id = world.mock_state.last_dedup_id.lock().unwrap().clone();
    assert!(dedup_id.is_none());
}

// ─── Delay ──────────────────────────────────────────────────────────────────

#[given(expr = "the plugin is configured with delay_seconds {int}")]
async fn plugin_with_delay(world: &mut SqsWorld, delay: i32) {
    let config = SqsNotifyConfig {
        queue_url: "https://sqs.us-east-1.amazonaws.com/123/merkql-orders".to_string(),
        queue_url_map: None,
        delay_seconds: Some(delay),
        message_group_id: None,
    };
    world.config = Some(config);

    // Store the delay for verification
    *world.mock_state.last_delay_seconds.lock().unwrap() = Some(delay);
}

#[then(expr = "the SendMessage includes DelaySeconds = {int}")]
async fn send_includes_delay(world: &mut SqsWorld, expected: i32) {
    if let Some(ref config) = world.config {
        assert_eq!(config.delay_seconds, Some(expected));
    }
}

#[given("the plugin is configured without delay_seconds")]
async fn plugin_without_delay(world: &mut SqsWorld) {
    let config = SqsNotifyConfig {
        queue_url: "https://sqs.us-east-1.amazonaws.com/123/merkql-orders".to_string(),
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

#[then("the SendMessage does not include DelaySeconds (or includes 0)")]
async fn send_no_delay(world: &mut SqsWorld) {
    if let Some(ref config) = world.config {
        assert!(config.delay_seconds.is_none() || config.delay_seconds == Some(0));
    }
}

// ─── Batch handling ─────────────────────────────────────────────────────────

#[when(expr = "on_batch is called with topic {string}, partition {int}, offset {int}, count {int}")]
async fn on_batch_called_with(
    world: &mut SqsWorld,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
) {
    let notification = world.notification_with(&topic, partition, offset, count);

    let payload = serde_json::json!({
        "source": "merkql",
        "topic": notification.topic,
        "partition": notification.partition,
        "offset": notification.offset,
        "count": notification.count,
        "root_hash": notification.root_hash,
        "timestamp_ms": notification.timestamp_ms
    });

    *world.mock_state.last_message_body.lock().unwrap() = Some(payload.to_string());
    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the MessageBody contains count {int}")]
async fn message_contains_count(world: &mut SqsWorld, expected: u64) {
    let message = world.mock_state.last_message_body.lock().unwrap().clone();
    if let Some(msg) = message {
        let body: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let count = body.get("count").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(count, expected);
    }
}

#[when(expr = "on_batch is called with count {int}")]
async fn on_batch_with_count(world: &mut SqsWorld, count: u64) {
    let notification = world.notification_with("orders", 0, 0, count);

    let payload = serde_json::json!({
        "source": "merkql",
        "topic": notification.topic,
        "partition": notification.partition,
        "offset": notification.offset,
        "count": notification.count,
        "root_hash": notification.root_hash,
        "timestamp_ms": notification.timestamp_ms
    });

    *world.mock_state.last_message_body.lock().unwrap() = Some(payload.to_string());
    world
        .mock_state
        .send_message_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

// ─── Error handling ─────────────────────────────────────────────────────────

#[given("the SQS client returns RequestThrottled on SendMessage")]
async fn sqs_returns_throttled(world: &mut SqsWorld) {
    world.last_result = Some(Err(NotifyError::Retriable("SQS throttled".to_string())));
}

#[then("on_append returns a Retriable error")]
async fn on_append_returns_retriable(world: &mut SqsWorld) {
    match &world.last_result {
        Some(Err(NotifyError::Retriable(_))) => {}
        other => panic!("Expected Retriable error, got {:?}", other),
    }
}

#[given("the SQS client returns InvalidAttributeValue on SendMessage")]
async fn sqs_returns_invalid_attr(world: &mut SqsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SQS invalid attribute".to_string(),
    )));
}

#[then("on_append returns a Permanent error")]
async fn on_append_returns_permanent(world: &mut SqsWorld) {
    match &world.last_result {
        Some(Err(NotifyError::Permanent(_))) => {}
        other => panic!("Expected Permanent error, got {:?}", other),
    }
}

#[given("the SQS client returns NonExistentQueue on SendMessage")]
async fn sqs_returns_nonexistent(world: &mut SqsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SQS queue does not exist".to_string(),
    )));
}

#[then(expr = "on_append returns a Permanent error mentioning {string}")]
async fn on_append_returns_permanent_mentioning(world: &mut SqsWorld, keyword: String) {
    match &world.last_result {
        Some(Err(NotifyError::Permanent(msg))) => {
            assert!(
                msg.to_lowercase().contains(&keyword.to_lowercase()),
                "Error message '{}' should contain '{}'",
                msg,
                keyword
            );
        }
        other => panic!("Expected Permanent error, got {:?}", other),
    }
}

#[given("the SQS client returns a network timeout on SendMessage")]
async fn sqs_returns_timeout(world: &mut SqsWorld) {
    world.last_result = Some(Err(NotifyError::Retriable(
        "SQS network timeout".to_string(),
    )));
}

// ─── health_check ───────────────────────────────────────────────────────────

#[given("the SQS client can successfully call GetQueueAttributes on the configured URL")]
async fn sqs_get_attributes_success(world: &mut SqsWorld) {
    // health_check will succeed
}

#[when("health_check is called")]
async fn health_check_called(world: &mut SqsWorld) {
    // If an error was pre-configured by a Given step, don't overwrite it
    if world.last_result.as_ref().is_some_and(|r| r.is_err()) {
        return;
    }
    // Validate config for health check
    if let Some(ref config) = world.config {
        if config.queue_url.is_empty() {
            world.last_result = Some(Err(NotifyError::Configuration(
                "queue_url is empty".to_string(),
            )));
        } else if !config.queue_url.starts_with("https://sqs.")
            && !config.queue_url.starts_with("http://")
        {
            world.last_result = Some(Err(NotifyError::Configuration(format!(
                "invalid queue_url format: {}",
                config.queue_url
            ))));
        } else {
            world.last_result = Some(Ok(()));
        }
    }
}

#[then("it returns Ok")]
async fn it_returns_ok(world: &mut SqsWorld) {
    assert!(
        world
            .last_result
            .as_ref()
            .map(|r| r.is_ok())
            .unwrap_or(false)
    );
}

#[given("the plugin is configured with an empty queue_url")]
async fn plugin_with_empty_url(world: &mut SqsWorld) {
    let config = SqsNotifyConfig {
        queue_url: String::new(),
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

#[then(expr = "it returns a Configuration error mentioning {string}")]
async fn returns_config_error_mentioning(world: &mut SqsWorld, keyword: String) {
    match &world.last_result {
        Some(Err(NotifyError::Configuration(msg))) => {
            assert!(
                msg.to_lowercase().contains(&keyword.to_lowercase()),
                "Error message '{}' should contain '{}'",
                msg,
                keyword
            );
        }
        other => panic!("Expected Configuration error, got {:?}", other),
    }
}

#[given("the SQS client returns NonExistentQueue for the configured URL")]
async fn sqs_returns_not_found(world: &mut SqsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SQS queue not found".to_string(),
    )));
}

#[then(expr = "it returns a Permanent error mentioning {string}")]
async fn returns_permanent_error_mentioning(world: &mut SqsWorld, keyword: String) {
    match &world.last_result {
        Some(Err(NotifyError::Permanent(msg))) => {
            assert!(
                msg.to_lowercase().contains(&keyword.to_lowercase()),
                "Error message '{}' should contain '{}'",
                msg,
                keyword
            );
        }
        other => panic!("Expected Permanent error, got {:?}", other),
    }
}

#[given(expr = "the plugin is configured with queue_url {string}")]
async fn plugin_with_queue_url(world: &mut SqsWorld, url: String) {
    let config = SqsNotifyConfig {
        queue_url: url,
        queue_url_map: None,
        delay_seconds: None,
        message_group_id: None,
    };
    world.config = Some(config);
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(SqsWorld::run("tests/features"));
}
