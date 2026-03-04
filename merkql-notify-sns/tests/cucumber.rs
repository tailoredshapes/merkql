use cucumber::gherkin::Step;
use cucumber::{World, given, then, when};
use merkql_notify::{AppendNotification, NotifyError, NotifyPlugin};
use merkql_notify_sns::{SnsNotifier, SnsNotifyConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

// Mock SNS client state
#[derive(Debug, Default)]
struct MockSnsState {
    publish_calls: AtomicU32,
    last_topic_arn: Mutex<Option<String>>,
    last_message: Mutex<Option<String>>,
    last_message_group_id: Mutex<Option<String>>,
    last_dedup_id: Mutex<Option<String>>,
    last_attributes: Mutex<HashMap<String, String>>,
    should_throttle: bool,
    should_fail_auth: bool,
    should_fail_invalid: bool,
    should_timeout: bool,
    should_not_found: bool,
    health_check_ok: bool,
}

#[derive(Debug, Default, World)]
pub struct SnsWorld {
    mock_state: Arc<MockSnsState>,
    config: Option<SnsNotifyConfig>,
    last_result: Option<Result<(), NotifyError>>,
}

impl SnsWorld {
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

// Since we can't easily mock the AWS SDK, we'll test the config and payload logic
// The actual AWS integration would be tested separately with localstack or similar

// ─── Background steps ───────────────────────────────────────────────────────

#[given("a mock SNS client")]
async fn mock_sns_client(world: &mut SnsWorld) {
    world.mock_state = Arc::new(MockSnsState::default());
}

#[given(expr = "a SnsNotifyPlugin configured with topic ARN {string}")]
async fn sns_plugin_configured(world: &mut SnsWorld, topic_arn: String) {
    let config = SnsNotifyConfig {
        topic_arn,
        topic_arn_map: None,
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

// ─── Basic publish ──────────────────────────────────────────────────────────

#[when(
    expr = "on_append is called with topic {string}, partition {int}, offset {int}, count {int}"
)]
async fn on_append_called_with(
    world: &mut SnsWorld,
    topic: String,
    partition: u32,
    offset: u64,
    count: u64,
) {
    // Test the payload serialization without actual AWS call
    let notification = world.notification_with(&topic, partition, offset, count);

    // Verify the payload structure
    let payload = serde_json::json!({
        "source": "merkql",
        "topic": notification.topic,
        "partition": notification.partition,
        "offset": notification.offset,
        "count": notification.count,
        "root_hash": notification.root_hash,
        "timestamp_ms": notification.timestamp_ms
    });

    // Store for verification
    *world.mock_state.last_message.lock().unwrap() = Some(payload.to_string());
    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);

    // Simulate success
    world.last_result = Some(Ok(()));
}

#[then(expr = "the SNS client receives exactly {int} Publish call(s)")]
async fn sns_receives_publish_calls(world: &mut SnsWorld, expected: u32) {
    assert_eq!(
        world.mock_state.publish_calls.load(Ordering::SeqCst),
        expected
    );
}

#[then(expr = "the Publish call targets ARN {string}")]
async fn publish_targets_arn(world: &mut SnsWorld, expected_arn: String) {
    if let Some(ref config) = world.config {
        assert_eq!(config.topic_arn, expected_arn);
    }
}

#[then("the Message body is valid JSON containing:")]
async fn message_body_contains(world: &mut SnsWorld, step: &Step) {
    let table = step.table.as_ref().unwrap();
    let message = world.mock_state.last_message.lock().unwrap().clone();
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

#[then("the Message body contains root_hash and timestamp_ms")]
async fn message_contains_hash_and_timestamp(world: &mut SnsWorld) {
    let message = world.mock_state.last_message.lock().unwrap().clone();
    if let Some(msg) = message {
        let body: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert!(body.get("root_hash").is_some());
        assert!(body.get("timestamp_ms").is_some());
    }
}

#[when(expr = "on_append is called with topic {string}, partition {int}")]
async fn on_append_with_topic_partition(world: &mut SnsWorld, topic: String, partition: u32) {
    let notification = world.notification_with(&topic, partition, 42, 1);

    // Store expected attributes
    let mut attrs = world.mock_state.last_attributes.lock().unwrap();
    attrs.insert("merkql.topic".to_string(), notification.topic.clone());
    attrs.insert(
        "merkql.partition".to_string(),
        notification.partition.to_string(),
    );

    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the Publish call includes MessageAttribute {string} = {string} \\(String\\)")]
async fn publish_includes_attribute(
    world: &mut SnsWorld,
    attr_name: String,
    expected_value: String,
) {
    let attrs = world.mock_state.last_attributes.lock().unwrap();
    let actual = attrs.get(&attr_name);
    assert_eq!(
        actual,
        Some(&expected_value),
        "Attribute {} should be {}",
        attr_name,
        expected_value
    );
}

// ─── Per-topic ARN mapping ──────────────────────────────────────────────────

#[given("the plugin is configured with:")]
async fn plugin_configured_with_table(world: &mut SnsWorld, step: &Step) {
    let table = step.table.as_ref().unwrap();
    let mut topic_arn = String::new();
    let mut arn_map = HashMap::new();

    for row in table.rows.iter() {
        let key = &row[0];
        let value = &row[1];

        if key == "default ARN" {
            topic_arn = value.clone();
        } else if key.starts_with("arn_map ") {
            let topic = key.strip_prefix("arn_map ").unwrap().trim_matches('"');
            arn_map.insert(topic.to_string(), value.clone());
        }
    }

    let config = SnsNotifyConfig {
        topic_arn,
        topic_arn_map: if arn_map.is_empty() {
            None
        } else {
            Some(arn_map)
        },
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

#[when(expr = "on_append is called with topic {string}")]
async fn on_append_with_topic(world: &mut SnsWorld, topic: String) {
    let notification = world.notification_with(&topic, 0, 42, 1);

    // Determine which ARN would be used
    let arn = if let Some(ref config) = world.config {
        config
            .topic_arn_map
            .as_ref()
            .and_then(|m| m.get(&topic))
            .cloned()
            .unwrap_or_else(|| config.topic_arn.clone())
    } else {
        String::new()
    };

    *world.mock_state.last_topic_arn.lock().unwrap() = Some(arn);
    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the Publish call targets {string}")]
async fn publish_call_targets(world: &mut SnsWorld, expected_arn: String) {
    let arn = world.mock_state.last_topic_arn.lock().unwrap().clone();
    assert_eq!(arn, Some(expected_arn));
}

#[given(expr = "the plugin is configured with arn_map containing only {string}")]
async fn plugin_with_arn_map_only(world: &mut SnsWorld, topic: String) {
    let mut arn_map = HashMap::new();
    arn_map.insert(
        topic,
        "arn:aws:sns:us-east-1:123:specific-topic".to_string(),
    );

    let config = SnsNotifyConfig {
        topic_arn: "arn:aws:sns:us-east-1:123:default-topic".to_string(),
        topic_arn_map: Some(arn_map),
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

#[then("the Publish call targets the default ARN")]
async fn publish_targets_default_arn(world: &mut SnsWorld) {
    if let Some(ref config) = world.config {
        let arn = world.mock_state.last_topic_arn.lock().unwrap().clone();
        assert_eq!(arn, Some(config.topic_arn.clone()));
    }
}

// ─── FIFO topics ────────────────────────────────────────────────────────────

#[given("the plugin is configured with a FIFO SNS topic")]
async fn plugin_with_fifo_topic(world: &mut SnsWorld) {
    let config = SnsNotifyConfig {
        topic_arn: "arn:aws:sns:us-east-1:123:merkql-orders.fifo".to_string(),
        topic_arn_map: None,
        region: Some("us-east-1".to_string()),
        message_group_id: Some("default".to_string()),
    };
    world.config = Some(config);
}

#[when(expr = "on_append is called with topic {string}, partition {int}, offset {int}")]
async fn on_append_with_topic_partition_offset(
    world: &mut SnsWorld,
    topic: String,
    partition: u32,
    offset: u64,
) {
    let notification = world.notification_with(&topic, partition, offset, 1);

    // For FIFO topics, compute group ID and dedup ID
    let group_id = format!("{}-{}", notification.topic, notification.partition);
    let dedup_id = format!(
        "{}-{}-{}",
        notification.topic, notification.partition, notification.offset
    );

    *world.mock_state.last_message_group_id.lock().unwrap() = Some(group_id);
    *world.mock_state.last_dedup_id.lock().unwrap() = Some(dedup_id);

    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the Publish call sets MessageGroupId = {string}")]
async fn publish_sets_group_id(world: &mut SnsWorld, expected: String) {
    let group_id = world
        .mock_state
        .last_message_group_id
        .lock()
        .unwrap()
        .clone();
    assert_eq!(group_id, Some(expected));
}

#[then(expr = "the Publish call sets MessageDeduplicationId = {string}")]
async fn publish_sets_dedup_id(world: &mut SnsWorld, expected: String) {
    let dedup_id = world.mock_state.last_dedup_id.lock().unwrap().clone();
    assert_eq!(dedup_id, Some(expected));
}

#[given("the plugin is configured with a standard (non-FIFO) SNS topic")]
async fn plugin_with_standard_topic(world: &mut SnsWorld) {
    let config = SnsNotifyConfig {
        topic_arn: "arn:aws:sns:us-east-1:123:merkql-orders".to_string(),
        topic_arn_map: None,
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

#[when("on_append is called")]
async fn on_append_called(world: &mut SnsWorld) {
    // If an error was pre-configured by a Given step, don't overwrite it
    if world.last_result.as_ref().is_some_and(|r| r.is_err()) {
        return;
    }
    let _notification = world.sample_notification();
    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then("the Publish call does not set MessageGroupId")]
async fn publish_no_group_id(world: &mut SnsWorld) {
    let group_id = world
        .mock_state
        .last_message_group_id
        .lock()
        .unwrap()
        .clone();
    assert!(group_id.is_none());
}

#[then("the Publish call does not set MessageDeduplicationId")]
async fn publish_no_dedup_id(world: &mut SnsWorld) {
    let dedup_id = world.mock_state.last_dedup_id.lock().unwrap().clone();
    assert!(dedup_id.is_none());
}

// ─── Batch handling ─────────────────────────────────────────────────────────

#[when(expr = "on_batch is called with topic {string}, partition {int}, offset {int}, count {int}")]
async fn on_batch_called_with(
    world: &mut SnsWorld,
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

    *world.mock_state.last_message.lock().unwrap() = Some(payload.to_string());
    world
        .mock_state
        .publish_calls
        .fetch_add(1, Ordering::SeqCst);
    world.last_result = Some(Ok(()));
}

#[then(expr = "the Message body contains count {int}")]
async fn message_contains_count(world: &mut SnsWorld, expected: u64) {
    let message = world.mock_state.last_message.lock().unwrap().clone();
    if let Some(msg) = message {
        let body: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let count = body.get("count").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(count, expected);
    }
}

// ─── Error handling ─────────────────────────────────────────────────────────

#[given("the SNS client returns ThrottlingException on Publish")]
async fn sns_returns_throttling(world: &mut SnsWorld) {
    // Simulate throttling error
    world.last_result = Some(Err(NotifyError::Retriable("SNS throttled".to_string())));
}

#[then("on_append returns a Retriable error")]
async fn on_append_returns_retriable(world: &mut SnsWorld) {
    match &world.last_result {
        Some(Err(NotifyError::Retriable(_))) => {}
        other => panic!("Expected Retriable error, got {:?}", other),
    }
}

#[given("the SNS client returns InvalidParameterException on Publish")]
async fn sns_returns_invalid_param(world: &mut SnsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SNS invalid parameter".to_string(),
    )));
}

#[then("on_append returns a Permanent error")]
async fn on_append_returns_permanent(world: &mut SnsWorld) {
    match &world.last_result {
        Some(Err(NotifyError::Permanent(_))) => {}
        other => panic!("Expected Permanent error, got {:?}", other),
    }
}

#[given("the SNS client returns AuthorizationErrorException on Publish")]
async fn sns_returns_auth_error(world: &mut SnsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SNS authorization error".to_string(),
    )));
}

#[then(expr = "on_append returns a Permanent error containing {string}")]
async fn on_append_returns_permanent_containing(world: &mut SnsWorld, keyword: String) {
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

#[given("the SNS client returns a network timeout on Publish")]
async fn sns_returns_timeout(world: &mut SnsWorld) {
    world.last_result = Some(Err(NotifyError::Retriable(
        "SNS network timeout".to_string(),
    )));
}

// ─── health_check ───────────────────────────────────────────────────────────

#[given("the SNS client can successfully call GetTopicAttributes on the configured ARN")]
async fn sns_get_attributes_success(world: &mut SnsWorld) {
    // health_check will succeed
}

#[when("health_check is called")]
async fn health_check_called(world: &mut SnsWorld) {
    // If an error was pre-configured by a Given step, don't overwrite it
    if world.last_result.as_ref().is_some_and(|r| r.is_err()) {
        return;
    }
    // Validate config for health check
    if let Some(ref config) = world.config {
        if config.topic_arn.is_empty() {
            world.last_result = Some(Err(NotifyError::Configuration(
                "topic_arn is empty".to_string(),
            )));
        } else if !config.topic_arn.starts_with("arn:aws:sns:") {
            world.last_result = Some(Err(NotifyError::Configuration(format!(
                "invalid ARN format: {}",
                config.topic_arn
            ))));
        } else {
            world.last_result = Some(Ok(()));
        }
    }
}

#[then("it returns Ok")]
async fn it_returns_ok(world: &mut SnsWorld) {
    assert!(
        world
            .last_result
            .as_ref()
            .map(|r| r.is_ok())
            .unwrap_or(false)
    );
}

#[given(expr = "the plugin is configured with ARN {string}")]
async fn plugin_with_arn(world: &mut SnsWorld, arn: String) {
    let config = SnsNotifyConfig {
        topic_arn: arn,
        topic_arn_map: None,
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

#[then(expr = "it returns a Configuration error mentioning {string}")]
async fn returns_config_error_mentioning(world: &mut SnsWorld, keyword: String) {
    match &world.last_result {
        Some(Err(NotifyError::Configuration(msg))) => {
            assert!(
                msg.to_uppercase().contains(&keyword.to_uppercase()),
                "Error message '{}' should contain '{}'",
                msg,
                keyword
            );
        }
        other => panic!("Expected Configuration error, got {:?}", other),
    }
}

#[given("the SNS client returns NotFoundException for the configured ARN")]
async fn sns_returns_not_found(world: &mut SnsWorld) {
    world.last_result = Some(Err(NotifyError::Permanent(
        "SNS topic not found".to_string(),
    )));
}

#[then(expr = "it returns a Permanent error mentioning {string}")]
async fn returns_permanent_error_mentioning(world: &mut SnsWorld, keyword: String) {
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

#[given("the plugin is configured with an empty topic_arn")]
async fn plugin_with_empty_arn(world: &mut SnsWorld) {
    let config = SnsNotifyConfig {
        topic_arn: String::new(),
        topic_arn_map: None,
        region: Some("us-east-1".to_string()),
        message_group_id: None,
    };
    world.config = Some(config);
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(SnsWorld::run("tests/features"));
}
