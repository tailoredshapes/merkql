Feature: merkql-notify-sqs AWS SQS plugin

  The SQS plugin sends AppendNotification JSON to an AWS SQS queue.
  Lambda event source mappings automatically poll SQS, triggering
  the consumer Lambda at zero additional cost.

  Background:
    Given a mock SQS client
    And a SqsNotifyPlugin configured with queue URL "https://sqs.us-east-1.amazonaws.com/123/merkql-orders"

  # ─── Basic send ───────────────────────────────────────────────────────────────

  Scenario: Successful append sends message to SQS queue
    When on_append is called with topic "orders", partition 0, offset 42, count 1
    Then the SQS client receives exactly 1 SendMessage call
    And the SendMessage targets queue URL "https://sqs.us-east-1.amazonaws.com/123/merkql-orders"
    And the MessageBody is valid JSON containing:
      | field     | value  |
      | source    | merkql |
      | topic     | orders |
      | partition | 0      |
      | offset    | 42     |
      | count     | 1      |
    And the MessageBody contains root_hash and timestamp_ms

  # ─── Per-topic queue mapping ──────────────────────────────────────────────────

  Scenario: Per-topic queue URL overrides the default
    Given the plugin is configured with:
      | default queue URL         | https://sqs.us-east-1.amazonaws.com/123/default  |
      | queue_url_map "orders"    | https://sqs.us-east-1.amazonaws.com/123/orders   |
      | queue_url_map "payments"  | https://sqs.us-east-1.amazonaws.com/123/payments |
    When on_append is called with topic "orders"
    Then the SendMessage targets "https://sqs.us-east-1.amazonaws.com/123/orders"

  Scenario: Topics not in queue_url_map use the default queue URL
    Given the plugin is configured with queue_url_map containing only "orders"
    When on_append is called with topic "users"
    Then the SendMessage targets the default queue URL

  # ─── FIFO queues ─────────────────────────────────────────────────────────────

  Scenario: FIFO queue sets MessageGroupId from topic and partition
    Given the plugin is configured with a FIFO SQS queue
    When on_append is called with topic "orders", partition 2, offset 7
    Then the SendMessage sets MessageGroupId = "orders-2"
    And the SendMessage sets MessageDeduplicationId = "orders-2-7"

  Scenario: Standard queue does not set MessageGroupId or MessageDeduplicationId
    Given the plugin is configured with a standard (non-FIFO) SQS queue
    When on_append is called
    Then the SendMessage does not set MessageGroupId
    And the SendMessage does not set MessageDeduplicationId

  # ─── Delay ───────────────────────────────────────────────────────────────────

  Scenario: Configured delay_seconds is set on the message
    Given the plugin is configured with delay_seconds 30
    When on_append is called
    Then the SendMessage includes DelaySeconds = 30

  Scenario: No delay_seconds configured means no delay on the message
    Given the plugin is configured without delay_seconds
    When on_append is called
    Then the SendMessage does not include DelaySeconds (or includes 0)

  # ─── Batch handling ──────────────────────────────────────────────────────────

  Scenario: Batch append sends a single SQS message with count > 1
    When on_batch is called with topic "orders", partition 0, offset 10, count 5
    Then the SQS client receives exactly 1 SendMessage call
    And the MessageBody contains count 5

  Scenario: on_batch sends one message, not one-per-record
    When on_batch is called with count 100
    Then the SQS client receives exactly 1 SendMessage call

  # ─── Error handling ───────────────────────────────────────────────────────────

  Scenario: SQS throttling returns Retriable
    Given the SQS client returns RequestThrottled on SendMessage
    When on_append is called
    Then on_append returns a Retriable error

  Scenario: SQS invalid attribute value returns Permanent
    Given the SQS client returns InvalidAttributeValue on SendMessage
    When on_append is called
    Then on_append returns a Permanent error

  Scenario: SQS queue does not exist returns Permanent
    Given the SQS client returns NonExistentQueue on SendMessage
    When on_append is called
    Then on_append returns a Permanent error mentioning "queue"

  Scenario: SQS network error returns Retriable
    Given the SQS client returns a network timeout on SendMessage
    When on_append is called
    Then on_append returns a Retriable error

  # ─── health_check ────────────────────────────────────────────────────────────

  Scenario: health_check succeeds when queue is reachable and credentials valid
    Given the SQS client can successfully call GetQueueAttributes on the configured URL
    When health_check is called
    Then it returns Ok

  Scenario: health_check returns Configuration error for empty queue_url
    Given the plugin is configured with an empty queue_url
    When health_check is called
    Then it returns a Configuration error mentioning "queue_url"

  Scenario: health_check returns Permanent error when queue does not exist
    Given the SQS client returns NonExistentQueue for the configured URL
    When health_check is called
    Then it returns a Permanent error mentioning "not found"

  Scenario: health_check returns Configuration error for malformed queue URL
    Given the plugin is configured with queue_url "not-a-url"
    When health_check is called
    Then it returns a Configuration error mentioning "queue_url"
