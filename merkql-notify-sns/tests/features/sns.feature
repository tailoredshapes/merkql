Feature: merkql-notify-sns AWS SNS plugin

  The SNS plugin publishes AppendNotification JSON to an AWS SNS topic,
  enabling Lambda fan-out via SNS subscriptions.

  Background:
    Given a mock SNS client
    And a SnsNotifyPlugin configured with topic ARN "arn:aws:sns:us-east-1:123456789:merkql-orders"

  # ─── Basic publish ───────────────────────────────────────────────────────────

  Scenario: Successful append publishes to SNS topic
    When on_append is called with topic "orders", partition 0, offset 42, count 1
    Then the SNS client receives exactly 1 Publish call
    And the Publish call targets ARN "arn:aws:sns:us-east-1:123456789:merkql-orders"
    And the Message body is valid JSON containing:
      | field     | value  |
      | source    | merkql |
      | topic     | orders |
      | partition | 0      |
      | offset    | 42     |
      | count     | 1      |
    And the Message body contains root_hash and timestamp_ms

  Scenario: SNS publish sets MessageAttributes for topic filtering
    When on_append is called with topic "orders", partition 0
    Then the Publish call includes MessageAttribute "merkql.topic" = "orders" (String)
    And the Publish call includes MessageAttribute "merkql.partition" = "0" (String)

  # ─── Per-topic ARN mapping ────────────────────────────────────────────────────

  Scenario: Per-topic ARN overrides the default ARN
    Given the plugin is configured with:
      | default ARN          | arn:aws:sns:us-east-1:123:default        |
      | arn_map "orders"     | arn:aws:sns:us-east-1:123:orders-topic   |
      | arn_map "payments"   | arn:aws:sns:us-east-1:123:payments-topic |
    When on_append is called with topic "orders"
    Then the Publish call targets "arn:aws:sns:us-east-1:123:orders-topic"

  Scenario: Topics not in arn_map use the default ARN
    Given the plugin is configured with arn_map containing only "orders"
    When on_append is called with topic "users"
    Then the Publish call targets the default ARN

  # ─── FIFO topics ─────────────────────────────────────────────────────────────

  Scenario: FIFO topic sets MessageGroupId from topic and partition
    Given the plugin is configured with a FIFO SNS topic
    When on_append is called with topic "orders", partition 2, offset 7
    Then the Publish call sets MessageGroupId = "orders-2"
    And the Publish call sets MessageDeduplicationId = "orders-2-7"

  Scenario: Standard topic does not set MessageGroupId or MessageDeduplicationId
    Given the plugin is configured with a standard (non-FIFO) SNS topic
    When on_append is called
    Then the Publish call does not set MessageGroupId
    And the Publish call does not set MessageDeduplicationId

  # ─── Batch handling ──────────────────────────────────────────────────────────

  Scenario: Batch append sends a single SNS message
    When on_batch is called with topic "orders", partition 0, offset 10, count 5
    Then the SNS client receives exactly 1 Publish call
    And the Message body contains count 5

  # ─── Error handling ───────────────────────────────────────────────────────────

  Scenario: SNS throttling error returns Retriable
    Given the SNS client returns ThrottlingException on Publish
    When on_append is called
    Then on_append returns a Retriable error

  Scenario: SNS invalid parameter error returns Permanent
    Given the SNS client returns InvalidParameterException on Publish
    When on_append is called
    Then on_append returns a Permanent error

  Scenario: SNS authorization failure returns Permanent
    Given the SNS client returns AuthorizationErrorException on Publish
    When on_append is called
    Then on_append returns a Permanent error containing "authorization"

  Scenario: SNS endpoint unreachable returns Retriable
    Given the SNS client returns a network timeout on Publish
    When on_append is called
    Then on_append returns a Retriable error

  # ─── health_check ────────────────────────────────────────────────────────────

  Scenario: health_check succeeds when topic ARN is reachable and credentials valid
    Given the SNS client can successfully call GetTopicAttributes on the configured ARN
    When health_check is called
    Then it returns Ok

  Scenario: health_check returns Configuration error for malformed ARN
    Given the plugin is configured with ARN "not-a-valid-arn"
    When health_check is called
    Then it returns a Configuration error mentioning "ARN"

  Scenario: health_check returns Permanent error when topic does not exist
    Given the SNS client returns NotFoundException for the configured ARN
    When health_check is called
    Then it returns a Permanent error mentioning "not found"

  Scenario: health_check returns Configuration error when topic_arn is empty
    Given the plugin is configured with an empty topic_arn
    When health_check is called
    Then it returns a Configuration error mentioning "topic_arn"
