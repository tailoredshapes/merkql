Feature: merkql-notify-webhook HTTP webhook plugin

  The webhook plugin POSTs AppendNotification JSON to a configurable HTTP endpoint.
  It is the reference implementation and universal fallback.

  Background:
    Given a running mock HTTP server
    And a WebhookNotifyPlugin configured with the mock server URL

  # ─── Basic delivery ──────────────────────────────────────────────────────────

  Scenario: Successful append POSTs JSON payload to configured URL
    When on_append is called with topic "orders", partition 0, offset 42, count 1
    Then the mock server receives exactly 1 POST request
    And the request Content-Type is "application/json"
    And the request body is valid JSON containing:
      | field        | value    |
      | source       | merkql   |
      | topic        | orders   |
      | partition    | 0        |
      | offset       | 42       |
      | count        | 1        |
    And the request body contains a non-empty root_hash string
    And the request body contains a timestamp_ms integer

  Scenario: HTTP 200 response is treated as success
    Given the mock server returns 200
    When on_append is called
    Then on_append returns Ok

  Scenario: HTTP 201 response is treated as success
    Given the mock server returns 201
    When on_append is called
    Then on_append returns Ok

  Scenario: HTTP 204 response is treated as success
    Given the mock server returns 204
    When on_append is called
    Then on_append returns Ok

  # ─── Error handling ───────────────────────────────────────────────────────────

  Scenario: HTTP 4xx response is a permanent failure (no retry)
    Given the mock server returns 400
    When on_append is called
    Then on_append returns a Permanent error
    And the mock server receives exactly 1 request (no retries)

  Scenario: HTTP 5xx response is retriable
    Given the mock server always returns 500
    And the plugin is configured with max_retries 3
    When on_append is called
    Then on_append returns a Retriable error
    And the mock server receives exactly 4 requests (1 initial + 3 retries)

  Scenario: Connection timeout is retriable
    Given the mock server does not respond within the timeout window
    And the plugin is configured with timeout_ms 100 and max_retries 2
    When on_append is called
    Then on_append returns a Retriable error
    And the total elapsed time is less than 2000ms

  Scenario: Retry uses exponential backoff
    Given the mock server returns 503 for the first 2 attempts then 200
    And the plugin is configured with max_retries 3
    When on_append is called
    Then on_append returns Ok
    And the mock server receives exactly 3 requests
    And the delay between attempt 1 and 2 is approximately 100ms
    And the delay between attempt 2 and 3 is approximately 200ms

  # ─── HMAC signing ────────────────────────────────────────────────────────────

  Scenario: Signing secret adds X-MerkQL-Signature header
    Given the plugin is configured with signing_secret "s3cr3t"
    When on_append is called
    Then the request contains header "X-MerkQL-Signature"
    And the header value is the HMAC-SHA256 of the raw request body using "s3cr3t"

  Scenario: No signing secret means no signature header
    Given the plugin is configured with no signing_secret
    When on_append is called
    Then the request does not contain header "X-MerkQL-Signature"

  # ─── Per-topic URL mapping ────────────────────────────────────────────────────

  Scenario: Per-topic URL overrides the default URL
    Given the plugin is configured with:
      | default URL       | http://default-server/notify   |
      | url_map "orders"  | http://orders-server/notify    |
      | url_map "users"   | http://users-server/notify     |
    When on_append is called with topic "orders"
    Then the request is sent to "http://orders-server/notify"

  Scenario: Topics not in url_map use the default URL
    Given the plugin is configured with:
      | default URL       | http://default-server/notify |
      | url_map "orders"  | http://orders-server/notify  |
    When on_append is called with topic "payments"
    Then the request is sent to "http://default-server/notify"

  # ─── Custom headers ───────────────────────────────────────────────────────────

  Scenario: Custom headers are included in every request
    Given the plugin is configured with headers:
      | Authorization | Bearer tok3n |
      | X-App-ID      | merkql-prod  |
    When on_append is called
    Then the request contains header "Authorization" with value "Bearer tok3n"
    And the request contains header "X-App-ID" with value "merkql-prod"

  # ─── health_check ────────────────────────────────────────────────────────────

  Scenario: health_check succeeds when URL is reachable
    Given the mock server returns 200 for any request
    When health_check is called
    Then it returns Ok

  Scenario: health_check returns Configuration error for empty URL
    Given the plugin is configured with an empty URL
    When health_check is called
    Then it returns a Configuration error mentioning "url"

  Scenario: health_check returns Retriable error when server is unreachable
    Given no server is running at the configured URL
    When health_check is called
    Then it returns a Retriable error
