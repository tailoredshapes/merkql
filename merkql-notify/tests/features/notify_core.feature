Feature: merkql-notify core trait behaviour

  The notify trait defines the contract all plugins must honour.
  Notification is best-effort: the append is the source of truth.

  Background:
    Given a merkql broker with a registered notification plugin
    And the topic "orders" with partition 0

  # ─── Append triggers notification ───────────────────────────────────────────

  Scenario: Single append fires on_append exactly once
    When a single record is appended to topic "orders" partition 0
    Then the plugin receives exactly 1 call to on_append
    And the notification contains topic "orders", partition 0, count 1
    And the notification contains the Merkle root hash of the append
    And the notification contains a timestamp_ms within 1000ms of now

  Scenario: Batch append fires on_batch exactly once, not per-record
    When a batch of 5 records is appended to topic "orders" partition 0
    Then the plugin receives exactly 1 call to on_batch
    And the plugin receives 0 calls to on_append
    And the notification contains count 5

  Scenario: on_batch default implementation delegates to on_append
    Given a plugin that does not override on_batch
    When a batch of 3 records is appended to topic "orders" partition 0
    Then on_append is called with count 3

  # ─── Notification failure must not fail the append ───────────────────────────

  Scenario: Retriable notification error does not fail the append
    Given the plugin is configured to return a Retriable error on on_append
    When a single record is appended to topic "orders" partition 0
    Then the append succeeds and returns the new offset
    And a warning is logged containing the plugin name and error message
    And the record is durable on the filesystem

  Scenario: Permanent notification error does not fail the append
    Given the plugin is configured to return a Permanent error on on_append
    When a single record is appended to topic "orders" partition 0
    Then the append succeeds and returns the new offset
    And a warning is logged containing the plugin name and error message

  Scenario: Plugin panic does not crash the producer
    Given the plugin is configured to panic on on_append
    When a single record is appended to topic "orders" partition 0
    Then the append succeeds and returns the new offset

  # ─── health_check ────────────────────────────────────────────────────────────

  Scenario: health_check returns Ok when plugin is correctly configured
    Given a correctly configured plugin
    When health_check is called
    Then it returns Ok

  Scenario: health_check returns Configuration error for missing required config
    Given a plugin with a missing required configuration field
    When health_check is called
    Then it returns a Configuration error with a descriptive message

  # ─── CompositeNotifier ───────────────────────────────────────────────────────

  Scenario: CompositeNotifier fans out to all registered plugins
    Given a CompositeNotifier with 3 plugins registered
    When a single record is appended
    Then each plugin receives exactly 1 call to on_append

  Scenario: CompositeNotifier stops on first error
    Given a CompositeNotifier with plugins [A, B, C]
    And plugin B is configured to return a Retriable error
    When a single record is appended
    Then plugin A receives on_append
    And plugin B receives on_append and returns an error
    And plugin C does not receive on_append

  Scenario: CompositeNotifier health_check fails fast on first unhealthy plugin
    Given a CompositeNotifier with plugins [A, B, C]
    And plugin B is unhealthy
    When health_check is called on the composite
    Then it returns the error from plugin B
    And plugin C health_check is not called

  # ─── No plugin configured ────────────────────────────────────────────────────

  Scenario: Broker with no notification plugin appends silently
    Given a merkql broker with no notification plugin configured
    When a single record is appended to topic "orders" partition 0
    Then the append succeeds and returns the new offset
    And no notification is sent
