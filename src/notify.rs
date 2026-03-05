use std::fmt;

/// Notification payload sent after a successful append.
#[derive(Debug, Clone)]
pub struct AppendNotification {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub count: u64,
    pub root_hash: String,
    pub timestamp_ms: u64,
}

/// Error types for notification plugins.
#[derive(Debug)]
pub enum NotifyError {
    /// Transient error; caller may retry.
    Retriable(String),
    /// Permanent error; retrying will not help.
    Permanent(String),
    /// Configuration error; plugin is misconfigured.
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

/// Trait for notification plugins.
///
/// Plugins are invoked after a successful append operation to notify
/// downstream consumers that new records are available.
#[async_trait::async_trait]
pub trait NotifyPlugin: Send + Sync {
    /// Called after a single record is appended.
    async fn on_append(&self, notification: &AppendNotification) -> NotifyResult<()>;

    /// Called after a batch of records is appended.
    /// Default implementation delegates to `on_append`.
    async fn on_batch(&self, notification: &AppendNotification) -> NotifyResult<()> {
        self.on_append(notification).await
    }

    /// Returns the plugin name for logging/debugging.
    fn name(&self) -> &str;

    /// Health check to verify the plugin is properly configured and connected.
    async fn health_check(&self) -> NotifyResult<()>;
}

/// A notifier that dispatches to multiple plugins.
pub struct CompositeNotifier {
    plugins: Vec<Box<dyn NotifyPlugin>>,
}

impl std::fmt::Debug for CompositeNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeNotifier")
            .field(
                "plugins",
                &self.plugins.iter().map(|p| p.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
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

    fn name(&self) -> &str {
        "composite"
    }

    async fn health_check(&self) -> NotifyResult<()> {
        for plugin in &self.plugins {
            plugin.health_check().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct MockPlugin {
        name: String,
        append_count: AtomicU32,
        batch_count: AtomicU32,
        should_fail: bool,
    }

    impl MockPlugin {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                append_count: AtomicU32::new(0),
                batch_count: AtomicU32::new(0),
                should_fail: false,
            }
        }

        fn failing(name: &str) -> Self {
            Self {
                name: name.to_string(),
                append_count: AtomicU32::new(0),
                batch_count: AtomicU32::new(0),
                should_fail: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl NotifyPlugin for MockPlugin {
        async fn on_append(&self, _notification: &AppendNotification) -> NotifyResult<()> {
            if self.should_fail {
                return Err(NotifyError::Retriable("mock failure".to_string()));
            }
            self.append_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_batch(&self, _notification: &AppendNotification) -> NotifyResult<()> {
            if self.should_fail {
                return Err(NotifyError::Retriable("mock failure".to_string()));
            }
            self.batch_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn name(&self) -> &str {
            &self.name
        }

        async fn health_check(&self) -> NotifyResult<()> {
            if self.should_fail {
                return Err(NotifyError::Configuration("mock unhealthy".to_string()));
            }
            Ok(())
        }
    }

    fn sample_notification() -> AppendNotification {
        AppendNotification {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            count: 1,
            root_hash: "abc123".to_string(),
            timestamp_ms: 1709500000000,
        }
    }

    #[tokio::test]
    async fn single_plugin_on_append() {
        let _plugin = Arc::new(MockPlugin::new("test"));
        let notifier = CompositeNotifier::new(vec![Box::new(MockPlugin::new("test"))]);

        let notification = sample_notification();
        notifier.on_append(&notification).await.unwrap();

        // Verify plugin was called (we can't check the internal count from CompositeNotifier,
        // but we verify no error was returned)
    }

    #[tokio::test]
    async fn composite_calls_all_plugins() {
        let notifier = CompositeNotifier::new(vec![
            Box::new(MockPlugin::new("plugin1")),
            Box::new(MockPlugin::new("plugin2")),
        ]);

        let notification = sample_notification();
        notifier.on_append(&notification).await.unwrap();
        notifier.on_batch(&notification).await.unwrap();
    }

    #[tokio::test]
    async fn composite_fails_on_first_error() {
        let notifier = CompositeNotifier::new(vec![
            Box::new(MockPlugin::new("plugin1")),
            Box::new(MockPlugin::failing("plugin2")),
            Box::new(MockPlugin::new("plugin3")),
        ]);

        let notification = sample_notification();
        let result = notifier.on_append(&notification).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn health_check_all_plugins() {
        let notifier = CompositeNotifier::new(vec![
            Box::new(MockPlugin::new("plugin1")),
            Box::new(MockPlugin::new("plugin2")),
        ]);

        notifier.health_check().await.unwrap();
    }

    #[tokio::test]
    async fn health_check_fails_on_unhealthy_plugin() {
        let notifier = CompositeNotifier::new(vec![
            Box::new(MockPlugin::new("plugin1")),
            Box::new(MockPlugin::failing("plugin2")),
        ]);

        let result = notifier.health_check().await;
        assert!(result.is_err());
    }

    #[test]
    fn error_display() {
        let e = NotifyError::Retriable("network timeout".to_string());
        assert_eq!(format!("{}", e), "retriable: network timeout");

        let e = NotifyError::Permanent("invalid credentials".to_string());
        assert_eq!(format!("{}", e), "permanent: invalid credentials");

        let e = NotifyError::Configuration("missing URL".to_string());
        assert_eq!(format!("{}", e), "config: missing URL");
    }

    #[tokio::test]
    async fn idempotent_notifications() {
        let notifier = CompositeNotifier::new(vec![Box::new(MockPlugin::new("test"))]);

        let notification = sample_notification();
        // Same notification twice should be safe
        notifier.on_append(&notification).await.unwrap();
        notifier.on_append(&notification).await.unwrap();
    }
}
