//! Per-implementation test-site provisioning.
//!
//! A "site" is a fresh, isolated storage location an implementation can open a
//! broker against: a temp directory for local merkql, a bucket prefix for
//! merk-aws, a container prefix for merk-azure. Provisioning is the ONLY
//! implementation-specific code in the suite — everything in `tests/` goes
//! through [`TestSite::config`] and the `subject` alias.

use crate::subject::broker::BrokerConfig;

/// A fresh storage location for one certification test. Dropping the site
/// releases its backing resources (temp dir, cloud prefix, ...).
pub struct TestSite {
    location: String,
    #[cfg(feature = "impl-local")]
    _tmp: tempfile::TempDir,
}

impl TestSite {
    /// The opaque location string this site was provisioned at.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// A broker config pointing at this site. Call it repeatedly to reopen the
    /// same site (persistence tests depend on this).
    pub fn config(&self) -> BrokerConfig {
        BrokerConfig::new(self.location.as_str())
    }
}

/// Provision a fresh, empty site for the implementation under test.
#[cfg(feature = "impl-local")]
pub fn fresh_site() -> TestSite {
    let tmp = tempfile::tempdir().expect("provision temp dir");
    TestSite {
        location: tmp.path().to_string_lossy().into_owned(),
        _tmp: tmp,
    }
}
