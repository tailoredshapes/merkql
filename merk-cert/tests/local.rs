//! Certifies merkql itself — the reference implementation.
//!
//! merkql passing its own suite is not a tautology: the suite is what every
//! other implementation is held to, so this run is what makes the golden
//! wire-format pins and the behavioral contract mean something.

/// A fresh directory for one test. Dropping it removes the storage.
pub struct Site {
    tmp: tempfile::TempDir,
}

impl Site {
    pub fn config(&self) -> merkql::broker::BrokerConfig {
        merkql::broker::BrokerConfig::new(self.tmp.path())
    }
}

pub fn fresh_site() -> Site {
    Site {
        tmp: tempfile::tempdir().expect("provision temp dir"),
    }
}

merk_cert::merk_cert_suite!(merkql, crate::fresh_site);
