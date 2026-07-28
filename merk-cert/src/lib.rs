//! # merk-cert
//!
//! The certification suite that *defines* the merkql surface.
//!
//! Every test in `tests/` is written against [`subject`], a feature-selected
//! alias for the implementation under test. Certification is two checks in one:
//!
//! 1. **Compile** — the implementation exposes the exact module paths, type
//!    names, and signatures the suite imports. If it compiles, the surface
//!    matches; an embedder can swap the dependency and recompile unchanged.
//! 2. **Pass** — the implementation honors the behavioral contract: produce /
//!    consume round-trips, group offset semantics, persistence across reopen,
//!    key routing, and merkle integrity.
//!
//! Run against the reference implementation (local merkql):
//!
//! ```sh
//! cargo test -p merk-cert
//! ```
//!
//! Certify another implementation by wiring its dependency to the matching
//! `impl-*` feature and running:
//!
//! ```sh
//! cargo test -p merk-cert --no-default-features --features impl-aws
//! ```

#[cfg(not(any(feature = "impl-local", feature = "impl-aws", feature = "impl-azure")))]
compile_error!("merk-cert: enable exactly one impl-* feature (default is impl-local)");

/// The implementation under test. All certification tests import through this
/// alias and never name a concrete crate.
#[cfg(feature = "impl-local")]
pub use merkql as subject;

#[cfg(feature = "impl-aws")]
compile_error!(
    "merk-aws is not wired up yet: add its git dependency to merk-cert/Cargo.toml, \
     replace this compile_error! with `pub use merk_aws as subject;`, and provide a \
     `fresh_site()` for it in src/site.rs"
);

#[cfg(feature = "impl-azure")]
compile_error!(
    "merk-azure is not wired up yet: add its git dependency to merk-cert/Cargo.toml, \
     replace this compile_error! with `pub use merk_azure as subject;`, and provide a \
     `fresh_site()` for it in src/site.rs"
);

pub mod site;
