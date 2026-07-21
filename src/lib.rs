//! Server-neutral TAK/CoT transport benchmark harness.

pub mod config;
pub mod connection;
pub mod metrics;
pub mod protocol;
pub mod provisioning;
pub mod report;
pub mod runner;
pub mod safety;
pub mod scenarios;
pub mod scheduler;
pub mod test_support;
pub mod thresholds;

pub use config::{AppConfig, Environment, Profile};
