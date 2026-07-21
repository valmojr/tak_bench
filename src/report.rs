use crate::scenarios::AssertionResult;
use crate::{
    config::{AppConfig, Environment, Profile},
    metrics::MetricsSnapshot,
};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{path::Path, time::Duration};
use time::OffsetDateTime;

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub version: &'static str,
    pub started_at: OffsetDateTime,
    pub finished_at: OffsetDateTime,
    pub elapsed_ms: u128,
    pub environment: Environment,
    pub target: String,
    pub profile: Profile,
    pub clients: u32,
    pub metrics: MetricsSnapshot,
    pub stop_reason: String,
    pub status: RunStatus,
    pub abort_reason: Option<String>,
    pub assertions: Vec<AssertionResult>,
    pub sanitized_config: SanitizedConfig,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Passed,
    Failed,
    Aborted,
}
#[derive(Debug, Serialize)]
pub struct SanitizedConfig {
    pub environment: Environment,
    pub profile: Profile,
    pub participants: Vec<String>,
    pub tls_enabled: bool,
    pub reconnect_enabled: bool,
}

impl RunReport {
    #[must_use]
    pub fn new(
        config: &AppConfig,
        started_at: OffsetDateTime,
        elapsed: Duration,
        metrics: MetricsSnapshot,
        stop_reason: impl Into<String>,
        status: RunStatus,
        assertions: Vec<AssertionResult>,
    ) -> Self {
        let stop_reason = stop_reason.into();
        Self {
            version: env!("CARGO_PKG_VERSION"),
            started_at,
            finished_at: OffsetDateTime::now_utc(),
            elapsed_ms: elapsed.as_millis(),
            environment: config.environment,
            target: config.target.server.clone(),
            profile: config.run.profile,
            clients: config.run.clients,
            metrics,
            abort_reason: (!matches!(status, RunStatus::Passed)).then(|| stop_reason.clone()),
            stop_reason,
            status,
            assertions,
            sanitized_config: SanitizedConfig {
                environment: config.environment,
                profile: config.run.profile,
                participants: config
                    .participants
                    .iter()
                    .map(|participant| participant.id.clone())
                    .collect(),
                tls_enabled: config.tls.enabled,
                reconnect_enabled: config.reconnect.enabled,
            },
        }
    }
    /// # Errors
    ///
    /// Returns an error when the report directory cannot be created or written.
    pub fn write_json(&self, output: &Path) -> Result<()> {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(output, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", output.display()))
    }
    #[must_use]
    pub fn terminal(&self) -> String {
        format!(
            "tak_bench {} against {}: {} sent, {}/{} connections succeeded, p95 handshake: {} ms",
            self.profile_name(),
            self.target,
            self.metrics.sent_messages,
            self.metrics.connection_successes,
            self.metrics.connection_attempts,
            self.metrics
                .handshake_p95_ms
                .map_or_else(|| "n/a".into(), |v| format!("{v:.2}"))
        )
    }
    fn profile_name(&self) -> &'static str {
        match self.profile {
            Profile::Smoke => "smoke",
            Profile::Functional => "functional",
            Profile::Load => "load",
            Profile::Stress => "stress",
            Profile::Spike => "spike",
            Profile::Soak => "soak",
            Profile::Reconnect => "reconnect",
        }
    }
}
