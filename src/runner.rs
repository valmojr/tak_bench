//! Reusable lifecycle for executing a validated TAK Bench workload.

use std::{sync::Arc, time::Instant};

use crate::lifecycle::{CompletionStatus, LifecycleEmitter, LifecycleEvent};
use crate::report::{RunReport, RunStatus};
use crate::{
    config::AppConfig,
    metrics::Metrics,
    safety::{self, SafetyOptions},
    thresholds,
};
use anyhow::{Result, anyhow};
use tokio::sync::watch;

/// A completed execution always carries a sanitized report, including failed scenarios.
pub struct RunExecution {
    pub report: RunReport,
    pub failure: Option<anyhow::Error>,
}

impl RunExecution {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failure.is_none() && self.report.status == RunStatus::Passed
    }

    /// # Errors
    ///
    /// Returns the workload failure, if the execution did not pass.
    pub fn into_result(self) -> Result<RunReport> {
        self.failure.map_or(Ok(self.report), Err)
    }
}

/// Executes the configured workload while monitoring abort thresholds.
///
/// # Errors
///
/// Returns an error only when configuration is unsafe or invalid. Runtime failures are returned
/// in [`RunExecution`] so callers can persist the report before choosing an exit status.
pub async fn execute(
    config: AppConfig,
    safety_options: SafetyOptions,
    stop: watch::Receiver<bool>,
) -> Result<RunExecution> {
    let lifecycle = LifecycleEmitter::stdout(config.output.lifecycle_jsonl);
    execute_with_lifecycle(config, safety_options, stop, lifecycle).await
}

/// Executes a workload with a caller-provided sanitized lifecycle event sink.
///
/// # Errors
///
/// Returns an error only when configuration is unsafe or invalid.
pub async fn execute_with_lifecycle(
    config: AppConfig,
    safety_options: SafetyOptions,
    stop: watch::Receiver<bool>,
    lifecycle: LifecycleEmitter,
) -> Result<RunExecution> {
    safety::validate_with_options(&config, safety_options)?;
    let metrics = Arc::new(Metrics::new());
    let (threshold_tx, threshold_rx) = watch::channel(*stop.borrow());
    let mut external_stop = stop;
    let forwarded_tx = threshold_tx.clone();
    let forward_stop = tokio::spawn(async move {
        loop {
            if *external_stop.borrow() || external_stop.changed().await.is_err() {
                let _ = forwarded_tx.send(true);
                break;
            }
        }
    });

    let started_at = time::OffsetDateTime::now_utc();
    let started = Instant::now();
    let threshold_reason = Arc::new(tokio::sync::Mutex::new(None));
    let monitor_reason = Arc::clone(&threshold_reason);
    let monitor_metrics = Arc::clone(&metrics);
    let monitor_abort = config.abort.clone();
    let monitor_tx = threshold_tx;
    let monitor = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            ticker.tick().await;
            if let Some(violation) =
                thresholds::evaluate(&monitor_abort, &monitor_metrics.snapshot().await)
            {
                *monitor_reason.lock().await = Some(format!("threshold:{violation:?}"));
                let _ = monitor_tx.send(true);
                break;
            }
        }
    });
    let outcome = crate::scenarios::run_fixed_positions_with_lifecycle(
        config.clone(),
        Arc::clone(&metrics),
        threshold_rx,
        safety_options,
        lifecycle.clone(),
    )
    .await;
    monitor.abort();
    forward_stop.abort();
    let threshold = threshold_reason.lock().await.clone();
    let assertions = outcome
        .as_ref()
        .map_or_else(|_| Vec::new(), |value| value.assertions.clone());
    let assertions_failed = assertions.iter().any(|assertion| !assertion.passed);
    let participant_failures = outcome
        .as_ref()
        .map_or_else(|_| Vec::new(), |value| value.participant_failures.clone());
    let participants_failed = !participant_failures.is_empty();
    let stop_reason = if let Some(reason) = threshold.clone() {
        reason
    } else if outcome.is_ok() && !assertions_failed && !participants_failed {
        "completed".to_owned()
    } else if assertions_failed {
        "routing_assertion_failed".to_owned()
    } else {
        "scenario_error".to_owned()
    };
    let status = if threshold.is_some() {
        RunStatus::Aborted
    } else if outcome.is_ok() && !assertions_failed && !participants_failed {
        RunStatus::Passed
    } else {
        RunStatus::Failed
    };
    let report = RunReport::new(
        &config,
        started_at,
        started.elapsed(),
        metrics.snapshot().await,
        stop_reason,
        status,
        assertions,
        participant_failures,
    );
    let failure = match outcome {
        Err(error) => Some(error),
        Ok(_) if assertions_failed || participants_failed => {
            Some(anyhow!("run failed; see the sanitized JSON report"))
        }
        Ok(_) => None,
    };
    lifecycle.emit(&LifecycleEvent::RunCompleted {
        status: match status {
            RunStatus::Passed => CompletionStatus::Passed,
            RunStatus::Failed => CompletionStatus::Failed,
            RunStatus::Aborted => CompletionStatus::Aborted,
        },
    });
    Ok(RunExecution { report, failure })
}
