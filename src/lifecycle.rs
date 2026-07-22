//! Sanitized, opt-in lifecycle events for external orchestration.

use std::{io::Write, sync::Arc};

use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DisconnectReason {
    RunDeadline,
    ExternalStop,
    PeerClosed,
    ReadFailed,
    WriteFailed,
    ParseFailed,
    ConnectFailed,
    ReadinessTimeout,
    ReconnectExhausted,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Passed,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LifecycleEvent {
    ParticipantConnected {
        participant: String,
    },
    ParticipantReady {
        participant: String,
    },
    ParticipantDisconnected {
        participant: String,
        reason: DisconnectReason,
    },
    RunCompleted {
        status: CompletionStatus,
    },
}

/// Cloneable event emitter. Disabled emitters perform no serialization or I/O.
#[derive(Clone)]
pub struct LifecycleEmitter {
    enabled: bool,
    sink: Arc<dyn Fn(&str) + Send + Sync>,
}

impl LifecycleEmitter {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            sink: Arc::new(|_| {}),
        }
    }

    #[must_use]
    pub fn stdout(enabled: bool) -> Self {
        Self::new(enabled, |line| {
            let stdout = std::io::stdout();
            let mut locked = stdout.lock();
            let _ = writeln!(locked, "{line}");
        })
    }

    #[must_use]
    pub fn new<F>(enabled: bool, sink: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        Self {
            enabled,
            sink: Arc::new(sink),
        }
    }

    pub fn emit(&self, event: &LifecycleEvent) {
        if !self.enabled {
            return;
        }
        if let Ok(line) = serde_json::to_string(event) {
            (self.sink)(&line);
        }
    }
}

impl Default for LifecycleEmitter {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_records_only_contain_the_sanitized_schema() {
        let event = LifecycleEvent::ParticipantDisconnected {
            participant: "a2".into(),
            reason: DisconnectReason::PeerClosed,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"event":"participant_disconnected","participant":"a2","reason":"peer_closed"}"#
        );
    }
}
