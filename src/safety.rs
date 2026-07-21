use std::{collections::HashSet, time::Duration};

use crate::config::{
    AppConfig, Environment, Movement, ParticipantRole, Profile, ScenarioKind, host_from_server,
    is_loopback,
};
use thiserror::Error;

pub const AUTHORIZATION_BANNER: &str =
    "Use somente contra servidores que você administra ou possui autorização para testar.";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SafetyError {
    #[error(
        "explicit authorization is required: use --acknowledge-authorization or authorization.acknowledged: true"
    )]
    AuthorizationRequired,
    #[error("target host {0:?} is not in allow_hosts")]
    HostNotAllowed(String),
    #[error("production requires --allow-production")]
    ProductionNotAllowed,
    #[error("profile {0:?} is not permitted in production")]
    ProductionProfile(Profile),
    #[error("production permits at most three clients")]
    ProductionClients,
    #[error("production runs may not exceed 15 minutes")]
    ProductionDuration,
    #[error("production position interval must be at least 30 seconds")]
    ProductionRate,
    #[error("clients cannot exceed max_clients")]
    ClientLimit,
    #[error("run.clients must match the number of explicit participants")]
    ParticipantCountMismatch,
    #[error("at least one participant is required")]
    NoParticipants,
    #[error("participant IDs must be non-empty and unique")]
    InvalidParticipantId,
    #[error("routing assertions must reference participants with compatible roles")]
    InvalidRoutingAssertion,
    #[error("only fixed position workloads are currently implemented")]
    UnsupportedWorkload,
    #[error("the configured option is not implemented by the current runner")]
    UnsupportedOption,
    #[error("participant TLS certificate templates are invalid")]
    InvalidTlsTemplates,
    #[error("durations, timeouts, rates, and reconnect bounds must be finite and positive")]
    InvalidBounds,
    #[error("stress, spike, soak, and disruptive scenarios are local or temporary only")]
    EnvironmentProfileNotAllowed,
    #[error(
        "invalid event scenarios require explicit opt-in outside local or temporary environments"
    )]
    InvalidEventsNotAllowed,
    #[error("invalid event scenarios require max_events and are limited to one event per second")]
    InvalidEventLimit,
    #[error(
        "slow-client, slow-connect, and abrupt-disconnect scenarios are not permitted in production"
    )]
    SlowClientNotAllowed,
}

/// # Errors
///
/// Returns an error when authorization, host allowlisting, or environment limits fail.
pub fn validate(config: &AppConfig, allow_production: bool) -> Result<(), SafetyError> {
    validate_with_options(
        config,
        SafetyOptions {
            allow_production,
            allow_invalid_events: false,
        },
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SafetyOptions {
    pub allow_production: bool,
    pub allow_invalid_events: bool,
}

/// # Errors
///
/// Returns an error when authorization, host allowlisting, or environment limits fail.
pub fn validate_with_options(
    config: &AppConfig,
    options: SafetyOptions,
) -> Result<(), SafetyError> {
    if !config.authorization.acknowledged {
        return Err(SafetyError::AuthorizationRequired);
    }
    let host = host_from_server(&config.target.server);
    let local_loopback = config.environment == Environment::Local && is_loopback(host);
    if !local_loopback && !config.allow_hosts.iter().any(|allowed| allowed == host) {
        return Err(SafetyError::HostNotAllowed(host.into()));
    }
    validate_workload(config)?;
    if config.environment == Environment::Production && !options.allow_production {
        return Err(SafetyError::ProductionNotAllowed);
    }
    if config.scenario.invalid.enabled {
        let environment_permits = matches!(
            config.environment,
            Environment::Local | Environment::Temporary
        ) || (config.environment == Environment::Staging
            && options.allow_invalid_events);
        if !environment_permits {
            return Err(SafetyError::InvalidEventsNotAllowed);
        }
    }
    Ok(())
}

/// Validates workload invariants which must hold even for direct library callers.
///
/// # Errors
///
/// Returns an error for unsupported behavior, invalid bounds, or a workload that exceeds its
/// declared participant and environment limits.
pub fn validate_workload(config: &AppConfig) -> Result<(), SafetyError> {
    let participant_count = if config.participants.is_empty() {
        config.run.clients
    } else {
        let count =
            u32::try_from(config.participants.len()).map_err(|_| SafetyError::ClientLimit)?;
        if count != config.run.clients {
            return Err(SafetyError::ParticipantCountMismatch);
        }
        count
    };
    if participant_count == 0 {
        return Err(SafetyError::NoParticipants);
    }
    if participant_count > config.run.max_clients {
        return Err(SafetyError::ClientLimit);
    }
    validate_participants_and_routing(config)?;

    let timeouts = &config.timeouts;
    if config.run.duration.is_zero()
        || config.run.gps_interval.is_zero()
        || timeouts.connect.is_zero()
        || timeouts.tls_handshake.is_zero()
        || timeouts.read.is_zero()
        || timeouts.write.is_zero()
        || config.run.max_rate.is_some_and(|rate| {
            let interval = 1.0 / rate;
            !rate.is_finite()
                || rate <= 0.0
                || !interval.is_finite()
                || interval > Duration::MAX.as_secs_f64()
        })
        || (config.reconnect.enabled
            && (config.reconnect.min_backoff.is_zero()
                || config.reconnect.max_backoff.is_zero()
                || config.reconnect.min_backoff > config.reconnect.max_backoff))
    {
        return Err(SafetyError::InvalidBounds);
    }
    if config.scenario.kind != ScenarioKind::Position || config.scenario.movement != Movement::Fixed
    {
        return Err(SafetyError::UnsupportedWorkload);
    }
    if !config.scheduler.ramp_down.is_zero() {
        return Err(SafetyError::UnsupportedOption);
    }
    validate_tls_templates(config)?;

    let disruptive = config.scenario.slow_connect.enabled
        || config.scenario.abrupt_disconnect.enabled
        || config.participants.iter().any(|participant| {
            participant.read_delay.is_some() || participant.pause_read_for.is_some()
        });
    let local_or_temporary = matches!(
        config.environment,
        Environment::Local | Environment::Temporary
    );
    if (!local_or_temporary
        && matches!(
            config.run.profile,
            Profile::Stress | Profile::Spike | Profile::Soak
        ))
        || (config.environment == Environment::Staging && disruptive)
    {
        return Err(SafetyError::EnvironmentProfileNotAllowed);
    }

    if config.environment == Environment::Production {
        if config.run.profile != Profile::Smoke {
            return Err(SafetyError::ProductionProfile(config.run.profile));
        }
        if participant_count > 3 || config.run.max_clients > 3 {
            return Err(SafetyError::ProductionClients);
        }
        if config.run.duration > Duration::from_secs(15 * 60) {
            return Err(SafetyError::ProductionDuration);
        }
        if config.run.gps_interval < Duration::from_secs(30)
            || config.run.max_rate.is_some_and(|rate| rate > 0.1)
        {
            return Err(SafetyError::ProductionRate);
        }
        if disruptive {
            return Err(SafetyError::SlowClientNotAllowed);
        }
    }
    if config.scenario.invalid.enabled
        && (config
            .scenario
            .invalid
            .max_events
            .is_none_or(|events| events == 0)
            || config.run.max_rate.is_some_and(|rate| rate > 1.0)
            || config.run.gps_interval < Duration::from_secs(1))
    {
        return Err(SafetyError::InvalidEventLimit);
    }
    Ok(())
}

fn validate_tls_templates(config: &AppConfig) -> Result<(), SafetyError> {
    if config.participants.is_empty() {
        if (0..config.run.clients)
            .map(|index| format!("client-{index}"))
            .any(|id| config.tls.for_participant(&id).is_err())
        {
            return Err(SafetyError::InvalidTlsTemplates);
        }
    } else if config
        .participants
        .iter()
        .any(|participant| config.tls.for_participant(&participant.id).is_err())
    {
        return Err(SafetyError::InvalidTlsTemplates);
    }
    Ok(())
}

fn validate_participants_and_routing(config: &AppConfig) -> Result<(), SafetyError> {
    if config.participants.is_empty() {
        return if config.scenario.routing.is_empty() {
            Ok(())
        } else {
            Err(SafetyError::InvalidRoutingAssertion)
        };
    }
    let mut ids = HashSet::with_capacity(config.participants.len());
    if config
        .participants
        .iter()
        .any(|participant| participant.id.is_empty() || !ids.insert(participant.id.as_str()))
    {
        return Err(SafetyError::InvalidParticipantId);
    }
    for assertion in &config.scenario.routing {
        let sender = config
            .participants
            .iter()
            .find(|participant| participant.id == assertion.sender);
        if sender.is_none_or(|participant| participant.role == ParticipantRole::ReceiveOnly)
            || assertion.receivers.is_empty() && assertion.forbidden_receivers.is_empty()
            || assertion
                .receivers
                .iter()
                .chain(&assertion.forbidden_receivers)
                .any(|id| {
                    config
                        .participants
                        .iter()
                        .find(|participant| &participant.id == id)
                        .is_none_or(|participant| participant.role == ParticipantRole::SendOnly)
                })
        {
            return Err(SafetyError::InvalidRoutingAssertion);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_needs_explicit_opt_in() {
        let config = AppConfig {
            environment: Environment::Production,
            authorization: crate::config::AuthorizationConfig { acknowledged: true },
            allow_hosts: vec!["example.test".into()],
            target: crate::config::TargetConfig {
                server: "example.test:8089".into(),
                sni: None,
            },
            ..AppConfig::default()
        };
        assert_eq!(
            validate(&config, false),
            Err(SafetyError::ProductionNotAllowed)
        );
    }

    #[test]
    fn invalid_events_need_a_bounded_rate() {
        let config = AppConfig {
            authorization: crate::config::AuthorizationConfig { acknowledged: true },
            scenario: crate::config::ScenarioConfig {
                invalid: crate::config::InvalidScenarioConfig {
                    enabled: true,
                    max_events: Some(1),
                    ..crate::config::InvalidScenarioConfig::default()
                },
                ..crate::config::ScenarioConfig::default()
            },
            run: crate::config::RunConfig {
                gps_interval: Duration::from_millis(500),
                ..crate::config::RunConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            validate(&config, false),
            Err(SafetyError::InvalidEventLimit)
        );
    }

    #[test]
    fn invalid_event_kinds_are_blocked_in_production_and_cannot_bypass_rate_limit() {
        use crate::config::{InvalidEventKind, InvalidScenarioConfig, ScenarioConfig};

        let kinds = [
            InvalidEventKind::MalformedXml,
            InvalidEventKind::UnterminatedXml,
            InvalidEventKind::OversizedFrame,
            InvalidEventKind::InvalidCoordinates,
            InvalidEventKind::InvalidTime,
        ];
        for kind in kinds {
            let scenario = ScenarioConfig {
                invalid: InvalidScenarioConfig {
                    enabled: true,
                    kind: Some(kind),
                    max_events: Some(2),
                },
                ..ScenarioConfig::default()
            };
            let production = AppConfig {
                authorization: crate::config::AuthorizationConfig { acknowledged: true },
                environment: Environment::Production,
                allow_hosts: vec!["example.test".into()],
                target: crate::config::TargetConfig {
                    server: "example.test:8089".into(),
                    sni: None,
                },
                scenario: scenario.clone(),
                ..AppConfig::default()
            };
            assert_eq!(
                validate_with_options(
                    &production,
                    SafetyOptions {
                        allow_production: true,
                        allow_invalid_events: true,
                    },
                ),
                Err(SafetyError::InvalidEventsNotAllowed)
            );

            let rate_bypass = AppConfig {
                authorization: crate::config::AuthorizationConfig { acknowledged: true },
                scenario,
                run: crate::config::RunConfig {
                    gps_interval: Duration::from_millis(999),
                    max_rate: Some(1.0),
                    ..crate::config::RunConfig::default()
                },
                ..AppConfig::default()
            };
            assert_eq!(
                validate(&rate_bypass, false),
                Err(SafetyError::InvalidEventLimit)
            );
        }
    }

    #[test]
    fn abrupt_disconnect_is_blocked_in_production() {
        let config = AppConfig {
            environment: Environment::Production,
            authorization: crate::config::AuthorizationConfig { acknowledged: true },
            allow_hosts: vec!["example.test".into()],
            target: crate::config::TargetConfig {
                server: "example.test:8089".into(),
                sni: None,
            },
            scenario: crate::config::ScenarioConfig {
                abrupt_disconnect: crate::config::AbruptDisconnectConfig {
                    enabled: true,
                    after_events: 1,
                },
                ..crate::config::ScenarioConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            validate_with_options(
                &config,
                SafetyOptions {
                    allow_production: true,
                    allow_invalid_events: false
                }
            ),
            Err(SafetyError::SlowClientNotAllowed)
        );
    }

    #[test]
    fn explicit_participants_cannot_bypass_declared_limits() {
        let participants = (0..4)
            .map(|index| crate::config::ParticipantConfig {
                id: format!("participant-{index}"),
                ..crate::config::ParticipantConfig::default()
            })
            .collect();
        let mismatched = AppConfig {
            participants,
            run: crate::config::RunConfig {
                clients: 1,
                max_clients: 3,
                ..crate::config::RunConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            validate_workload(&mismatched),
            Err(SafetyError::ParticipantCountMismatch)
        );

        let over_limit = AppConfig {
            run: crate::config::RunConfig {
                clients: 4,
                max_clients: 3,
                ..crate::config::RunConfig::default()
            },
            ..mismatched
        };
        assert_eq!(
            validate_workload(&over_limit),
            Err(SafetyError::ClientLimit)
        );
    }

    #[test]
    fn participant_ids_routing_and_workload_options_are_validated() {
        let duplicate = AppConfig {
            run: crate::config::RunConfig {
                clients: 2,
                ..crate::config::RunConfig::default()
            },
            participants: vec![
                crate::config::ParticipantConfig {
                    id: "duplicate".into(),
                    ..crate::config::ParticipantConfig::default()
                },
                crate::config::ParticipantConfig {
                    id: "duplicate".into(),
                    ..crate::config::ParticipantConfig::default()
                },
            ],
            ..AppConfig::default()
        };
        assert_eq!(
            validate_workload(&duplicate),
            Err(SafetyError::InvalidParticipantId)
        );

        let unsupported = AppConfig {
            scenario: crate::config::ScenarioConfig {
                kind: ScenarioKind::Chat,
                ..crate::config::ScenarioConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            validate_workload(&unsupported),
            Err(SafetyError::UnsupportedWorkload)
        );
    }
}
