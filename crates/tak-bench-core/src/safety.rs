use std::time::Duration;

use crate::config::{AppConfig, Environment, Profile, host_from_server, is_loopback};
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
    #[error(
        "invalid event scenarios require explicit opt-in outside local or temporary environments"
    )]
    InvalidEventsNotAllowed,
    #[error("invalid event scenarios require max_events and are limited to one event per second")]
    InvalidEventLimit,
    #[error("slow-client and slow-connect scenarios are not permitted in production")]
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
    if config.run.clients > config.run.max_clients {
        return Err(SafetyError::ClientLimit);
    }
    if config.environment == Environment::Production {
        if !options.allow_production {
            return Err(SafetyError::ProductionNotAllowed);
        }
        if config.run.profile != Profile::Smoke {
            return Err(SafetyError::ProductionProfile(config.run.profile));
        }
        if config.run.clients > 3 || config.run.max_clients > 3 {
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
        if config.scenario.invalid.max_events.is_none()
            || config.run.max_rate.is_some_and(|rate| rate > 1.0)
            || config.run.max_rate.is_none() && config.run.gps_interval < Duration::from_secs(1)
        {
            return Err(SafetyError::InvalidEventLimit);
        }
    }
    if config.environment == Environment::Production
        && (config.scenario.slow_connect.enabled
            || config.participants.iter().any(|participant| {
                participant.read_delay.is_some() || participant.pause_read_for.is_some()
            }))
    {
        return Err(SafetyError::SlowClientNotAllowed);
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
}
