use std::{net::IpAddr, path::PathBuf, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    #[default]
    Local,
    Staging,
    Temporary,
    Production,
}

impl FromStr for Environment {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::Local),
            "staging" => Ok(Self::Staging),
            "temporary" => Ok(Self::Temporary),
            "production" => Ok(Self::Production),
            _ => Err("environment must be local, staging, temporary, or production".into()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    #[default]
    Smoke,
    Functional,
    Load,
    Stress,
    Spike,
    Soak,
    Reconnect,
}

impl FromStr for Profile {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "functional" => Ok(Self::Functional),
            "load" => Ok(Self::Load),
            "stress" => Ok(Self::Stress),
            "spike" => Ok(Self::Spike),
            "soak" => Ok(Self::Soak),
            "reconnect" => Ok(Self::Reconnect),
            _ => Err("unknown profile".into()),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub version: u32,
    pub authorization: AuthorizationConfig,
    pub environment: Environment,
    pub allow_hosts: Vec<String>,
    pub target: TargetConfig,
    pub tls: TlsConfig,
    pub run: RunConfig,
    pub scheduler: SchedulerConfig,
    pub reconnect: ReconnectConfig,
    pub abort: AbortConfig,
    pub participants: Vec<ParticipantConfig>,
    pub scenario: ScenarioConfig,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthorizationConfig {
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TargetConfig {
    pub server: String,
    pub sni: Option<String>,
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            server: "127.0.0.1:8089".into(),
            sni: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub client_cert_template: Option<String>,
    pub client_key_template: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunConfig {
    pub profile: Profile,
    pub clients: u32,
    pub max_clients: u32,
    #[serde(with = "duration_serde")]
    pub duration: Duration,
    #[serde(with = "duration_serde")]
    pub gps_interval: Duration,
    pub max_rate: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RampStrategy {
    #[default]
    Immediate,
    Linear,
    Step,
    Randomized,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulerConfig {
    pub strategy: RampStrategy,
    #[serde(with = "duration_serde")]
    pub ramp_up: Duration,
    #[serde(with = "duration_serde")]
    pub ramp_down: Duration,
    pub steps: Vec<RampStep>,
    pub seed: Option<u64>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            strategy: RampStrategy::Immediate,
            ramp_up: Duration::ZERO,
            ramp_down: Duration::ZERO,
            steps: Vec::new(),
            seed: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RampStep {
    #[serde(with = "duration_serde")]
    pub at: Duration,
    pub clients: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconnectConfig {
    pub enabled: bool,
    #[serde(with = "duration_serde")]
    pub min_backoff: Duration,
    #[serde(with = "duration_serde")]
    pub max_backoff: Duration,
    pub max_attempts: u32,
    pub jitter_percent: u8,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
            max_attempts: 5,
            jitter_percent: 20,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AbortConfig {
    pub connection_error_rate: Option<f64>,
    pub message_error_rate: Option<f64>,
    #[serde(with = "optional_duration_serde")]
    pub p95_latency: Option<Duration>,
    #[serde(with = "optional_duration_serde")]
    pub p99_latency: Option<Duration>,
    pub max_dropped_messages: Option<u64>,
    pub min_samples: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ParticipantConfig {
    pub id: String,
    pub role: ParticipantRole,
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantRole {
    SendOnly,
    ReceiveOnly,
    #[default]
    SendReceive,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            profile: Profile::Smoke,
            clients: 1,
            max_clients: 3,
            duration: Duration::from_secs(120),
            gps_interval: Duration::from_secs(30),
            max_rate: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScenarioConfig {
    pub kind: ScenarioKind,
    pub movement: Movement,
    pub routing: Vec<RoutingAssertion>,
    pub fragmentation: FragmentationConfig,
    pub invalid: InvalidScenarioConfig,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioKind {
    #[default]
    Position,
    Marker,
    Chat,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Movement {
    #[default]
    Fixed,
    RandomWalk,
    Line,
    Circle,
    Route,
    GeojsonRoute,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingAssertion {
    pub sender: String,
    pub receivers: Vec<String>,
    pub forbidden_receivers: Vec<String>,
    #[serde(with = "optional_duration_serde")]
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FragmentationConfig {
    pub chunk_sizes: Vec<usize>,
    pub events_per_write: usize,
}

impl Default for FragmentationConfig {
    fn default() -> Self {
        Self {
            chunk_sizes: Vec::new(),
            events_per_write: 1,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct InvalidScenarioConfig {
    pub enabled: bool,
    pub kind: Option<String>,
    pub max_events: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutputConfig {
    pub json: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not parse duration {0:?}; use 30s, 5m, or 1h")]
    Duration(String),
}

#[allow(clippy::missing_errors_doc)]
pub mod duration_serde {
    use super::{ConfigError, Duration};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}s", value.as_secs()))
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        parse_duration(&raw).map_err(serde::de::Error::custom)
    }
    pub fn parse_duration(raw: &str) -> Result<Duration, ConfigError> {
        let (number, unit) = raw.trim().split_at(
            raw.trim()
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(raw.len()),
        );
        let value = number
            .parse::<u64>()
            .map_err(|_| ConfigError::Duration(raw.into()))?;
        match unit {
            "s" => Ok(Duration::from_secs(value)),
            "m" => Ok(Duration::from_secs(value.saturating_mul(60))),
            "h" => Ok(Duration::from_secs(value.saturating_mul(3_600))),
            _ => Err(ConfigError::Duration(raw.into())),
        }
    }
}

#[allow(clippy::missing_errors_doc, clippy::ref_option)]
mod optional_duration_serde {
    use super::duration_serde;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(duration) => serializer.serialize_some(&format!("{}s", duration.as_secs())),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|raw| duration_serde::parse_duration(&raw).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[must_use]
pub fn host_from_server(server: &str) -> &str {
    server
        .rsplit_once(':')
        .map_or(server, |(host, _)| host.trim_matches(['[', ']']))
}

#[must_use]
pub fn is_loopback(host: &str) -> bool {
    host == "localhost" || IpAddr::from_str(host).is_ok_and(|address| address.is_loopback())
}
