use std::{path::PathBuf, str::FromStr, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tak_bench_core::{
    config::{AppConfig, Environment, Profile},
    connection,
    metrics::Metrics,
    safety::{self, AUTHORIZATION_BANNER},
    thresholds,
};
use tak_bench_report::{RunReport, RunStatus};

#[derive(Debug, Parser)]
#[command(
    name = "tak-bench",
    version,
    about = "Authorized TAK/CoT server test tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connect: bool,
        #[arg(long)]
        allow_production: bool,
        #[arg(long)]
        allow_invalid_events: bool,
    },
    Smoke(RunArgs),
    Run(RunArgs),
    Scenario {
        #[command(subcommand)]
        command: ScenarioCommand,
    },
    Certs {
        #[command(subcommand)]
        command: CertCommand,
    },
    Report {
        #[command(subcommand)]
        command: ReportCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ScenarioCommand {
    List,
}
#[derive(Debug, Subcommand)]
enum CertCommand {
    Inspect {
        #[arg(long)]
        ca: PathBuf,
    },
}
#[derive(Debug, Subcommand)]
enum ReportCommand {
    Render {
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    server: Option<String>,
    #[arg(long)]
    environment: Option<String>,
    #[arg(long)]
    profile: Option<String>,
    #[arg(long)]
    clients: Option<u32>,
    #[arg(long)]
    max_clients: Option<u32>,
    #[arg(long)]
    duration: Option<String>,
    #[arg(long)]
    gps_interval: Option<String>,
    #[arg(long)]
    max_rate: Option<f64>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    acknowledge_authorization: bool,
    #[arg(long)]
    allow_production: bool,
    #[arg(long)]
    allow_invalid_events: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    match Cli::parse().command {
        Command::Validate {
            config,
            connect,
            allow_production,
            allow_invalid_events,
        } => validate(config, connect, allow_production, allow_invalid_events).await,
        Command::Smoke(args) => run(args, Some(Profile::Smoke)).await,
        Command::Run(args) => run(args, None).await,
        Command::Scenario {
            command: ScenarioCommand::List,
        } => {
            println!("position (fixed)\nfuture: marker, chat, route, groups, invalid, slow-client");
            Ok(())
        }
        Command::Certs {
            command: CertCommand::Inspect { ca },
        } => {
            let metadata =
                std::fs::metadata(&ca).with_context(|| format!("reading {}", ca.display()))?;
            println!(
                "CA file {} is readable ({} bytes). Full certificate inspection is planned with test-PKI support.",
                ca.display(),
                metadata.len()
            );
            Ok(())
        }
        Command::Report {
            command: ReportCommand::Render { input },
        } => {
            println!(
                "{}",
                std::fs::read_to_string(&input)
                    .with_context(|| format!("reading {}", input.display()))?
            );
            Ok(())
        }
    }
}

async fn validate(
    path: PathBuf,
    connect: bool,
    allow_production: bool,
    allow_invalid_events: bool,
) -> Result<()> {
    let config = read_config(&path)?;
    let mut structural = config.clone();
    structural.authorization.acknowledged = true;
    safety::validate_with_options(
        &structural,
        safety::SafetyOptions {
            allow_production: true,
            allow_invalid_events: true,
        },
    )
    .context("configuration is unsafe or invalid")?;
    if connect {
        safety::validate_with_options(
            &config,
            safety::SafetyOptions {
                allow_production,
                allow_invalid_events,
            },
        )?;
        println!("{AUTHORIZATION_BANNER}");
        let _ = connection::connect(&config.target, &config.tls, &config.timeouts).await?;
        println!("Connection preflight succeeded.");
    } else {
        println!("Configuration is valid. Use --connect to perform a connection preflight.");
    }
    Ok(())
}

async fn run(args: RunArgs, forced_profile: Option<Profile>) -> Result<()> {
    let mut config = args
        .config
        .as_ref()
        .map_or_else(|| Ok(AppConfig::default()), read_config)?;
    apply_overrides(&mut config, &args, forced_profile)?;
    safety::validate_with_options(
        &config,
        safety::SafetyOptions {
            allow_production: args.allow_production,
            allow_invalid_events: args.allow_invalid_events,
        },
    )?;
    println!("{AUTHORIZATION_BANNER}");
    let metrics = Arc::new(Metrics::new());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let signal_tx = stop_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = signal_tx.send(true);
        }
    });
    let started_at = time::OffsetDateTime::now_utc();
    let started = Instant::now();
    let threshold_reason = Arc::new(tokio::sync::Mutex::new(None));
    let monitor_reason = Arc::clone(&threshold_reason);
    let monitor_metrics = Arc::clone(&metrics);
    let monitor_abort = config.abort.clone();
    let monitor_tx = stop_tx.clone();
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
    let outcome =
        tak_bench_scenarios::run_fixed_positions(config.clone(), Arc::clone(&metrics), stop_rx)
            .await;
    monitor.abort();
    let threshold = threshold_reason.lock().await.clone();
    let assertions = outcome
        .as_ref()
        .map_or_else(|_| Vec::new(), |value| value.assertions.clone());
    let assertions_failed = assertions.iter().any(|assertion| !assertion.passed);
    let stop_reason = if let Some(reason) = threshold.clone() {
        reason
    } else if outcome.is_ok() && !assertions_failed {
        "completed".to_owned()
    } else if assertions_failed {
        "routing_assertion_failed".to_owned()
    } else {
        "scenario_error".to_owned()
    };
    let status = if threshold.is_some() {
        RunStatus::Aborted
    } else if outcome.is_ok() && !assertions_failed {
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
    );
    println!("{}", report.terminal());
    if let Some(path) = &config.output.json {
        report.write_json(path)?;
    }
    outcome?;
    if assertions_failed {
        anyhow::bail!(
            "one or more routing assertions failed (participant IDs and counts are in the report)"
        );
    }
    Ok(())
}

fn read_config(path: &PathBuf) -> Result<AppConfig> {
    serde_yaml::from_str(
        &std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?,
    )
    .with_context(|| format!("parsing {}", path.display()))
}

fn apply_overrides(
    config: &mut AppConfig,
    args: &RunArgs,
    forced_profile: Option<Profile>,
) -> Result<()> {
    if let Some(server) = &args.server {
        config.target.server.clone_from(server);
    }
    if let Some(value) = &args.environment {
        config.environment = Environment::from_str(value).map_err(anyhow::Error::msg)?;
    }
    if let Some(value) = forced_profile {
        config.run.profile = value;
    } else if let Some(value) = &args.profile {
        config.run.profile = Profile::from_str(value).map_err(anyhow::Error::msg)?;
    }
    if let Some(value) = args.clients {
        config.run.clients = value;
    }
    if let Some(value) = args.max_clients {
        config.run.max_clients = value;
    }
    if let Some(value) = &args.duration {
        config.run.duration = parse_duration(value)?;
    }
    if let Some(value) = &args.gps_interval {
        config.run.gps_interval = parse_duration(value)?;
    }
    if let Some(value) = args.max_rate {
        config.run.max_rate = Some(value);
    }
    if let Some(path) = &args.output {
        config.output.json = Some(path.clone());
    }
    config.authorization.acknowledged |= args.acknowledge_authorization;
    Ok(())
}

fn parse_duration(raw: &str) -> Result<std::time::Duration> {
    let (number, unit) = raw.trim().split_at(
        raw.trim()
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(raw.len()),
    );
    let value: u64 = number.parse().context("duration needs a numeric value")?;
    match unit {
        "s" => Ok(std::time::Duration::from_secs(value)),
        "m" => Ok(std::time::Duration::from_secs(value * 60)),
        "h" => Ok(std::time::Duration::from_secs(value * 3_600)),
        _ => anyhow::bail!("duration must use s, m, or h"),
    }
}
