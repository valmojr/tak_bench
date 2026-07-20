//! Built-in, server-neutral workload scenarios.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use anyhow::{Result, bail};
use rand::Rng;
use tak_bench_core::{
    config::{AppConfig, InvalidEventKind, ParticipantConfig, ParticipantRole, RoutingAssertion},
    connection::{self, ConnectionReader, ConnectionWriter},
    metrics::Metrics,
    scheduler::start_delays,
};
use tak_bench_protocol::{CotStreamDecoder, PositionEvent, inspect_event};
use time::OffsetDateTime;
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, watch},
    time::{MissedTickBehavior, interval},
};
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AssertionResult {
    pub sender: String,
    pub passed: bool,
    pub expected_receivers: u64,
    pub forbidden_receivers: u64,
}
#[derive(Debug, Default, serde::Serialize)]
pub struct ScenarioOutcome {
    pub assertions: Vec<AssertionResult>,
}

#[derive(Default)]
struct CorrelationLedger {
    sent: HashMap<Uuid, (String, Instant)>,
    seen: HashMap<(String, Uuid), Instant>,
}

/// Runs fixed `CoT` positions. Routing checks only observe the stream; they never configure a server.
///
/// # Errors
///
/// Returns an error for a failed connection, timeout, invalid schedule, or exhausted reconnect budget.
pub async fn run_fixed_positions(
    config: AppConfig,
    metrics: Arc<Metrics>,
    stop: watch::Receiver<bool>,
) -> Result<ScenarioOutcome> {
    let deadline = tokio::time::Instant::now() + config.run.duration;
    let participants = participants(&config);
    let delays = start_delays(
        u32::try_from(participants.len()).map_err(|_| anyhow::anyhow!("too many participants"))?,
        &config.scheduler,
    )?;
    let ledger = Arc::new(Mutex::new(CorrelationLedger::default()));
    let mut tasks = Vec::new();
    for (participant, delay) in participants.into_iter().zip(delays) {
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let ledger = Arc::clone(&ledger);
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            run_participant(participant, config, metrics, ledger, stop, deadline).await
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(ScenarioOutcome {
        assertions: evaluate_routing(&config.scenario.routing, &*ledger.lock().await),
    })
}

fn participants(config: &AppConfig) -> Vec<ParticipantConfig> {
    if !config.participants.is_empty() {
        return config.participants.clone();
    }
    (0..config.run.clients)
        .map(|index| ParticipantConfig {
            id: format!("client-{index}"),
            ..ParticipantConfig::default()
        })
        .collect()
}

async fn run_participant(
    participant: ParticipantConfig,
    config: AppConfig,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
    stop: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut attempt = 0_u32;
    let recovery_started = Instant::now();
    loop {
        metrics.connection_attempts.fetch_add(1, Ordering::Relaxed);
        match connection::connect(&config.target, &config.tls, &config.timeouts).await {
            Ok(connection) => {
                metrics.connection_successes.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                metrics.record_handshake(connection.handshake_time).await;
                if attempt > 0 {
                    metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                    metrics.record_recovery(recovery_started.elapsed()).await;
                }
                let result = run_connected(
                    &participant,
                    &config,
                    Arc::clone(&metrics),
                    Arc::clone(&ledger),
                    stop.clone(),
                    deadline,
                    connection,
                )
                .await;
                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                if result.is_ok()
                    || !config.reconnect.enabled
                    || tokio::time::Instant::now() >= deadline
                {
                    return result;
                }
            }
            Err(error) => {
                metrics.connection_failures.fetch_add(1, Ordering::Relaxed);
                if config.tls.enabled {
                    metrics.tls_failures.fetch_add(1, Ordering::Relaxed);
                }
                if !config.reconnect.enabled {
                    return Err(error);
                }
                metrics.reconnect_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        if attempt >= config.reconnect.max_attempts {
            bail!(
                "participant {} exhausted reconnect attempts",
                participant.id
            );
        }
        attempt += 1;
        tokio::time::sleep(reconnect_delay(&config, attempt)).await;
    }
}

fn reconnect_delay(config: &AppConfig, attempt: u32) -> std::time::Duration {
    let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
    let base = config
        .reconnect
        .min_backoff
        .saturating_mul(multiplier)
        .min(config.reconnect.max_backoff);
    let jitter = f64::from(config.reconnect.jitter_percent.min(100)) / 100.0;
    if jitter == 0.0 {
        return base;
    }
    let factor = rand::rng().random_range((1.0 - jitter)..=(1.0 + jitter));
    base.mul_f64(factor)
}

async fn run_connected(
    participant: &ParticipantConfig,
    config: &AppConfig,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
    mut stop: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
    connection: connection::ClientConnection,
) -> Result<()> {
    let (reader, mut writer) = connection.into_split();
    if config.scenario.slow_connect.enabled {
        tokio::time::sleep(config.scenario.slow_connect.initial_write_delay).await;
    }
    if participant.role == ParticipantRole::ReceiveOnly {
        return read_events(
            participant,
            reader,
            &metrics,
            &ledger,
            &mut stop,
            deadline,
            config.timeouts.read,
        )
        .await;
    }
    if participant.role == ParticipantRole::SendOnly {
        return send_positions(
            participant,
            config,
            &metrics,
            &ledger,
            &mut writer,
            &mut stop,
            deadline,
        )
        .await;
    }
    let reader_participant = participant.clone();
    let mut reader_stop = stop.clone();
    let read_timeout = config.timeouts.read;
    let reader_metrics = Arc::clone(&metrics);
    let reader_ledger = Arc::clone(&ledger);
    let mut reader_task = tokio::spawn(async move {
        read_events(
            &reader_participant,
            reader,
            &reader_metrics,
            &reader_ledger,
            &mut reader_stop,
            deadline,
            read_timeout,
        )
        .await
    });
    let mut sender = std::pin::pin!(send_positions(
        participant,
        config,
        &metrics,
        &ledger,
        &mut writer,
        &mut stop,
        deadline,
    ));
    tokio::select! {
        sent = &mut sender => { reader_task.abort(); let _ = reader_task.await; sent }
        read = &mut reader_task => {
            read.map_err(|error| anyhow::anyhow!("reader task failed: {error}"))?
        }
    }
}

async fn read_events(
    participant: &ParticipantConfig,
    mut reader: ConnectionReader,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
    read_timeout: std::time::Duration,
) -> Result<()> {
    let mut decoder = CotStreamDecoder::new(8 * 1024 * 1024);
    let mut buffer = [0_u8; 8192];
    if let Some(pause) = participant.pause_read_for {
        tokio::time::sleep(pause).await;
    }
    loop {
        tokio::select! {
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { return Ok(()); },
            () = tokio::time::sleep_until(deadline) => return Ok(()),
            result = tokio::time::timeout(read_timeout, reader.read(&mut buffer)) => {
                let count = result.map_err(|_| anyhow::anyhow!("read timed out"))??;
                if count == 0 { bail!("peer closed the connection before the run deadline"); }
                if let Some(delay) = participant.read_delay { tokio::time::sleep(delay).await; }
                for raw in decoder.push(&buffer[..count])? {
                    let event = inspect_event(raw)?; metrics.received_messages.fetch_add(1, Ordering::Relaxed);
                    if let Some(correlation) = event.correlation_id { let mut state = ledger.lock().await;
                        if state.seen.insert((participant.id.clone(), correlation), Instant::now()).is_some() { metrics.duplicate_messages.fetch_add(1, Ordering::Relaxed); }
                        else if let Some((_, sent)) = state.sent.get(&correlation) { metrics.record_delivery(sent.elapsed()).await; }
                    }
                }
            }
        }
    }
}

async fn send_positions(
    participant: &ParticipantConfig,
    config: &AppConfig,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    writer: &mut ConnectionWriter,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut ticker = interval(config.run.gps_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut batch = Vec::new();
    let batch_size =
        u32::try_from(config.scenario.fragmentation.events_per_write.max(1)).unwrap_or(u32::MAX);
    let mut event_count = 0;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
            let xml = invalid_or_position(participant, config, event_count);
                let correlation = Uuid::new_v4();
                let xml = xml.unwrap_or_else(|| position_xml(participant, config, correlation));
                ledger.lock().await.sent.insert(correlation, (participant.id.clone(), Instant::now()));
                batch.extend_from_slice(xml.as_bytes()); event_count += 1;
                if batch.len() > 8 * 1024 * 1024 { bail!("outgoing batch exceeds frame safety limit"); }
                if event_count % batch_size == 0 { write_fragmented(writer, &batch, &config.scenario.fragmentation.chunk_sizes, config.timeouts.write).await?; metrics.sent_messages.fetch_add(u64::from(batch_size), Ordering::Relaxed); batch.clear(); }
                if config.scenario.abrupt_disconnect.enabled && event_count >= config.scenario.abrupt_disconnect.after_events.max(1) { return Ok(()); }
            }
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { break; },
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    if !batch.is_empty() {
        let events = u64::from(event_count % batch_size);
        write_fragmented(
            writer,
            &batch,
            &config.scenario.fragmentation.chunk_sizes,
            config.timeouts.write,
        )
        .await?;
        metrics.sent_messages.fetch_add(events, Ordering::Relaxed);
    }
    Ok(())
}

fn position_xml(
    participant: &ParticipantConfig,
    config: &AppConfig,
    correlation_id: Uuid,
) -> String {
    let now = OffsetDateTime::now_utc();
    PositionEvent {
        uid: format!("tak-bench-{}", participant.id),
        callsign: participant.id.clone(),
        latitude: -23.5505,
        longitude: -46.6333,
        altitude_m: 760.0,
        course_deg: 0.0,
        speed_mps: 0.0,
        time: now,
        stale: now
            + time::Duration::seconds(
                i64::try_from(config.run.gps_interval.as_secs()).unwrap_or(30) * 2,
            ),
        correlation_id,
    }
    .to_xml()
}
fn invalid_or_position(
    _participant: &ParticipantConfig,
    config: &AppConfig,
    count: u32,
) -> Option<String> {
    if !config.scenario.invalid.enabled || count >= config.scenario.invalid.max_events.unwrap_or(0)
    {
        return None;
    }
    Some(
        match config
            .scenario
            .invalid
            .kind
            .unwrap_or(InvalidEventKind::MalformedXml)
        {
            InvalidEventKind::MalformedXml => "<event".into(),
            InvalidEventKind::UnterminatedXml => "<event uid=\"invalid\">".into(),
            InvalidEventKind::OversizedFrame => {
                format!("<event>{}</event>", "x".repeat(8 * 1024 * 1024 + 1))
            }
            InvalidEventKind::InvalidCoordinates => {
                "<event><point lat=\"nan\" lon=\"999\"/></event>".into()
            }
            InvalidEventKind::InvalidTime => "<event time=\"not-a-time\"></event>".into(),
        },
    )
}

async fn write_fragmented(
    writer: &mut ConnectionWriter,
    bytes: &[u8],
    chunks: &[usize],
    timeout: std::time::Duration,
) -> Result<()> {
    if chunks.is_empty() {
        return connection::write_all(writer, bytes, timeout).await;
    }
    let mut offset = 0;
    for chunk in chunks.iter().copied().cycle() {
        if offset == bytes.len() {
            return Ok(());
        }
        let end = offset.saturating_add(chunk.max(1)).min(bytes.len());
        connection::write_all(writer, &bytes[offset..end], timeout).await?;
        offset = end;
    }
    Ok(())
}

fn evaluate_routing(
    assertions: &[RoutingAssertion],
    state: &CorrelationLedger,
) -> Vec<AssertionResult> {
    assertions
        .iter()
        .map(|assertion| {
            let correlations: Vec<_> = state
                .sent
                .iter()
                .filter(|(_, (sender, _))| sender == &assertion.sender)
                .map(|(id, _)| *id)
                .collect();
            let timeout = assertion
                .timeout
                .unwrap_or(std::time::Duration::from_secs(30));
            let expected = correlations
                .iter()
                .flat_map(|id| {
                    assertion
                        .receivers
                        .iter()
                        .map(move |receiver| (receiver, id))
                })
                .filter(|(receiver, id)| {
                    state
                        .seen
                        .get(&((*receiver).clone(), **id))
                        .is_some_and(|seen| {
                            state
                                .sent
                                .get(*id)
                                .is_some_and(|(_, sent)| seen.duration_since(*sent) <= timeout)
                        })
                })
                .count() as u64;
            let forbidden = correlations
                .iter()
                .flat_map(|id| {
                    assertion
                        .forbidden_receivers
                        .iter()
                        .map(move |receiver| (receiver, id))
                })
                .filter(|(receiver, id)| state.seen.contains_key(&((*receiver).clone(), **id)))
                .count() as u64;
            AssertionResult {
                sender: assertion.sender.clone(),
                passed: expected == (correlations.len() * assertion.receivers.len()) as u64
                    && forbidden == 0,
                expected_receivers: expected,
                forbidden_receivers: forbidden,
            }
        })
        .collect()
}

#[must_use]
pub fn minimum_stale_interval(interval: std::time::Duration) -> std::time::Duration {
    interval.saturating_mul(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tak_bench_core::config::{ReconnectConfig, RoutingAssertion};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn loopback_config(address: std::net::SocketAddr) -> AppConfig {
        AppConfig {
            authorization: tak_bench_core::config::AuthorizationConfig { acknowledged: true },
            target: tak_bench_core::config::TargetConfig {
                server: address.to_string(),
                sni: None,
            },
            run: tak_bench_core::config::RunConfig {
                clients: 1,
                max_clients: 3,
                duration: std::time::Duration::from_millis(500),
                gps_interval: std::time::Duration::from_millis(30),
                ..tak_bench_core::config::RunConfig::default()
            },
            timeouts: tak_bench_core::config::TimeoutConfig {
                connect: std::time::Duration::from_millis(50),
                tls_handshake: std::time::Duration::from_millis(50),
                read: std::time::Duration::from_millis(300),
                write: std::time::Duration::from_millis(50),
            },
            ..AppConfig::default()
        }
    }

    async fn relay(listener: TcpListener, accepted: Arc<AtomicUsize>) {
        let (tx, _) = tokio::sync::broadcast::channel::<(usize, Vec<u8>)>(32);
        let mut next = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let id = next;
            next += 1;
            accepted.fetch_add(1, Ordering::Relaxed);
            let (mut read, mut write) = tokio::io::split(stream);
            let sender = tx.clone();
            let mut receiver = tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match receiver.recv().await {
                        Ok((source, bytes)) if source != id => {
                            if write.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                loop {
                    match read.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            let _ = sender.send((id, buffer[..count].to_vec()));
                        }
                    }
                }
            });
        }
    }

    #[tokio::test]
    async fn tcp_roles_routing_and_fragmented_batches_work_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(relay(listener, Arc::clone(&accepted)));
        let mut config = loopback_config(address);
        config.participants = vec![
            ParticipantConfig {
                id: "sender".into(),
                role: ParticipantRole::SendOnly,
                ..ParticipantConfig::default()
            },
            ParticipantConfig {
                id: "receiver".into(),
                role: ParticipantRole::ReceiveOnly,
                ..ParticipantConfig::default()
            },
        ];
        config.scenario.fragmentation.chunk_sizes = vec![3, 7, 11];
        config.scenario.fragmentation.events_per_write = 2;
        config.scenario.routing = vec![RoutingAssertion {
            sender: "sender".into(),
            receivers: vec!["receiver".into()],
            forbidden_receivers: vec![],
            timeout: Some(std::time::Duration::from_secs(1)),
        }];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        let outcome = run_fixed_positions(config, Arc::clone(&metrics), stop)
            .await
            .unwrap();
        assert!(outcome.assertions[0].passed);
        assert!(metrics.received_messages.load(Ordering::Relaxed) > 0);
        assert_eq!(accepted.load(Ordering::Relaxed), 2);
        server.abort();
    }

    #[tokio::test]
    async fn eof_triggers_bounded_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let (_second, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        let mut config = loopback_config(address);
        config.participants = vec![ParticipantConfig {
            id: "receiver".into(),
            role: ParticipantRole::ReceiveOnly,
            ..ParticipantConfig::default()
        }];
        config.reconnect = ReconnectConfig {
            enabled: true,
            min_backoff: std::time::Duration::from_millis(1),
            max_backoff: std::time::Duration::from_millis(2),
            max_attempts: 2,
            jitter_percent: 0,
        };
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        let _ = run_fixed_positions(config, Arc::clone(&metrics), stop).await;
        assert!(metrics.reconnects.load(Ordering::Relaxed) >= 1);
        server.abort();
    }

    #[tokio::test]
    async fn read_timeout_is_reported_against_a_real_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        let mut config = loopback_config(address);
        config.timeouts.read = std::time::Duration::from_millis(40);
        config.participants = vec![ParticipantConfig {
            id: "receiver".into(),
            role: ParticipantRole::ReceiveOnly,
            ..ParticipantConfig::default()
        }];
        let (_stop_tx, stop) = watch::channel(false);
        assert!(
            run_fixed_positions(config, Arc::new(Metrics::new()), stop)
                .await
                .is_err()
        );
        server.abort();
    }

    #[test]
    fn reconnect_backoff_is_bounded() {
        let config = AppConfig {
            reconnect: ReconnectConfig {
                min_backoff: std::time::Duration::from_secs(1),
                max_backoff: std::time::Duration::from_secs(3),
                jitter_percent: 0,
                ..ReconnectConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            reconnect_delay(&config, 1),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            reconnect_delay(&config, 3),
            std::time::Duration::from_secs(3)
        );
    }

    #[test]
    fn routing_reports_expected_and_forbidden_receivers_without_payloads() {
        let id = Uuid::nil();
        let mut state = CorrelationLedger::default();
        state.sent.insert(id, ("sender".into(), Instant::now()));
        state.seen.insert(("receiver".into(), id), Instant::now());
        let result = evaluate_routing(
            &[RoutingAssertion {
                sender: "sender".into(),
                receivers: vec!["receiver".into()],
                forbidden_receivers: vec!["forbidden".into()],
                timeout: None,
            }],
            &state,
        );
        assert!(result[0].passed);
        state.seen.insert(("forbidden".into(), id), Instant::now());
        assert!(
            !evaluate_routing(
                &[RoutingAssertion {
                    sender: "sender".into(),
                    receivers: vec!["receiver".into()],
                    forbidden_receivers: vec!["forbidden".into()],
                    timeout: None
                }],
                &state
            )[0]
            .passed
        );
    }

    #[test]
    fn default_participants_follow_client_count() {
        let config = AppConfig {
            run: tak_bench_core::config::RunConfig {
                clients: 2,
                ..tak_bench_core::config::RunConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(participants(&config).len(), 2);
    }
}
