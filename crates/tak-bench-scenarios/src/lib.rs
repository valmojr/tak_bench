//! Built-in safe workload scenarios.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use anyhow::Result;
use tak_bench_core::{
    config::AppConfig,
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

#[derive(Default)]
struct CorrelationLedger {
    sent: HashMap<Uuid, Instant>,
    seen_by_client: HashSet<(u32, Uuid)>,
}

/// # Errors
///
/// Returns the first connection, send, or workload task error.
pub async fn run_fixed_positions(
    config: AppConfig,
    metrics: Arc<Metrics>,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + config.run.duration;
    let ledger = Arc::new(Mutex::new(CorrelationLedger::default()));
    let mut tasks = Vec::new();
    for (client_id, delay) in start_delays(config.run.clients, &config.scheduler)?
        .into_iter()
        .enumerate()
    {
        let client_id =
            u32::try_from(client_id).map_err(|_| anyhow::anyhow!("client count exceeds u32"))?;
        let client_config = config.clone();
        let client_metrics = Arc::clone(&metrics);
        let client_ledger = Arc::clone(&ledger);
        let client_stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            run_client(
                client_id,
                client_config,
                client_metrics,
                client_ledger,
                client_stop,
                deadline,
            )
            .await
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(())
}

async fn run_client(
    client_id: u32,
    config: AppConfig,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
    mut stop: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    metrics.connection_attempts.fetch_add(1, Ordering::Relaxed);
    let connection = match connection::connect(&config.target, &config.tls).await {
        Ok(connection) => connection,
        Err(error) => {
            metrics.connection_failures.fetch_add(1, Ordering::Relaxed);
            if config.tls.enabled {
                metrics.tls_failures.fetch_add(1, Ordering::Relaxed);
            }
            return Err(error);
        }
    };
    metrics.connection_successes.fetch_add(1, Ordering::Relaxed);
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    metrics.record_handshake(connection.handshake_time).await;
    let (reader, mut writer) = connection.into_split();
    let reader_metrics = Arc::clone(&metrics);
    let reader_ledger = Arc::clone(&ledger);
    let reader_task =
        tokio::spawn(
            async move { read_events(client_id, reader, reader_metrics, reader_ledger).await },
        );
    let result = send_positions(
        client_id,
        &config,
        &metrics,
        &ledger,
        &mut writer,
        &mut stop,
        deadline,
    )
    .await;
    reader_task.abort();
    let _ = reader_task.await;
    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
    result
}

async fn read_events(
    client_id: u32,
    mut reader: ConnectionReader,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
) -> Result<()> {
    let mut decoder = CotStreamDecoder::new(8 * 1024 * 1024);
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        for raw in decoder.push(&buffer[..count])? {
            let event = inspect_event(raw)?;
            metrics.received_messages.fetch_add(1, Ordering::Relaxed);
            if let Some(correlation_id) = event.correlation_id {
                let mut ledger = ledger.lock().await;
                if !ledger.seen_by_client.insert((client_id, correlation_id)) {
                    metrics.duplicate_messages.fetch_add(1, Ordering::Relaxed);
                } else if let Some(sent) = ledger.sent.get(&correlation_id) {
                    metrics.record_delivery(sent.elapsed()).await;
                }
            }
        }
    }
}

async fn send_positions(
    client_id: u32,
    config: &AppConfig,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    writer: &mut ConnectionWriter,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut ticker = interval(config.run.gps_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Tokio intervals tick immediately. Delay the first position so servers
    // which establish their per-client routing state asynchronously can finish
    // registering the connection before the initial CoT event arrives.
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = OffsetDateTime::now_utc();
                let correlation_id = Uuid::new_v4();
                let event = PositionEvent {
                    uid: format!("tak-bench-{client_id}"), callsign: format!("TB-{client_id:04}"),
                    latitude: -23.5505, longitude: -46.6333, altitude_m: 760.0,
                    course_deg: 0.0, speed_mps: 0.0, time: now,
                    stale: now + time::Duration::seconds(i64::try_from(config.run.gps_interval.as_secs()).unwrap_or(30) * 2),
                    correlation_id,
                };
                ledger.lock().await.sent.insert(correlation_id, Instant::now());
                write_fragmented(writer, event.to_xml().as_bytes(), &config.scenario.fragmentation.chunk_sizes).await?;
                metrics.sent_messages.fetch_add(1, Ordering::Relaxed);
            }
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { break; },
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    Ok(())
}

async fn write_fragmented(
    writer: &mut ConnectionWriter,
    bytes: &[u8],
    chunks: &[usize],
) -> Result<()> {
    if chunks.is_empty() {
        return connection::write_all(writer, bytes).await;
    }
    let mut offset = 0;
    for chunk in chunks.iter().copied().cycle() {
        if offset == bytes.len() {
            return Ok(());
        }
        let end = offset.saturating_add(chunk.max(1)).min(bytes.len());
        connection::write_all(writer, &bytes[offset..end]).await?;
        offset = end;
    }
    Ok(())
}

#[must_use]
pub fn minimum_stale_interval(interval: Duration) -> Duration {
    interval.saturating_mul(2)
}
