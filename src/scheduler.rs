use std::time::Duration;

use rand::{SeedableRng, seq::SliceRandom};
use thiserror::Error;

use crate::config::{RampStrategy, SchedulerConfig};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("step ramps must be ordered by time and cannot exceed the configured client count")]
    InvalidSteps,
}

/// # Errors
///
/// Returns an error for an invalid step schedule.
pub fn start_delays(
    clients: u32,
    scheduler: &SchedulerConfig,
) -> Result<Vec<Duration>, SchedulerError> {
    let mut delays = match scheduler.strategy {
        RampStrategy::Immediate => vec![Duration::ZERO; clients as usize],
        RampStrategy::Linear | RampStrategy::Randomized => (0..clients)
            .map(|index| {
                scheduler
                    .ramp_up
                    .mul_f64(f64::from(index) / f64::from(clients.max(1)))
            })
            .collect(),
        RampStrategy::Step => step_delays(clients, scheduler)?,
    };
    if scheduler.strategy == RampStrategy::Randomized {
        let mut rng = rand::rngs::StdRng::seed_from_u64(scheduler.seed.unwrap_or(0));
        delays.shuffle(&mut rng);
    }
    Ok(delays)
}

fn step_delays(clients: u32, scheduler: &SchedulerConfig) -> Result<Vec<Duration>, SchedulerError> {
    let mut previous_at = Duration::ZERO;
    let mut previous_clients = 0;
    let mut result = Vec::with_capacity(clients as usize);
    for step in &scheduler.steps {
        if step.at < previous_at || step.clients < previous_clients || step.clients > clients {
            return Err(SchedulerError::InvalidSteps);
        }
        result.extend(std::iter::repeat_n(
            step.at,
            (step.clients - previous_clients) as usize,
        ));
        previous_at = step.at;
        previous_clients = step.clients;
    }
    result.extend(std::iter::repeat_n(
        previous_at,
        (clients - previous_clients) as usize,
    ));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linear_spreads_clients() {
        let scheduler = SchedulerConfig {
            strategy: RampStrategy::Linear,
            ramp_up: Duration::from_secs(10),
            ..SchedulerConfig::default()
        };
        assert_eq!(
            start_delays(3, &scheduler).unwrap(),
            vec![
                Duration::ZERO,
                Duration::from_secs_f64(10.0 / 3.0),
                Duration::from_secs_f64(20.0 / 3.0)
            ]
        );
    }
}
