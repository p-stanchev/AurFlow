use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};

/// Application configuration derived from environment variables.
#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub providers_path: PathBuf,
    pub probe_interval: Duration,
    pub request_timeout: Duration,
    pub retry_read_requests: bool,
    pub dashboard_assets_dir: Option<PathBuf>,
    pub slot_lag_penalty_ms: f64,
    pub slot_lag_alert_slots: u64,
}

impl Config {
    /// Load configuration from environment variables with sensible defaults.
    pub fn from_env() -> Result<Self> {
        let listen_addr = parse_env("ORLB_LISTEN_ADDR", "0.0.0.0:8080", parse_socket_addr)?;
        let providers_path = parse_env("ORLB_PROVIDERS_PATH", "providers.json", parse_path)?;
        let probe_interval = clamp_duration(
            parse_env("ORLB_PROBE_INTERVAL_SECS", "5", parse_duration_secs)?,
            Duration::from_secs(2),
            Duration::from_secs(60),
        );
        let request_timeout = clamp_duration(
            parse_env("ORLB_REQUEST_TIMEOUT_SECS", "10", parse_duration_secs)?,
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        let retry_read_requests = parse_env("ORLB_RETRY_READ_REQUESTS", "true", parse_bool)?;
        let dashboard_assets_dir = env::var("ORLB_DASHBOARD_DIR")
            .ok()
            .map(|value| Path::new(&value).to_path_buf());
        let slot_lag_penalty_ms = clamp_f64(
            parse_env("ORLB_SLOT_LAG_PENALTY_MS", "5", parse_f64)?,
            0.0,
            5000.0,
        );
        let slot_lag_alert_slots =
            parse_env("ORLB_SLOT_LAG_ALERT_SLOTS", "50", parse_u64)?.min(10_000);

        Ok(Self {
            listen_addr,
            providers_path,
            probe_interval,
            request_timeout,
            retry_read_requests,
            dashboard_assets_dir,
            slot_lag_penalty_ms,
            slot_lag_alert_slots,
        })
    }
}

fn clamp_duration(value: Duration, min: Duration, max: Duration) -> Duration {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn clamp_f64(value: f64, min: f64, max: f64) -> f64 {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn parse_socket_addr(input: &str) -> Result<SocketAddr> {
    input
        .parse::<SocketAddr>()
        .map_err(|err| anyhow!("invalid socket address `{input}`: {err}"))
}

fn parse_path(input: &str) -> Result<PathBuf> {
    Ok(Path::new(input).to_path_buf())
}

fn parse_duration_secs(input: &str) -> Result<Duration> {
    let secs: u64 = input
        .parse()
        .with_context(|| format!("invalid duration seconds `{input}`"))?;
    Ok(Duration::from_secs(secs))
}

fn parse_f64(input: &str) -> Result<f64> {
    input
        .parse::<f64>()
        .with_context(|| format!("invalid floating point value `{input}`"))
}

fn parse_u64(input: &str) -> Result<u64> {
    input
        .parse::<u64>()
        .with_context(|| format!("invalid integer value `{input}`"))
}

fn parse_bool(input: &str) -> Result<bool> {
    match input.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => Err(anyhow!("invalid boolean `{input}`")),
    }
}

fn parse_env<T, F>(key: &str, default: &str, parser: F) -> Result<T>
where
    F: Fn(&str) -> Result<T>,
{
    match env::var(key).ok().filter(|value| !value.is_empty()) {
        Some(value) => parser(&value),
        None => parser(default),
    }
}
