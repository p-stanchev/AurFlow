use std::collections::HashMap;
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
    pub hedge_requests: bool,
    pub hedge_delay: Duration,
    pub adaptive_hedging: bool,
    pub hedge_min_delay_ms: f64,
    pub hedge_max_delay_ms: f64,
    pub slo_target: f64,
    pub otel: OtelConfig,
    pub tag_weights: HashMap<String, f64>,
    pub secrets: SecretsConfig,
}

/// OpenTelemetry configuration derived from environment variables.
#[derive(Clone, Debug)]
pub struct OtelConfig {
    pub exporter: OtelExporter,
    pub service_name: String,
}

/// Supported OpenTelemetry exporters.
#[derive(Clone, Debug)]
pub enum OtelExporter {
    None,
    Stdout,
    OtlpHttp { endpoint: String },
}

#[derive(Clone, Debug)]
pub struct SecretsConfig {
    pub backend: SecretBackend,
}

#[derive(Clone, Debug)]
pub enum SecretBackend {
    None,
    Vault(VaultConfig),
    Gcp(GcpConfig),
    Aws(AwsConfig),
}

#[derive(Clone, Debug)]
pub struct VaultConfig {
    pub addr: String,
    pub token: String,
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct GcpConfig {
    pub project: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AwsConfig {
    pub region: Option<String>,
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
        let hedge_requests = parse_env("ORLB_HEDGE_REQUESTS", "false", parse_bool)?;
        let hedge_delay = clamp_duration(
            parse_env("ORLB_HEDGE_DELAY_MS", "60", parse_duration_millis)?,
            Duration::from_millis(5),
            Duration::from_millis(500),
        );
        let adaptive_hedging = parse_env("ORLB_ADAPTIVE_HEDGING", "true", parse_bool)?;
        let hedge_min_delay_ms = clamp_f64(
            parse_env("ORLB_HEDGE_MIN_DELAY_MS", "10", parse_f64)?,
            5.0,
            100.0,
        );
        let hedge_max_delay_ms = clamp_f64(
            parse_env("ORLB_HEDGE_MAX_DELAY_MS", "200", parse_f64)?,
            50.0,
            1000.0,
        );
        if hedge_min_delay_ms > hedge_max_delay_ms {
            return Err(anyhow!(
                "ORLB_HEDGE_MIN_DELAY_MS ({hedge_min_delay_ms}) must be <= ORLB_HEDGE_MAX_DELAY_MS ({hedge_max_delay_ms})"
            ));
        }
        let slo_target = clamp_f64(
            parse_env("ORLB_SLO_TARGET", "0.995", parse_f64)?,
            0.9,
            0.9999,
        );
        let otel_service_name = parse_env("ORLB_OTEL_SERVICE_NAME", "orlb", parse_string)?
            .trim()
            .to_string();
        let otel_exporter_raw = env::var("ORLB_OTEL_EXPORTER")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "none".to_string());
        let otel_endpoint = env::var("ORLB_OTEL_ENDPOINT").ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        let otel_exporter = parse_otel_exporter(&otel_exporter_raw, otel_endpoint)?;
        let tag_weights_raw = env::var("ORLB_TAG_WEIGHTS").unwrap_or_default();
        let tag_weights = parse_tag_weights(tag_weights_raw.trim())?;
        let secrets = parse_secret_backend()?;

        Ok(Self {
            listen_addr,
            providers_path,
            probe_interval,
            request_timeout,
            retry_read_requests,
            dashboard_assets_dir,
            slot_lag_penalty_ms,
            slot_lag_alert_slots,
            hedge_requests,
            hedge_delay,
            adaptive_hedging,
            hedge_min_delay_ms,
            hedge_max_delay_ms,
            slo_target,
            otel: OtelConfig {
                exporter: otel_exporter,
                service_name: otel_service_name,
            },
            tag_weights,
            secrets,
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

fn parse_duration_millis(input: &str) -> Result<Duration> {
    let ms: u64 = input
        .parse()
        .with_context(|| format!("invalid duration milliseconds `{input}`"))?;
    Ok(Duration::from_millis(ms))
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

fn parse_string(input: &str) -> Result<String> {
    Ok(input.to_string())
}

fn parse_otel_exporter(value: &str, endpoint: Option<String>) -> Result<OtelExporter> {
    match value.to_ascii_lowercase().as_str() {
        "" | "none" => Ok(OtelExporter::None),
        "stdout" => Ok(OtelExporter::Stdout),
        "otlp_http" | "otlp-http" => {
            let endpoint = endpoint.ok_or_else(|| {
                anyhow!("ORLB_OTEL_ENDPOINT must be set when ORLB_OTEL_EXPORTER=otlp_http")
            })?;
            Ok(OtelExporter::OtlpHttp { endpoint })
        }
        other => Err(anyhow!("unsupported OTLP exporter `{other}`")),
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

fn parse_tag_weights(input: &str) -> Result<HashMap<String, f64>> {
    let mut weights = HashMap::new();
    if input.trim().is_empty() {
        return Ok(weights);
    }

    for part in input.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut pieces = trimmed.splitn(2, '=');
        let tag = pieces
            .next()
            .map(|tag| tag.trim().to_ascii_lowercase())
            .filter(|tag| !tag.is_empty())
            .ok_or_else(|| anyhow!("invalid tag weight segment `{trimmed}`"))?;
        let value = pieces
            .next()
            .ok_or_else(|| anyhow!("missing value for tag `{tag}`"))?
            .trim();
        let multiplier: f64 = value
            .parse()
            .with_context(|| format!("invalid weight `{value}` for tag `{tag}`"))?;
        if multiplier <= 0.0 {
            return Err(anyhow!(
                "tag weight for `{tag}` must be greater than zero (got {multiplier})"
            ));
        }
        weights.insert(tag, multiplier);
    }

    Ok(weights)
}

fn parse_secret_backend() -> Result<SecretsConfig> {
    let backend = env::var("ORLB_SECRET_BACKEND")
        .unwrap_or_else(|_| "none".into())
        .to_ascii_lowercase();
    let backend = match backend.as_str() {
        "" | "none" => SecretBackend::None,
        "vault" => {
            let addr = env::var("ORLB_VAULT_ADDR").map_err(|_| {
                anyhow!("ORLB_VAULT_ADDR must be set when ORLB_SECRET_BACKEND=vault")
            })?;
            let token = env::var("ORLB_VAULT_TOKEN").map_err(|_| {
                anyhow!("ORLB_VAULT_TOKEN must be set when ORLB_SECRET_BACKEND=vault")
            })?;
            let namespace = env::var("ORLB_VAULT_NAMESPACE")
                .ok()
                .filter(|value| !value.is_empty());
            SecretBackend::Vault(VaultConfig {
                addr,
                token,
                namespace,
            })
        }
        "gcp" => {
            let project = env::var("ORLB_GCP_PROJECT")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            SecretBackend::Gcp(GcpConfig { project })
        }
        "aws" | "aws_secrets_manager" | "aws-secrets-manager" => {
            let region = env::var("ORLB_AWS_REGION")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            SecretBackend::Aws(AwsConfig { region })
        }
        other => return Err(anyhow!("unsupported secret backend `{other}`")),
    };
    Ok(SecretsConfig { backend })
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::env;
    use std::sync::Mutex;

    static ENV_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn reset_env() {
        env::remove_var("ORLB_HEDGE_MIN_DELAY_MS");
        env::remove_var("ORLB_HEDGE_MAX_DELAY_MS");
    }

    #[test]
    fn rejects_inverted_hedge_delays() {
        let _lock = ENV_GUARD.lock().unwrap();
        reset_env();
        env::set_var("ORLB_HEDGE_MIN_DELAY_MS", "80");
        env::set_var("ORLB_HEDGE_MAX_DELAY_MS", "60");

        let result = Config::from_env();

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ORLB_HEDGE_MIN_DELAY_MS"),
            "expected inverted hedge delay to error"
        );
        reset_env();
    }

    #[test]
    fn accepts_valid_hedge_delays() {
        let _lock = ENV_GUARD.lock().unwrap();
        reset_env();
        env::set_var("ORLB_HEDGE_MIN_DELAY_MS", "20");
        env::set_var("ORLB_HEDGE_MAX_DELAY_MS", "120");

        let config = Config::from_env().expect("valid hedge delay bounds");
        assert_eq!(config.hedge_min_delay_ms, 20.0);
        assert_eq!(config.hedge_max_delay_ms, 120.0);
        reset_env();
    }
}
