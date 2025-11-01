mod config;
mod dashboard;
mod doctor;
mod errors;
mod forward;
mod health;
mod metrics;
mod registry;
mod router;
mod telemetry;

use anyhow::Result;

use crate::config::Config;
use crate::forward::build_http_client;
use crate::metrics::Metrics;
use crate::registry::Registry;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let _ = args.next();
    let subcommand = args.next();

    let config = Config::from_env()?;
    let _telemetry = telemetry::init(&config)?;
    let registry = Registry::load(&config.providers_path)?;

    if matches!(subcommand.as_deref(), Some("doctor")) {
        doctor::run(config.clone(), registry.clone()).await?;
        return Ok(());
    }

    tracing::info!(
        listen_addr = %config.listen_addr,
        providers = registry.len(),
        "starting ORLB"
    );

    let metrics = Metrics::new(registry.clone(), &config)?;
    let client = build_http_client(config.request_timeout)?;

    let health_registry = registry.clone();
    let health_metrics = metrics.clone();
    let health_client = client.clone();
    let probe_interval = config.probe_interval;

    tokio::spawn(async move {
        tracing::info!(
            interval_secs = probe_interval.as_secs(),
            "launching health monitor"
        );
        health::run(
            health_registry,
            health_metrics,
            health_client,
            probe_interval,
        )
        .await;
    });

    router::start_server(config, metrics, client).await
}
