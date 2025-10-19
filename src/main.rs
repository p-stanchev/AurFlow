mod config;
mod dashboard;
mod errors;
mod forward;
mod health;
mod metrics;
mod registry;
mod router;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::forward::build_http_client;
use crate::metrics::Metrics;
use crate::registry::Registry;

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing()?;

    let config = Config::from_env()?;
    let registry = Registry::load(&config.providers_path)?;

    tracing::info!(
        listen_addr = %config.listen_addr,
        providers = registry.len(),
        "starting ORLB"
    );

    let metrics = Metrics::new(registry.clone())?;
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

fn setup_tracing() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("orlb=info,hyper=warn,reqwest=warn"))?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
    Ok(())
}
