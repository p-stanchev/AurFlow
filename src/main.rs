mod commitment;
mod config;
mod dashboard;
mod doctor;
mod errors;
mod forward;
mod health;
mod metrics;
mod registry;
mod replay;
mod router;
mod secrets;
mod telemetry;
mod ws;

use anyhow::Result;

use crate::config::Config;
use crate::forward::build_http_client;
use crate::metrics::Metrics;
use crate::registry::{Registry, SharedRegistry};
use crate::secrets::SecretManager;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let _ = args.next();
    let subcommand = args.next();

    let config = Config::from_env()?;
    let _telemetry = telemetry::init(&config)?;
    let secrets = SecretManager::new(&config.secrets)?;
    let registry = Registry::load(&config.providers_path)?;
    let shared_registry = SharedRegistry::new(registry);

    if matches!(subcommand.as_deref(), Some("doctor")) {
        doctor::run(config.clone(), shared_registry.snapshot(), secrets.clone()).await?;
        return Ok(());
    }

    if matches!(subcommand.as_deref(), Some("replay")) {
        let bundle = args
            .next()
            .ok_or_else(|| anyhow::anyhow!("expected bundle path after `replay`"))?;
        let snapshot = shared_registry.snapshot();
        replay::run(&config, &snapshot, secrets.clone(), bundle.into()).await?;
        return Ok(());
    }

    tracing::info!(
        listen_addr = %config.listen_addr,
        providers = shared_registry.snapshot().len(),
        "starting ORLB"
    );

    let metrics = Metrics::new(shared_registry.clone(), &config)?;
    let client = build_http_client(config.request_timeout)?;

    let health_registry = shared_registry.clone();
    let health_metrics = metrics.clone();
    let health_client = client.clone();
    let probe_interval = config.probe_interval;
    let health_secrets = secrets.clone();

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
            health_secrets,
        )
        .await;
    });

    router::start_server(config, shared_registry, metrics, client, secrets).await
}
