use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use reqwest::Client;
use serde_json::Value;
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::forward;
use crate::metrics::Metrics;
use crate::registry::{Provider, Registry};

const HEALTH_METHOD: &str = "getSlot";

pub async fn run(registry: Registry, metrics: Metrics, client: Client, probe_interval: Duration) {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "orlb-health",
        "method": HEALTH_METHOD,
        "params": []
    });
    let payload_bytes = Bytes::from(
        serde_json::to_vec(&payload).expect("health payload serialization should never fail"),
    );

    let mut ticker = interval(probe_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let mut tasks = JoinSet::new();

        for provider in registry.providers().iter().cloned() {
            let payload = payload_bytes.clone();
            let client = client.clone();
            let metrics = metrics.clone();

            tasks.spawn(async move {
                if let Err(err) =
                    probe_provider(client, metrics, provider, payload.clone(), probe_interval).await
                {
                    tracing::warn!(error = ?err, "health probe failed");
                }
            });
        }

        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                tracing::warn!(error = ?err, "health probe task join error");
            }
        }
    }
}

async fn probe_provider(
    client: Client,
    metrics: Metrics,
    provider: Provider,
    payload: Bytes,
    interval: Duration,
) -> Result<()> {
    let start = Instant::now();
    let response = match forward::send_request(&client, &provider, payload.as_ref()).await {
        Ok(resp) => resp,
        Err(err) => {
            metrics
                .record_health_failure(&provider.name, "transport")
                .await;
            tracing::debug!(
                error = ?err,
                provider = provider.name,
                "health probe transport error"
            );
            return Ok(());
        }
    };

    let elapsed = start.elapsed();
    if !response.status().is_success() {
        metrics
            .record_health_failure(
                &provider.name,
                &format!("status_{}", response.status().as_u16()),
            )
            .await;
        tracing::debug!(
            status = %response.status(),
            provider = provider.name,
            "health probe non-success status"
        );
        return Ok(());
    }

    let slot = match response.json::<Value>().await {
        Ok(value) => extract_slot(&value),
        Err(err) => {
            metrics
                .record_health_failure(&provider.name, "decode")
                .await;
            tracing::debug!(
                error = ?err,
                provider = provider.name,
                "failed to decode health probe response"
            );
            None
        }
    };

    metrics
        .record_health_success(&provider.name, elapsed, slot)
        .await;
    tracing::trace!(
        provider = provider.name,
        latency_ms = elapsed.as_secs_f64() * 1000.0,
        slot,
        "health probe success"
    );

    // If probes run slower than interval, log.
    if elapsed > interval {
        tracing::warn!(
            provider = provider.name,
            latency_ms = elapsed.as_secs_f64() * 1000.0,
            "health probe exceeded interval"
        );
    }

    Ok(())
}

fn extract_slot(value: &Value) -> Option<u64> {
    value.get("result").and_then(Value::as_u64).or_else(|| {
        value
            .get("result")
            .and_then(|v| v.get("slot").and_then(Value::as_u64))
    })
}
