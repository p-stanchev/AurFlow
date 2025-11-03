use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::commitment::Commitment;
use crate::forward;
use crate::metrics::{CommitmentSlots, Metrics};
use crate::registry::{Provider, Registry};

const HEALTH_METHOD: &str = "getSlot";

pub async fn run(registry: Registry, metrics: Metrics, client: Client, probe_interval: Duration) {
    let payload = json!({
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
    let response = match forward::send_request(&client, &provider, payload).await {
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

    let mut slots = CommitmentSlots::from_slot(Commitment::Finalized, slot);
    for commitment in [Commitment::Confirmed, Commitment::Processed] {
        if let Some(extra_slot) = fetch_commitment_slot(&client, &provider, commitment).await {
            slots.set(commitment, Some(extra_slot));
        }
    }

    metrics
        .record_health_success(&provider.name, elapsed, slots)
        .await;

    let finalized_slot = slots.get(Commitment::Finalized);
    let confirmed_slot = slots.get(Commitment::Confirmed);
    let processed_slot = slots.get(Commitment::Processed);
    tracing::trace!(
        provider = provider.name,
        latency_ms = elapsed.as_secs_f64() * 1000.0,
        finalized_slot,
        confirmed_slot,
        processed_slot,
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

async fn fetch_commitment_slot(
    client: &Client,
    provider: &Provider,
    commitment: Commitment,
) -> Option<u64> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": format!("orlb-health-{}", commitment.as_str()),
        "method": HEALTH_METHOD,
        "params": [
            {
                "commitment": commitment.as_str()
            }
        ]
    });
    let payload = match serde_json::to_vec(&payload) {
        Ok(vec) => Bytes::from(vec),
        Err(err) => {
            tracing::debug!(
                error = ?err,
                provider = provider.name,
                commitment = %commitment,
                "failed to serialize commitment health payload"
            );
            return None;
        }
    };

    let response = match forward::send_request(client, provider, payload).await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::debug!(
                error = ?err,
                provider = provider.name,
                commitment = %commitment,
                "commitment probe transport error"
            );
            return None;
        }
    };

    if !response.status().is_success() {
        tracing::debug!(
            status = %response.status(),
            provider = provider.name,
            commitment = %commitment,
            "commitment probe non-success status"
        );
        return None;
    }

    match response.json::<Value>().await {
        Ok(value) => extract_slot(&value),
        Err(err) => {
            tracing::debug!(
                error = ?err,
                provider = provider.name,
                commitment = %commitment,
                "failed to decode commitment probe response"
            );
            None
        }
    }
}

fn extract_slot(value: &Value) -> Option<u64> {
    value.get("result").and_then(Value::as_u64).or_else(|| {
        value
            .get("result")
            .and_then(|v| v.get("slot").and_then(Value::as_u64))
    })
}
