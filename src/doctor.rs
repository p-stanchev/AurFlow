use std::collections::HashSet;
use std::time::Instant;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use reqwest::header::HeaderName;
use reqwest::{Client, Url};
use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::forward;
use crate::registry::{Header, Provider, Registry};
use crate::secrets::SecretManager;

pub async fn run(config: Config, registry: Registry, secrets: SecretManager) -> Result<()> {
    println!(
        "Running ORLB doctor against {} provider(s)...",
        registry.len()
    );

    let mut issues = Vec::new();
    validate_providers(registry.providers(), &mut issues);

    let (reports, connectivity_issues) =
        connectivity_probe(&config, registry.providers(), &secrets).await?;
    issues.extend(connectivity_issues);

    if reports.is_empty() {
        println!("Skipped reachability probes (no providers?).");
    } else {
        println!("Reachability summary:");
        for report in &reports {
            let slot = report
                .slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "--".into());
            println!(
                "    - {:<24} {:>6.1} ms (slot {})",
                report.name, report.latency_ms, slot
            );
            if let Some(tx) = &report.transaction {
                let tx_slot = tx
                    .slot
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "--".into());
                println!("        sample tx {} (slot {})", tx.signature, tx_slot);
            }
        }
        if let Some(best_slot) = reports.iter().filter_map(|r| r.slot).max() {
            println!("Latest observed slot: {}", best_slot);
            for report in &reports {
                match report.slot {
                    Some(slot) => {
                        let lag = best_slot.saturating_sub(slot);
                        if lag > config.slot_lag_alert_slots {
                            issues.push(format!(
                                "provider `{}` is {} slots behind latest ({} vs {})",
                                report.name, lag, slot, best_slot
                            ));
                        }
                    }
                    None => issues.push(format!(
                        "provider `{}` did not return a slot height",
                        report.name
                    )),
                }
            }
        } else {
            issues.push("no provider returned a slot height".to_string());
        }
    }

    if issues.is_empty() {
        println!("No issues detected. Provider pool looks healthy.");
        Ok(())
    } else {
        println!("{} issue(s) detected:", issues.len());
        for issue in &issues {
            println!("    - {}", issue);
        }
        Err(anyhow!("provider configuration requires attention"))
    }
}

fn validate_providers(providers: &[Provider], issues: &mut Vec<String>) {
    if providers.is_empty() {
        issues.push("provider registry is empty".to_string());
    }

    let mut names = HashSet::new();
    let mut urls = HashSet::new();

    for provider in providers {
        if !names.insert(provider.name.clone()) {
            issues.push(format!(
                "duplicate provider name `{}` detected",
                provider.name
            ));
        }
        if !urls.insert(provider.url.clone()) {
            issues.push(format!(
                "duplicate provider URL `{}` detected",
                provider.url
            ));
        }
        if provider.weight == 0 {
            issues.push(format!(
                "provider `{}` has weight 0 (must be >= 1)",
                provider.name
            ));
        }
        if let Err(err) = Url::parse(&provider.url) {
            issues.push(format!(
                "provider `{}` has invalid URL `{}`: {}",
                provider.name, provider.url, err
            ));
        }
        if let Some(headers) = provider.headers.as_ref() {
            validate_headers(provider, headers, issues);
        }
    }
}

fn validate_headers(provider: &Provider, headers: &[Header], issues: &mut Vec<String>) {
    let mut seen = HashSet::new();
    for header in headers {
        if header.name.trim().is_empty() {
            issues.push(format!(
                "provider `{}` includes an empty header name",
                provider.name
            ));
            continue;
        }
        let normalized = header.name.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            issues.push(format!(
                "provider `{}` repeats header `{}`",
                provider.name, header.name
            ));
        }
        if header.value.trim().is_empty() {
            issues.push(format!(
                "provider `{}` header `{}` has an empty value",
                provider.name, header.name
            ));
        }
        if HeaderName::from_bytes(normalized.as_bytes()).is_err() {
            issues.push(format!(
                "provider `{}` header `{}` is not a valid HTTP header",
                provider.name, header.name
            ));
        }
    }
}

struct ProbeReport {
    name: String,
    latency_ms: f64,
    slot: Option<u64>,
    transaction: Option<TransactionProbe>,
}

struct TransactionProbe {
    signature: String,
    slot: Option<u64>,
}

async fn connectivity_probe(
    config: &Config,
    providers: &[Provider],
    secrets: &SecretManager,
) -> Result<(Vec<ProbeReport>, Vec<String>)> {
    if providers.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let client = forward::build_http_client(config.request_timeout)?;

    let payload = Bytes::from(serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": "orlb-doctor",
        "method": "getSlot",
        "params": []
    }))?);

    let mut probes = JoinSet::new();
    for provider in providers.iter().cloned() {
        let client = client.clone();
        let payload = payload.clone();
        let secrets = secrets.clone();
        probes.spawn(async move {
            let start = Instant::now();
            match forward::send_request(&client, &provider, payload, &secrets).await {
                Ok(response) => {
                    let latency = start.elapsed();
                    let status = response.status();
                    if !status.is_success() {
                        return Err(format!(
                            "provider `{}` responded with status {}",
                            provider.name, status
                        ));
                    }
                    let value: Value = match response.json().await {
                        Ok(value) => value,
                        Err(err) => {
                            return Err(format!(
                                "provider `{}` JSON decode failed: {}",
                                provider.name, err
                            ));
                        }
                    };
                    let slot = extract_slot(&value);
                    let transaction = match provider.sample_signature.as_ref() {
                        Some(signature) => {
                            match fetch_sample_transaction(&client, &provider, signature, &secrets)
                                .await
                            {
                                Ok(tx) => Some(tx),
                                Err(err) => {
                                    return Err(format!(
                                        "provider `{}` sample transaction `{}` failed: {}",
                                        provider.name, signature, err
                                    ));
                                }
                            }
                        }
                        None => None,
                    };
                    Ok(ProbeReport {
                        name: provider.name,
                        latency_ms: latency.as_secs_f64() * 1000.0,
                        slot,
                        transaction,
                    })
                }
                Err(err) => Err(format!(
                    "provider `{}` transport error: {}",
                    provider.name, err
                )),
            }
        });
    }

    let mut successes = Vec::new();
    let mut issues = Vec::new();
    while let Some(result) = probes.join_next().await {
        match result {
            Ok(Ok(report)) => successes.push(report),
            Ok(Err(issue)) => issues.push(issue),
            Err(join_err) => {
                issues.push(format!("diagnostic task failed to join: {}", join_err));
            }
        }
    }

    successes.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((successes, issues))
}

fn extract_slot(value: &Value) -> Option<u64> {
    value.get("result").and_then(Value::as_u64).or_else(|| {
        value
            .get("result")
            .and_then(|inner| inner.get("slot").and_then(Value::as_u64))
    })
}

async fn fetch_sample_transaction(
    client: &Client,
    provider: &Provider,
    signature: &str,
    secrets: &SecretManager,
) -> Result<TransactionProbe, String> {
    let payload = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": format!("orlb-doctor-{}", signature),
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "json",
                "commitment": "confirmed"
            }
        ]
    }))
    .map(Bytes::from)
    .map_err(|err| format!("failed to serialize transaction payload: {err}"))?;

    let response = forward::send_request(client, provider, payload, secrets)
        .await
        .map_err(|err| format!("transport error: {err}"))?;
    if !response.status().is_success() {
        return Err(format!("status {}", response.status()));
    }

    let value: Value = response
        .json()
        .await
        .map_err(|err| format!("JSON decode failed: {err}"))?;
    if value.get("result").is_none() {
        return Err("missing result field".into());
    }
    let slot = value
        .get("result")
        .and_then(|inner| inner.get("slot").and_then(Value::as_u64));
    Ok(TransactionProbe {
        signature: signature.to_string(),
        slot,
    })
}
