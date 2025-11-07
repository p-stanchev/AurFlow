use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;

use crate::config::Config;
use crate::forward::{build_http_client, send_request};
use crate::registry::{Provider, Registry};
use crate::secrets::SecretManager;

pub async fn run(
    config: &Config,
    registry: &Registry,
    secrets: SecretManager,
    bundle_path: PathBuf,
) -> Result<()> {
    let entries = load_entries(&bundle_path)?;
    if entries.is_empty() {
        bail!("replay bundle `{}` is empty", bundle_path.display());
    }

    let client = build_http_client(config.request_timeout)?;
    println!(
        "Replaying {} archived requests against {} providers...",
        entries.len(),
        registry.len()
    );

    let mut summary = ReplaySummary::default();

    for (index, entry) in entries.iter().enumerate() {
        let label = entry.label(index);
        let payload = serde_json::to_vec(&entry.request)
            .with_context(|| format!("entry `{}` request could not be encoded to JSON", label))?;

        let responses =
            replay_across_providers(&client, registry.providers(), &payload, &secrets).await;

        let baseline = pick_baseline(entry.expected.clone(), &responses);
        let (baseline_value, baseline_source) = match baseline {
            Some(pair) => pair,
            None => {
                summary.entries_without_baseline += 1;
                println!(
                    "[{}] {} — no successful responses to compare ({} providers failed)",
                    label,
                    entry.method().unwrap_or("unknown"),
                    responses.len()
                );
                for response in responses {
                    if let ReplayState::Error(err) = response.state {
                        println!("    - {} errored: {}", response.provider, err);
                    }
                }
                continue;
            }
        };

        let mut matches = 0usize;
        let mut successes = 0usize;
        let mut divergences = Vec::new();
        let mut failures = Vec::new();

        for response in responses {
            match response.state {
                ReplayState::Success(value) => {
                    successes += 1;
                    if value == baseline_value {
                        matches += 1;
                    } else {
                        divergences.push(Divergence {
                            provider: response.provider,
                            latency: response.latency,
                            value,
                        });
                    }
                }
                ReplayState::HttpFailure { status, body } => failures.push(format!(
                    "{} returned {} ({})",
                    response.provider, status, body
                )),
                ReplayState::Error(err) => {
                    failures.push(format!("{} errored: {}", response.provider, err))
                }
            }
        }

        if !divergences.is_empty() {
            summary.entries_with_divergence += 1;
        } else {
            summary.entries_fully_matching += 1;
        }

        println!(
            "[{}] method={} matches {}/{} successes, divergences {}, errors {} (baseline: {})",
            label,
            entry.method().unwrap_or("unknown"),
            matches,
            successes,
            divergences.len(),
            failures.len(),
            baseline_source.describe()
        );

        if let Some(description) = entry.description() {
            println!("    note: {}", description);
        }

        for divergence in divergences {
            println!(
                "    - {} diverged ({}): {}",
                divergence.provider,
                format_latency(divergence.latency),
                summarize_json(&divergence.value)
            );
        }

        for failure in failures {
            println!("    - {}", failure);
        }
    }

    println!(
        "Replay complete: {} entries ok, {} entries with divergence, {} without baseline responses",
        summary.entries_fully_matching,
        summary.entries_with_divergence,
        summary.entries_without_baseline
    );

    Ok(())
}

#[derive(Default)]
struct ReplaySummary {
    entries_fully_matching: usize,
    entries_with_divergence: usize,
    entries_without_baseline: usize,
}

#[derive(Debug)]
struct Divergence {
    provider: String,
    latency: Duration,
    value: Value,
}

async fn replay_across_providers(
    client: &Client,
    providers: &[Provider],
    payload: &[u8],
    secrets: &SecretManager,
) -> Vec<ReplayResponse> {
    let mut responses = Vec::with_capacity(providers.len());
    for provider in providers {
        responses.push(replay_single(client, provider, payload, secrets).await);
    }
    responses
}

async fn replay_single(
    client: &Client,
    provider: &Provider,
    payload: &[u8],
    secrets: &SecretManager,
) -> ReplayResponse {
    let start = Instant::now();
    let body = Bytes::copy_from_slice(payload);
    let outcome = send_request(client, provider, body, secrets).await;
    let latency = start.elapsed();

    match outcome {
        Ok(response) => {
            let status = response.status();
            match response.bytes().await {
                Ok(bytes) => {
                    if !status.is_success() {
                        ReplayResponse {
                            provider: provider.name.clone(),
                            latency,
                            state: ReplayState::HttpFailure {
                                status,
                                body: body_snippet(&bytes),
                            },
                        }
                    } else {
                        match serde_json::from_slice::<Value>(&bytes) {
                            Ok(value) => ReplayResponse {
                                provider: provider.name.clone(),
                                latency,
                                state: ReplayState::Success(value),
                            },
                            Err(err) => ReplayResponse {
                                provider: provider.name.clone(),
                                latency,
                                state: ReplayState::Error(format!("invalid JSON body: {}", err)),
                            },
                        }
                    }
                }
                Err(err) => ReplayResponse {
                    provider: provider.name.clone(),
                    latency,
                    state: ReplayState::Error(format!("failed to read response body: {err}")),
                },
            }
        }
        Err(err) => ReplayResponse {
            provider: provider.name.clone(),
            latency,
            state: ReplayState::Error(err.to_string()),
        },
    }
}

#[derive(Debug)]
struct ReplayResponse {
    provider: String,
    latency: Duration,
    state: ReplayState,
}

#[derive(Debug)]
enum ReplayState {
    Success(Value),
    HttpFailure { status: StatusCode, body: String },
    Error(String),
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayEntry {
    id: Option<String>,
    request: Value,
    #[serde(default)]
    expected: Option<Value>,
    #[serde(default)]
    description: Option<String>,
}

impl ReplayEntry {
    fn label(&self, index: usize) -> String {
        self.id
            .clone()
            .unwrap_or_else(|| format!("entry-{}", index + 1))
    }

    fn method(&self) -> Option<&str> {
        match &self.request {
            Value::Object(map) => map.get("method").and_then(|value| value.as_str()),
            _ => None,
        }
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn validate(&self) -> Result<()> {
        match &self.request {
            Value::Object(_) => Ok(()),
            _ => Err(anyhow!(
                "every replay entry must provide a JSON-RPC object in `request`"
            )),
        }
    }
}

fn load_entries(path: &Path) -> Result<Vec<ReplayEntry>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read replay bundle from `{}`", path.display()))?;
    let entries: Vec<ReplayEntry> = serde_json::from_str(&raw).with_context(|| {
        format!(
            "replay bundle `{}` was not valid JSON array of entries",
            path.display()
        )
    })?;
    for entry in &entries {
        entry.validate()?;
    }
    Ok(entries)
}

fn pick_baseline(
    expected: Option<Value>,
    responses: &[ReplayResponse],
) -> Option<(Value, BaselineSource)> {
    if let Some(value) = expected {
        return Some((value, BaselineSource::Expected));
    }
    responses.iter().find_map(|response| {
        if let ReplayState::Success(value) = &response.state {
            Some((
                value.clone(),
                BaselineSource::Provider(response.provider.clone()),
            ))
        } else {
            None
        }
    })
}

#[derive(Debug)]
enum BaselineSource {
    Expected,
    Provider(String),
}

impl BaselineSource {
    fn describe(&self) -> String {
        match self {
            BaselineSource::Expected => "bundle expected response".to_string(),
            BaselineSource::Provider(name) => format!("provider `{}`", name),
        }
    }
}

fn format_latency(duration: Duration) -> String {
    format!("{:.1}ms", duration.as_secs_f64() * 1000.0)
}

fn body_snippet(bytes: &Bytes) -> String {
    let text = String::from_utf8_lossy(bytes);
    truncate(&text, 120)
}

fn summarize_json(value: &Value) -> String {
    match serde_json::to_string(value) {
        Ok(text) => truncate(&text, 160),
        Err(_) => "<non-serializable>".to_string(),
    }
}

fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        text.to_string()
    } else {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &text[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pick_baseline_prefers_expected() {
        let responses = vec![ReplayResponse {
            provider: "alpha".into(),
            latency: Duration::from_millis(1),
            state: ReplayState::Success(Value::from(1)),
        }];
        let expected = Some(Value::from(42));
        let baseline = pick_baseline(expected, &responses).unwrap();
        assert_eq!(baseline.0, Value::from(42));
        assert!(matches!(baseline.1, BaselineSource::Expected));
    }

    #[test]
    fn pick_baseline_falls_back_to_first_provider() {
        let responses = vec![
            ReplayResponse {
                provider: "alpha".into(),
                latency: Duration::from_millis(1),
                state: ReplayState::Error("boom".into()),
            },
            ReplayResponse {
                provider: "beta".into(),
                latency: Duration::from_millis(2),
                state: ReplayState::Success(Value::from(7)),
            },
        ];
        let baseline = pick_baseline(None, &responses).unwrap();
        assert_eq!(baseline.0, Value::from(7));
        match baseline.1 {
            BaselineSource::Provider(name) => assert_eq!(name, "beta"),
            _ => panic!("expected provider baseline"),
        }
    }

    #[test]
    fn entry_label_defaults_to_index() {
        let entry = ReplayEntry {
            id: None,
            request: json!({"jsonrpc":"2.0","id":1}),
            expected: None,
            description: None,
        };
        assert_eq!(entry.label(4), "entry-5");
    }
}
