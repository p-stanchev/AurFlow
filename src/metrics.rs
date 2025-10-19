use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
    Registry as PromRegistry, TextEncoder,
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::registry::{Provider, Registry};

static TEXT_ENCODER: Lazy<TextEncoder> = Lazy::new(TextEncoder::new);

const EMA_ALPHA: f64 = 0.2;
const FALLBACK_LATENCY_MS: f64 = 400.0;
const FAILURE_HEALTH_THRESHOLD: u32 = 3;
const QUARANTINE_BASE_SECS: u64 = 15;
const QUARANTINE_MAX_SECS: u64 = 300;

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    request_counter: IntCounterVec,
    request_failures: IntCounterVec,
    retries: IntCounterVec,
    request_duration: HistogramVec,
    provider_latency: GaugeVec,
    provider_health: IntGaugeVec,
    provider_slot: GaugeVec,
    provider_errors: IntCounterVec,
    hedges: IntCounterVec,
    prometheus_registry: PromRegistry,
    state: Arc<RwLock<HashMap<String, ProviderState>>>,
    round_robin_cursor: Arc<AtomicUsize>,
    slot_lag_penalty_ms: f64,
}

#[derive(Clone, Debug)]
struct ProviderState {
    latency_ema_ms: f64,
    last_latency_ms: Option<f64>,
    success_count: u64,
    error_count: u64,
    consecutive_failures: u32,
    healthy: bool,
    last_slot: Option<u64>,
    last_updated: SystemTime,
    quarantined_until: Option<SystemTime>,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            latency_ema_ms: FALLBACK_LATENCY_MS,
            last_latency_ms: None,
            success_count: 0,
            error_count: 0,
            consecutive_failures: 0,
            healthy: true,
            last_slot: None,
            last_updated: SystemTime::UNIX_EPOCH,
            quarantined_until: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardProvider {
    pub name: String,
    pub latency: Option<f64>,
    pub latency_ema: f64,
    pub success: u64,
    pub errors: u64,
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub last_slot: Option<u64>,
    pub slots_behind: Option<u64>,
    pub last_updated_ms: u64,
    pub weight: u16,
    pub score: f64,
    pub raw_score: f64,
    pub quarantined: bool,
    pub quarantined_until_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardSnapshot {
    pub providers: Vec<DashboardProvider>,
    pub updated_at: u64,
    pub best_slot: Option<u64>,
}

impl Metrics {
    pub fn new(registry: Registry, config: &Config) -> Result<Self> {
        let prometheus_registry = PromRegistry::new_custom(Some("orlb".into()), None)?;

        let request_counter = IntCounterVec::new(
            Opts::new("requests_total", "Total JSON-RPC requests handled"),
            &["provider", "method", "status"],
        )?;
        let request_failures = IntCounterVec::new(
            Opts::new("request_failures_total", "Total upstream request failures"),
            &["provider", "reason"],
        )?;
        let retries = IntCounterVec::new(
            Opts::new("retries_total", "Retry attempts by reason"),
            &["reason"],
        )?;
        let request_duration = HistogramVec::new(
            HistogramOpts::new("request_duration_seconds", "Request latency"),
            &["provider", "method"],
        )?;
        let provider_latency = GaugeVec::new(
            Opts::new("provider_latency_ms", "Latest measured latency"),
            &["provider"],
        )?;
        let provider_health = IntGaugeVec::new(
            Opts::new("provider_health", "Provider health status"),
            &["provider"],
        )?;
        let provider_slot = GaugeVec::new(
            Opts::new("provider_slot", "Last observed slot height"),
            &["provider"],
        )?;
        let provider_errors = IntCounterVec::new(
            Opts::new("provider_errors_total", "Total provider errors"),
            &["provider", "kind"],
        )?;
        let hedges = IntCounterVec::new(
            Opts::new("hedges_total", "Hedged request launches by reason"),
            &["reason"],
        )?;

        prometheus_registry.register(Box::new(request_counter.clone()))?;
        prometheus_registry.register(Box::new(request_failures.clone()))?;
        prometheus_registry.register(Box::new(retries.clone()))?;
        prometheus_registry.register(Box::new(request_duration.clone()))?;
        prometheus_registry.register(Box::new(provider_latency.clone()))?;
        prometheus_registry.register(Box::new(provider_health.clone()))?;
        prometheus_registry.register(Box::new(provider_slot.clone()))?;
        prometheus_registry.register(Box::new(provider_errors.clone()))?;
        prometheus_registry.register(Box::new(hedges.clone()))?;

        let mut initial_state = HashMap::new();
        for provider in registry.providers() {
            provider_health
                .with_label_values(&[provider.name.as_str()])
                .set(1);
            initial_state.insert(provider.name.clone(), ProviderState::new());
        }

        Ok(Self {
            registry,
            request_counter,
            request_failures,
            retries,
            request_duration,
            provider_latency,
            provider_health,
            provider_slot,
            provider_errors,
            hedges,
            prometheus_registry,
            state: Arc::new(RwLock::new(initial_state)),
            round_robin_cursor: Arc::new(AtomicUsize::new(0)),
            slot_lag_penalty_ms: config.slot_lag_penalty_ms.max(0.0),
        })
    }

    pub async fn record_request_success(&self, provider: &str, method: &str, latency: Duration) {
        let latency_ms = latency.as_secs_f64() * 1000.0;
        self.request_counter
            .with_label_values(&[provider, method, "ok"])
            .inc();
        self.request_duration
            .with_label_values(&[provider, method])
            .observe(latency.as_secs_f64());

        self.provider_latency
            .with_label_values(&[provider])
            .set(latency_ms);

        {
            let mut guard = self.state.write().await;
            let state = guard
                .entry(provider.to_string())
                .or_insert_with(ProviderState::new);
            state.success_count = state.success_count.saturating_add(1);
            state.consecutive_failures = 0;
            state.healthy = true;
            state.last_latency_ms = Some(latency_ms);
            state.latency_ema_ms = ema(
                state.latency_ema_ms,
                latency_ms,
                EMA_ALPHA,
                FALLBACK_LATENCY_MS,
            );
            state.quarantined_until = None;
            state.last_updated = SystemTime::now();
        }

        self.provider_health.with_label_values(&[provider]).set(1);
    }

    pub async fn record_request_failure(&self, provider: &str, method: &str, reason: &str) {
        self.request_counter
            .with_label_values(&[provider, method, "err"])
            .inc();
        self.request_failures
            .with_label_values(&[provider, reason])
            .inc();
        self.provider_errors
            .with_label_values(&[provider, reason])
            .inc();

        let now = SystemTime::now();
        let mut mark_unhealthy = false;
        {
            let mut guard = self.state.write().await;
            let state = guard
                .entry(provider.to_string())
                .or_insert_with(ProviderState::new);
            state.error_count = state.error_count.saturating_add(1);
            state.consecutive_failures = (state.consecutive_failures + 1).min(16);
            if state.consecutive_failures >= FAILURE_HEALTH_THRESHOLD {
                state.healthy = false;
                mark_unhealthy = true;
                let quarantine_until = compute_quarantine_until(state.consecutive_failures, now);
                state.quarantined_until = Some(quarantine_until);
            }
            state.last_updated = now;
        }

        if mark_unhealthy {
            self.provider_health.with_label_values(&[provider]).set(0);
        }
    }

    pub async fn record_retry(&self, reason: &str) {
        self.retries.with_label_values(&[reason]).inc();
    }

    pub async fn record_health_success(
        &self,
        provider: &str,
        latency: Duration,
        slot: Option<u64>,
    ) {
        let latency_ms = latency.as_secs_f64() * 1000.0;
        self.provider_latency
            .with_label_values(&[provider])
            .set(latency_ms);

        if let Some(slot) = slot {
            self.provider_slot
                .with_label_values(&[provider])
                .set(slot as f64);
        }

        {
            let mut guard = self.state.write().await;
            let state = guard
                .entry(provider.to_string())
                .or_insert_with(ProviderState::new);
            state.healthy = true;
            state.consecutive_failures = 0;
            state.last_latency_ms = Some(latency_ms);
            state.latency_ema_ms = ema(
                state.latency_ema_ms,
                latency_ms,
                EMA_ALPHA,
                FALLBACK_LATENCY_MS,
            );
            if slot.is_some() {
                state.last_slot = slot;
            }
            state.quarantined_until = None;
            state.last_updated = SystemTime::now();
        }

        self.provider_health.with_label_values(&[provider]).set(1);
    }

    pub async fn record_health_failure(&self, provider: &str, reason: &str) {
        self.provider_fail(provider, reason).await;
    }

    async fn provider_fail(&self, provider: &str, reason: &str) {
        self.provider_errors
            .with_label_values(&[provider, reason])
            .inc();

        let now = SystemTime::now();
        let mut mark_unhealthy = false;
        {
            let mut guard = self.state.write().await;
            let state = guard
                .entry(provider.to_string())
                .or_insert_with(ProviderState::new);
            state.error_count = state.error_count.saturating_add(1);
            state.consecutive_failures = (state.consecutive_failures + 1).min(16);
            if state.consecutive_failures >= FAILURE_HEALTH_THRESHOLD {
                state.healthy = false;
                mark_unhealthy = true;
                let quarantine_until = compute_quarantine_until(state.consecutive_failures, now);
                state.quarantined_until = Some(quarantine_until);
            }
            state.last_updated = now;
        }

        if mark_unhealthy {
            self.provider_health.with_label_values(&[provider]).set(0);
        }
    }

    pub async fn provider_ranked_list(&self) -> Vec<(Provider, f64, bool)> {
        let state = self.state.read().await;
        let providers = self.registry.providers();
        if providers.is_empty() {
            return Vec::new();
        }
        let total = providers.len();
        let rr_seed = self.round_robin_cursor.fetch_add(1, AtomicOrdering::SeqCst) % total;

        let best_slot = state
            .values()
            .filter_map(|s| s.last_slot)
            .max()
            .unwrap_or(0);
        let now = SystemTime::now();
        let mut scored: Vec<_> = providers
            .iter()
            .enumerate()
            .map(|(idx, provider)| {
                let snapshot = state.get(&provider.name);
                let (healthy, raw_score, quarantined_until) = match snapshot {
                    Some(s) => {
                        let slots_behind = s.last_slot.map(|slot| best_slot.saturating_sub(slot));
                        let slot_penalty =
                            compute_slot_penalty(slots_behind, self.slot_lag_penalty_ms);
                        (
                            s.healthy,
                            compute_score(
                                s.latency_ema_ms,
                                s.consecutive_failures,
                                s.error_count,
                                slot_penalty,
                            ),
                            s.quarantined_until,
                        )
                    }
                    None => (
                        true,
                        compute_score(
                            FALLBACK_LATENCY_MS,
                            0,
                            0,
                            compute_slot_penalty(None, self.slot_lag_penalty_ms),
                        ),
                        None,
                    ),
                };

                let weight = provider.weight.max(1) as f64;
                let weighted_score = raw_score / weight;
                let quarantined = matches!(quarantined_until, Some(deadline) if deadline > now);
                let effective_healthy = healthy && !quarantined;

                (
                    idx,
                    provider.clone(),
                    ProviderPriority {
                        healthy: effective_healthy,
                        weighted_score,
                    },
                )
            })
            .collect();

        scored.sort_by(|(idx_a, _, a_prio), (idx_b, _, b_prio)| {
            a_prio.cmp(b_prio).then_with(|| {
                let pos_a = (idx_a + rr_seed) % total;
                let pos_b = (idx_b + rr_seed) % total;
                pos_a.cmp(&pos_b)
            })
        });

        scored
            .into_iter()
            .map(|(_, provider, priority)| (provider, priority.weighted_score, priority.healthy))
            .collect()
    }

    pub async fn dashboard_snapshot(&self) -> DashboardSnapshot {
        let state = self.state.read().await;
        let now = SystemTime::now();
        let best_slot_value = state
            .values()
            .filter_map(|s| s.last_slot)
            .max()
            .unwrap_or(0);
        let mut providers: Vec<_> = self
            .registry
            .providers()
            .iter()
            .map(|provider| {
                let snapshot = state.get(&provider.name);
                let (
                    latency,
                    ema,
                    success,
                    errors,
                    healthy,
                    failures,
                    slot,
                    updated,
                    quarantined_until,
                ) = snapshot
                    .map(|s| {
                        (
                            s.last_latency_ms,
                            s.latency_ema_ms,
                            s.success_count,
                            s.error_count,
                            s.healthy,
                            s.consecutive_failures,
                            s.last_slot,
                            s.last_updated,
                            s.quarantined_until,
                        )
                    })
                    .unwrap_or((
                        None,
                        FALLBACK_LATENCY_MS,
                        0,
                        0,
                        true,
                        0,
                        None,
                        SystemTime::UNIX_EPOCH,
                        None,
                    ));
                let slots_behind = slot.map(|value| best_slot_value.saturating_sub(value));
                let slot_penalty = compute_slot_penalty(slots_behind, self.slot_lag_penalty_ms);
                let raw_score = compute_score(ema, failures, errors, slot_penalty);
                let weighted_score = raw_score / provider.weight.max(1) as f64;
                let quarantined = matches!(quarantined_until, Some(deadline) if deadline > now);
                let healthy_display = healthy && !quarantined;

                DashboardProvider {
                    name: provider.name.clone(),
                    latency,
                    latency_ema: ema,
                    success,
                    errors,
                    healthy: healthy_display,
                    consecutive_failures: failures,
                    last_slot: slot,
                    slots_behind,
                    last_updated_ms: system_time_to_millis(updated),
                    weight: provider.weight,
                    score: weighted_score,
                    raw_score,
                    quarantined,
                    quarantined_until_ms: quarantined_until.map(system_time_to_millis),
                }
            })
            .collect();

        providers.sort_by(|a, b| match (a.healthy, b.healthy) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal),
        });

        DashboardSnapshot {
            providers,
            updated_at: system_time_to_millis(now),
            best_slot: if best_slot_value > 0 {
                Some(best_slot_value)
            } else {
                None
            },
        }
    }

    pub fn encode_prometheus(&self) -> Result<String> {
        let metric_families = self.prometheus_registry.gather();
        let mut buffer = Vec::new();
        TEXT_ENCODER.encode(&metric_families, &mut buffer)?;
        let output = String::from_utf8(buffer)?;
        Ok(output)
    }

    pub fn record_hedge(&self, reason: &str) {
        self.hedges.with_label_values(&[reason]).inc();
    }
}

#[derive(Clone, Copy)]
struct ProviderPriority {
    healthy: bool,
    weighted_score: f64,
}

impl PartialEq for ProviderPriority {
    fn eq(&self, other: &Self) -> bool {
        self.weighted_score == other.weighted_score && self.healthy == other.healthy
    }
}

impl Eq for ProviderPriority {}

impl PartialOrd for ProviderPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProviderPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.healthy, other.healthy) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => self
                .weighted_score
                .partial_cmp(&other.weighted_score)
                .unwrap_or(std::cmp::Ordering::Equal),
        }
    }
}

fn compute_slot_penalty(slots_behind: Option<u64>, penalty_per_slot: f64) -> f64 {
    if penalty_per_slot <= 0.0 {
        return 0.0;
    }
    match slots_behind {
        Some(0) => 0.0,
        Some(lag) => lag as f64 * penalty_per_slot,
        None => penalty_per_slot * 4.0,
    }
}

fn compute_quarantine_until(consecutive_failures: u32, now: SystemTime) -> SystemTime {
    let exponent = consecutive_failures.saturating_sub(FAILURE_HEALTH_THRESHOLD) as u32;
    let multiplier = 1u64 << exponent.min(4);
    let backoff_secs = QUARANTINE_BASE_SECS
        .saturating_mul(multiplier)
        .min(QUARANTINE_MAX_SECS);
    now + Duration::from_secs(backoff_secs)
}

fn compute_score(
    latency_ema: f64,
    consecutive_failures: u32,
    error_count: u64,
    slot_penalty: f64,
) -> f64 {
    let failure_penalty = consecutive_failures as f64 * 200.0;
    let error_penalty = (error_count as f64).sqrt() * 40.0;
    latency_ema + failure_penalty + error_penalty + slot_penalty
}

fn ema(current: f64, sample: f64, alpha: f64, default_value: f64) -> f64 {
    let base = if current <= 0.0 {
        default_value
    } else {
        current
    };
    (1.0 - alpha) * base + alpha * sample
}

fn system_time_to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{Provider, Registry};
    use std::path::PathBuf;

    fn provider(name: &str, url: &str) -> Provider {
        Provider {
            name: name.to_string(),
            url: url.to_string(),
            weight: 1,
            headers: None,
        }
    }

    fn test_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            providers_path: PathBuf::from("providers.json"),
            probe_interval: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            retry_read_requests: true,
            dashboard_assets_dir: None,
            slot_lag_penalty_ms: 10.0,
            slot_lag_alert_slots: 50,
            hedge_requests: false,
            hedge_delay: Duration::from_millis(60),
        }
    }

    #[tokio::test]
    async fn fresher_provider_ranks_first() {
        let registry = Registry::from_providers(vec![
            provider("Fresh", "https://fresh.example.com"),
            provider("Stale", "https://stale.example.com"),
        ])
        .unwrap();
        let config = test_config();
        let metrics = Metrics::new(registry, &config).unwrap();

        metrics
            .record_health_success("Fresh", Duration::from_millis(100), Some(2_000))
            .await;
        metrics
            .record_health_success("Stale", Duration::from_millis(80), Some(1_900))
            .await;

        let ranked = metrics.provider_ranked_list().await;
        assert_eq!(ranked.first().unwrap().0.name, "Fresh");
        assert_eq!(ranked.last().unwrap().0.name, "Stale");

        let snapshot = metrics.dashboard_snapshot().await;
        assert_eq!(snapshot.best_slot, Some(2_000));
        let stale_card = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Stale")
            .expect("stale provider present");
        assert_eq!(stale_card.slots_behind, Some(100));
        let fresh_card = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Fresh")
            .expect("fresh provider present");
        assert!(stale_card.score > fresh_card.score);
    }
}
