use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
    Registry as PromRegistry, TextEncoder,
};
use serde::Serialize;
use tokio::sync::{Mutex, RwLock};

use crate::commitment::Commitment;
use crate::config::Config;
use crate::registry::{Provider, Registry};

static TEXT_ENCODER: Lazy<TextEncoder> = Lazy::new(TextEncoder::new);

const EMA_ALPHA: f64 = 0.2;
const FALLBACK_LATENCY_MS: f64 = 400.0;
const FAILURE_HEALTH_THRESHOLD: u32 = 3;
const QUARANTINE_BASE_SECS: u64 = 15;
const QUARANTINE_MAX_SECS: u64 = 300;
const SLO_BUCKET_WIDTH: Duration = Duration::from_secs(10);
const SLO_WINDOWS: &[(&str, Duration)] = &[
    ("5m", Duration::from_secs(300)),
    ("30m", Duration::from_secs(1800)),
    ("2h", Duration::from_secs(7200)),
];
const MULTI_CLUSTER_SPREAD_MULTIPLIER: u64 = 1000;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommitmentSlots {
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

impl CommitmentSlots {
    pub(crate) fn set(&mut self, commitment: Commitment, slot: Option<u64>) {
        match commitment {
            Commitment::Processed => self.processed = slot,
            Commitment::Confirmed => self.confirmed = slot,
            Commitment::Finalized => self.finalized = slot,
        }
    }

    pub(crate) fn get(&self, commitment: Commitment) -> Option<u64> {
        match commitment {
            Commitment::Processed => self.processed,
            Commitment::Confirmed => self.confirmed,
            Commitment::Finalized => self.finalized,
        }
    }

    pub(crate) fn from_slot(commitment: Commitment, slot: Option<u64>) -> Self {
        let mut slots = Self::default();
        slots.set(commitment, slot);
        slots
    }
}

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
    slo_availability: GaugeVec,
    slo_burn_rate: GaugeVec,
    slo_window_requests: GaugeVec,
    slo_window_errors: GaugeVec,
    slo_provider_availability: GaugeVec,
    slo_provider_error_rate: GaugeVec,
    tag_weights: HashMap<String, f64>,
    prometheus_registry: PromRegistry,
    state: Arc<RwLock<HashMap<String, ProviderState>>>,
    round_robin_cursor: Arc<AtomicUsize>,
    slot_lag_penalty_ms: f64,
    slo_tracker: Arc<Mutex<SloTracker>>,
    provider_slo_trackers: Arc<Mutex<HashMap<String, SloTracker>>>,
    slo_target: f64,
    slot_lag_alert_slots: u64,
}

#[derive(Clone, Debug)]
struct ProviderState {
    latency_ema_ms: f64,
    last_latency_ms: Option<f64>,
    request_success_count: u64,
    request_error_count: u64,
    probe_success_count: u64,
    probe_error_count: u64,
    consecutive_failures: u32,
    healthy: bool,
    slots: CommitmentSlots,
    last_updated: SystemTime,
    quarantined_until: Option<SystemTime>,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            latency_ema_ms: FALLBACK_LATENCY_MS,
            last_latency_ms: None,
            request_success_count: 0,
            request_error_count: 0,
            probe_success_count: 0,
            probe_error_count: 0,
            consecutive_failures: 0,
            healthy: true,
            slots: CommitmentSlots::default(),
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
    pub probe_success: u64,
    pub probe_errors: u64,
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub last_slot: Option<u64>,
    pub slots_behind: Option<u64>,
    pub commitments: Vec<DashboardCommitmentSlot>,
    pub last_updated_ms: u64,
    pub weight: u16,
    pub effective_weight: f64,
    pub tag_multiplier: f64,
    pub tags: Vec<String>,
    pub score: f64,
    pub raw_score: f64,
    pub quarantined: bool,
    pub quarantined_until_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardCommitmentSlot {
    pub commitment: String,
    pub slot: Option<u64>,
    pub slots_behind: Option<u64>,
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
            &["provider", "commitment"],
        )?;
        let provider_errors = IntCounterVec::new(
            Opts::new("provider_errors_total", "Total provider errors"),
            &["provider", "kind"],
        )?;
        let hedges = IntCounterVec::new(
            Opts::new("hedges_total", "Hedged request launches by reason"),
            &["reason"],
        )?;
        let slo_availability = GaugeVec::new(
            Opts::new("slo_availability_ratio", "Rolling availability ratio"),
            &["window"],
        )?;
        let slo_burn_rate = GaugeVec::new(
            Opts::new(
                "slo_error_budget_burn",
                "Error budget burn rate computed against ORLB_SLO_TARGET",
            ),
            &["window"],
        )?;
        let slo_window_requests = GaugeVec::new(
            Opts::new(
                "slo_window_requests",
                "Total requests observed within the rolling window",
            ),
            &["window"],
        )?;
        let slo_window_errors = GaugeVec::new(
            Opts::new(
                "slo_window_errors",
                "Total errors observed within the rolling window",
            ),
            &["window"],
        )?;
        let slo_provider_availability = GaugeVec::new(
            Opts::new(
                "provider_slo_availability_ratio",
                "Provider availability ratio computed over rolling windows",
            ),
            &["provider", "window"],
        )?;
        let slo_provider_error_rate = GaugeVec::new(
            Opts::new(
                "provider_slo_error_ratio",
                "Provider error ratio computed over rolling windows",
            ),
            &["provider", "window"],
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
        prometheus_registry.register(Box::new(slo_availability.clone()))?;
        prometheus_registry.register(Box::new(slo_burn_rate.clone()))?;
        prometheus_registry.register(Box::new(slo_window_requests.clone()))?;
        prometheus_registry.register(Box::new(slo_window_errors.clone()))?;
        prometheus_registry.register(Box::new(slo_provider_availability.clone()))?;
        prometheus_registry.register(Box::new(slo_provider_error_rate.clone()))?;

        let mut initial_state = HashMap::new();
        for provider in registry.providers() {
            provider_health
                .with_label_values(&[provider.name.as_str()])
                .set(1);
            initial_state.insert(provider.name.clone(), ProviderState::new());
            for (label, _) in SLO_WINDOWS {
                slo_provider_availability
                    .with_label_values(&[provider.name.as_str(), *label])
                    .set(1.0);
                slo_provider_error_rate
                    .with_label_values(&[provider.name.as_str(), *label])
                    .set(0.0);
            }
        }
        for (label, _) in SLO_WINDOWS {
            slo_availability.with_label_values(&[*label]).set(1.0);
            slo_burn_rate.with_label_values(&[*label]).set(0.0);
            slo_window_requests.with_label_values(&[*label]).set(0.0);
            slo_window_errors.with_label_values(&[*label]).set(0.0);
        }

        let slo_tracker = Arc::new(Mutex::new(SloTracker::new(config.slo_target)));

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
            slo_availability,
            slo_burn_rate,
            slo_window_requests,
            slo_window_errors,
            slo_provider_availability,
            slo_provider_error_rate,
            tag_weights: config.tag_weights.clone(),
            prometheus_registry,
            state: Arc::new(RwLock::new(initial_state)),
            round_robin_cursor: Arc::new(AtomicUsize::new(0)),
            slot_lag_penalty_ms: config.slot_lag_penalty_ms.max(0.0),
            slo_tracker,
            provider_slo_trackers: Arc::new(Mutex::new(HashMap::new())),
            slo_target: config.slo_target,
            slot_lag_alert_slots: config.slot_lag_alert_slots,
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
            state.request_success_count = state.request_success_count.saturating_add(1);
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

        self.update_slo(provider, SloOutcome::Success).await;
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
            state.request_error_count = state.request_error_count.saturating_add(1);
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

        self.update_slo(provider, SloOutcome::Failure).await;
    }

    async fn update_slo(&self, provider: &str, outcome: SloOutcome) {
        let snapshots = {
            let mut tracker = self.slo_tracker.lock().await;
            tracker.record(outcome)
        };
        for snapshot in snapshots {
            let window = snapshot.window.as_str();
            self.slo_availability
                .with_label_values(&[window])
                .set(snapshot.availability);
            self.slo_burn_rate
                .with_label_values(&[window])
                .set(snapshot.burn_rate);
            self.slo_window_requests
                .with_label_values(&[window])
                .set(snapshot.total as f64);
            self.slo_window_errors
                .with_label_values(&[window])
                .set(snapshot.errors as f64);
        }

        let provider_snapshots = {
            let mut trackers = self.provider_slo_trackers.lock().await;
            let tracker = trackers
                .entry(provider.to_string())
                .or_insert_with(|| SloTracker::new(self.slo_target));
            tracker.record(outcome)
        };
        for snapshot in provider_snapshots {
            let window = snapshot.window.as_str();
            let labels = [provider, window];
            self.slo_provider_availability
                .with_label_values(&labels)
                .set(snapshot.availability);
            let error_ratio = if snapshot.total == 0 {
                0.0
            } else {
                snapshot.errors as f64 / snapshot.total as f64
            };
            self.slo_provider_error_rate
                .with_label_values(&labels)
                .set(error_ratio);
        }
    }

    pub async fn record_retry(&self, reason: &str) {
        self.retries.with_label_values(&[reason]).inc();
    }

    pub async fn record_health_success(
        &self,
        provider: &str,
        latency: Duration,
        slots: CommitmentSlots,
    ) {
        let latency_ms = latency.as_secs_f64() * 1000.0;
        self.provider_latency
            .with_label_values(&[provider])
            .set(latency_ms);

        for commitment in Commitment::ALL {
            if let Some(slot) = slots.get(commitment) {
                self.provider_slot
                    .with_label_values(&[provider, commitment.as_str()])
                    .set(slot as f64);
            }
        }

        {
            let mut guard = self.state.write().await;
            let state = guard
                .entry(provider.to_string())
                .or_insert_with(ProviderState::new);
            state.probe_success_count = state.probe_success_count.saturating_add(1);
            state.healthy = true;
            state.consecutive_failures = 0;
            state.last_latency_ms = Some(latency_ms);
            state.latency_ema_ms = ema(
                state.latency_ema_ms,
                latency_ms,
                EMA_ALPHA,
                FALLBACK_LATENCY_MS,
            );
            for commitment in Commitment::ALL {
                if let Some(slot) = slots.get(commitment) {
                    state.slots.set(commitment, Some(slot));
                }
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
            state.probe_error_count = state.probe_error_count.saturating_add(1);
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

    pub async fn provider_ranked_list(&self, commitment: Commitment) -> Vec<(Provider, f64, bool)> {
        let state = self.state.read().await;
        let providers = self.registry.providers();
        if providers.is_empty() {
            return Vec::new();
        }
        let total = providers.len();
        let rr_seed = self.round_robin_cursor.fetch_add(1, AtomicOrdering::SeqCst) % total;

        let slot_samples: Vec<u64> = state
            .values()
            .filter_map(|s| s.slots.get(commitment))
            .collect();
        let best_slot = slot_samples.iter().copied().max().unwrap_or(0);
        let min_slot = slot_samples.iter().copied().min().unwrap_or(best_slot);
        let alert_base = self.slot_lag_alert_slots.max(1);
        let skew_threshold = alert_base.saturating_mul(MULTI_CLUSTER_SPREAD_MULTIPLIER);
        let cluster_skew = best_slot.saturating_sub(min_slot) > skew_threshold;
        let now = SystemTime::now();
        let mut scored: Vec<_> = providers
            .iter()
            .enumerate()
            .map(|(idx, provider)| {
                let snapshot = state.get(&provider.name);
                let (healthy, raw_score, quarantined_until) = match snapshot {
                    Some(s) => {
                        let slot_opt = s.slots.get(commitment);
                        let slots_behind = slot_opt.map(|slot| best_slot.saturating_sub(slot));
                        let slot_penalty = if cluster_skew {
                            0.0
                        } else {
                            compute_slot_penalty(slots_behind, self.slot_lag_penalty_ms)
                        };
                        let healthy_flag = s.healthy && (best_slot == 0 || slot_opt.is_some());
                        (
                            healthy_flag,
                            compute_score(
                                s.latency_ema_ms,
                                s.consecutive_failures,
                                s.request_error_count.saturating_add(s.probe_error_count),
                                slot_penalty,
                            ),
                            s.quarantined_until,
                        )
                    }
                    None => (
                        best_slot == 0,
                        compute_score(
                            FALLBACK_LATENCY_MS,
                            0,
                            0,
                            if cluster_skew {
                                0.0
                            } else {
                                compute_slot_penalty(None, self.slot_lag_penalty_ms)
                            },
                        ),
                        None,
                    ),
                };

                let effective_weight = self.effective_weight(provider);
                let weighted_score = raw_score / effective_weight;
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

        let best_slots: HashMap<Commitment, u64> = Commitment::ALL
            .iter()
            .copied()
            .map(|commitment| {
                let best = state
                    .values()
                    .filter_map(|s| s.slots.get(commitment))
                    .max()
                    .unwrap_or(0);
                (commitment, best)
            })
            .collect();

        let best_finalized = *best_slots.get(&Commitment::Finalized).unwrap_or(&0);

        let mut providers: Vec<_> = self
            .registry
            .providers()
            .iter()
            .map(|provider| {
                let snapshot = state.get(&provider.name);
                let (
                    latency,
                    ema,
                    request_success,
                    request_errors,
                    probe_success,
                    probe_errors,
                    healthy,
                    failures,
                    slots,
                    updated,
                    quarantined_until,
                ) = snapshot
                    .map(|s| {
                        (
                            s.last_latency_ms,
                            s.latency_ema_ms,
                            s.request_success_count,
                            s.request_error_count,
                            s.probe_success_count,
                            s.probe_error_count,
                            s.healthy,
                            s.consecutive_failures,
                            s.slots,
                            s.last_updated,
                            s.quarantined_until,
                        )
                    })
                    .unwrap_or((
                        None,
                        FALLBACK_LATENCY_MS,
                        0,
                        0,
                        0,
                        0,
                        true,
                        0,
                        CommitmentSlots::default(),
                        SystemTime::UNIX_EPOCH,
                        None,
                    ));

                let finalized_slot = slots.get(Commitment::Finalized);
                let slots_behind = finalized_slot.and_then(|value| {
                    if best_finalized == 0 {
                        None
                    } else {
                        Some(best_finalized.saturating_sub(value))
                    }
                });
                let slot_penalty = compute_slot_penalty(slots_behind, self.slot_lag_penalty_ms);
                let total_errors = request_errors.saturating_add(probe_errors);
                let raw_score = compute_score(ema, failures, total_errors, slot_penalty);
                let tag_multiplier = self.tag_multiplier(provider);
                let effective_weight = self.effective_weight(provider);
                let weighted_score = raw_score / effective_weight;
                let quarantined = matches!(quarantined_until, Some(deadline) if deadline > now);
                let healthy_display = healthy && !quarantined;

                let commitments: Vec<DashboardCommitmentSlot> = Commitment::ALL
                    .iter()
                    .map(|commitment| {
                        let slot = slots.get(*commitment);
                        let best_for = *best_slots.get(commitment).unwrap_or(&0);
                        let slots_behind = slot.and_then(|value| {
                            if best_for == 0 {
                                None
                            } else {
                                Some(best_for.saturating_sub(value))
                            }
                        });
                        DashboardCommitmentSlot {
                            commitment: commitment.as_str().to_string(),
                            slot,
                            slots_behind,
                        }
                    })
                    .collect();

                DashboardProvider {
                    name: provider.name.clone(),
                    latency,
                    latency_ema: ema,
                    success: request_success,
                    errors: request_errors,
                    probe_success,
                    probe_errors,
                    healthy: healthy_display,
                    consecutive_failures: failures,
                    last_slot: finalized_slot,
                    slots_behind,
                    commitments,
                    last_updated_ms: system_time_to_millis(updated),
                    weight: provider.weight,
                    effective_weight,
                    tag_multiplier,
                    tags: provider.tags.clone(),
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
            best_slot: if best_finalized > 0 {
                Some(best_finalized)
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

    fn tag_multiplier(&self, provider: &Provider) -> f64 {
        let mut multiplier = 1.0f64;
        for tag in &provider.tags {
            if let Some(weight) = self.tag_weights.get(tag) {
                multiplier = multiplier.max(*weight);
            }
        }
        multiplier
    }

    fn effective_weight(&self, provider: &Provider) -> f64 {
        let base = provider.weight.max(1) as f64;
        base * self.tag_multiplier(provider)
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

#[derive(Clone, Copy)]
enum SloOutcome {
    Success,
    Failure,
}

struct SloTracker {
    target: f64,
    windows: Vec<SloWindow>,
}

impl SloTracker {
    fn new(target: f64) -> Self {
        let clamped_target = target.clamp(0.0, 0.999_999);
        let windows = SLO_WINDOWS
            .iter()
            .map(|(label, duration)| SloWindow::new(label, *duration, SLO_BUCKET_WIDTH))
            .collect();
        Self {
            target: clamped_target,
            windows,
        }
    }

    fn record(&mut self, outcome: SloOutcome) -> Vec<SloSnapshot> {
        let now = Instant::now();
        let mut snapshots = Vec::with_capacity(self.windows.len());

        for window in &mut self.windows {
            window.record(outcome, now);
            let total = window.total();
            let errors = window.errors();
            let availability = if total == 0 {
                1.0
            } else {
                ((total - errors) as f64 / total as f64).clamp(0.0, 1.0)
            };
            let error_rate = (1.0 - availability).clamp(0.0, 1.0);
            let error_budget = (1.0 - self.target).max(1e-6);
            let burn_rate = error_rate / error_budget;

            snapshots.push(SloSnapshot {
                window: window.label.clone(),
                availability,
                burn_rate,
                total,
                errors,
            });
        }

        snapshots
    }
}

struct SloSnapshot {
    window: String,
    availability: f64,
    burn_rate: f64,
    total: u64,
    errors: u64,
}

struct SloWindow {
    label: String,
    duration: Duration,
    bucket_width: Duration,
    epoch: Instant,
    buckets: VecDeque<Bucket>,
    total_success: u64,
    total_failure: u64,
}

impl SloWindow {
    fn new(label: &str, duration: Duration, bucket_width: Duration) -> Self {
        Self {
            label: label.to_string(),
            duration,
            bucket_width,
            epoch: Instant::now(),
            buckets: VecDeque::new(),
            total_success: 0,
            total_failure: 0,
        }
    }

    fn record(&mut self, outcome: SloOutcome, now: Instant) {
        let bucket_index = self.bucket_index(now);
        self.prune(bucket_index);

        if self
            .buckets
            .back()
            .map(|bucket| bucket.index != bucket_index)
            .unwrap_or(true)
        {
            self.buckets.push_back(Bucket {
                index: bucket_index,
                success: 0,
                failure: 0,
            });
        }

        if let Some(bucket) = self.buckets.back_mut() {
            match outcome {
                SloOutcome::Success => {
                    bucket.success = bucket.success.saturating_add(1);
                    self.total_success = self.total_success.saturating_add(1);
                }
                SloOutcome::Failure => {
                    bucket.failure = bucket.failure.saturating_add(1);
                    self.total_failure = self.total_failure.saturating_add(1);
                }
            }
        }
    }

    fn prune(&mut self, current_index: u64) {
        let capacity = self.window_capacity();
        let min_index = current_index.saturating_sub(capacity);
        while let Some(front) = self.buckets.front() {
            if front.index < min_index {
                if let Some(bucket) = self.buckets.pop_front() {
                    self.total_success = self.total_success.saturating_sub(bucket.success);
                    self.total_failure = self.total_failure.saturating_sub(bucket.failure);
                }
            } else {
                break;
            }
        }
    }

    fn window_capacity(&self) -> u64 {
        let bucket_secs = self.bucket_width.as_secs().max(1);
        (self.duration.as_secs() + bucket_secs - 1) / bucket_secs
    }

    fn bucket_index(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.epoch);
        let bucket_secs = self.bucket_width.as_secs().max(1);
        elapsed.as_secs() / bucket_secs
    }

    fn total(&self) -> u64 {
        self.total_success + self.total_failure
    }

    fn errors(&self) -> u64 {
        self.total_failure
    }
}

struct Bucket {
    index: u64,
    success: u64,
    failure: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, OtelConfig, OtelExporter};
    use crate::registry::{Provider, Registry};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn provider(name: &str, url: &str) -> Provider {
        Provider {
            name: name.to_string(),
            url: url.to_string(),
            weight: 1,
            headers: None,
            tags: Vec::new(),
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
            slo_target: 0.995,
            otel: OtelConfig {
                exporter: OtelExporter::None,
                service_name: "orlb-test".into(),
            },
            tag_weights: HashMap::new(),
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
            .record_health_success(
                "Fresh",
                Duration::from_millis(100),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(2_000)),
            )
            .await;
        metrics
            .record_health_success(
                "Stale",
                Duration::from_millis(80),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(1_900)),
            )
            .await;

        let ranked = metrics.provider_ranked_list(Commitment::Finalized).await;
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

    #[tokio::test]
    async fn dashboard_snapshot_tracks_request_and_probe_counts() {
        let registry =
            Registry::from_providers(vec![provider("Solo", "https://solo.example.com")]).unwrap();
        let config = test_config();
        let metrics = Metrics::new(registry, &config).unwrap();

        metrics
            .record_request_success("Solo", "getSlot", Duration::from_millis(25))
            .await;
        metrics
            .record_request_failure("Solo", "getSlot", "status_500")
            .await;
        metrics
            .record_health_success(
                "Solo",
                Duration::from_millis(30),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(1_234)),
            )
            .await;
        metrics.record_health_failure("Solo", "timeout").await;

        let snapshot = metrics.dashboard_snapshot().await;
        let card = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Solo")
            .expect("provider present");

        assert_eq!(card.success, 1);
        assert_eq!(card.errors, 1);
        assert_eq!(card.probe_success, 1);
        assert_eq!(card.probe_errors, 1);
        assert_eq!(card.commitments.len(), Commitment::ALL.len());
        assert!(card.tags.is_empty());
        assert_eq!(card.tag_multiplier, 1.0);
        assert_eq!(card.effective_weight, card.weight as f64);
    }

    #[tokio::test]
    async fn slot_penalty_disabled_with_large_spread() {
        let registry = Registry::from_providers(vec![
            provider("Main", "https://main.example.com"),
            provider("Alt", "https://alt.example.com"),
        ])
        .unwrap();
        let config = test_config();
        let metrics = Metrics::new(registry, &config).unwrap();

        metrics
            .record_health_success(
                "Main",
                Duration::from_millis(100),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(418_000_000)),
            )
            .await;
        metrics
            .record_health_success(
                "Alt",
                Duration::from_millis(100),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(2_000_000)),
            )
            .await;

        let ranked = metrics.provider_ranked_list(Commitment::Finalized).await;
        let scores: HashMap<_, _> = ranked
            .into_iter()
            .map(|(provider, score, _)| (provider.name, score))
            .collect();

        let diff = (scores["Main"] - scores["Alt"]).abs();
        assert!(
            diff < 1.0,
            "score delta should be negligible when spread is large, got {diff}"
        );
    }

    #[tokio::test]
    async fn tag_multiplier_adjusts_effective_weight() {
        let mut config = test_config();
        config.tag_weights.insert("paid".into(), 2.5);
        config.tag_weights.insert("premium".into(), 1.4);

        let registry = Registry::from_providers(vec![
            Provider {
                name: "Paid".into(),
                url: "https://paid.example.com".into(),
                weight: 1,
                headers: None,
                tags: vec!["paid".into()],
            },
            Provider {
                name: "Public".into(),
                url: "https://public.example.com".into(),
                weight: 1,
                headers: None,
                tags: vec!["public".into()],
            },
        ])
        .unwrap();
        let metrics = Metrics::new(registry, &config).unwrap();

        let slots = CommitmentSlots::from_slot(Commitment::Finalized, Some(1_500));
        metrics
            .record_health_success("Paid", Duration::from_millis(40), slots)
            .await;
        metrics
            .record_health_success(
                "Public",
                Duration::from_millis(40),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(1_500)),
            )
            .await;

        let snapshot = metrics.dashboard_snapshot().await;
        let paid = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Paid")
            .expect("paid provider present");
        assert_eq!(paid.tag_multiplier, 2.5);
        assert!((paid.effective_weight - 2.5).abs() < f64::EPSILON);

        let public = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Public")
            .expect("public provider present");
        assert_eq!(public.tag_multiplier, 1.0);
        assert_eq!(public.effective_weight, public.weight as f64);
    }
}
