use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use hyper::body::to_bytes;
use hyper::header::{HeaderName, HeaderValue, ALLOW, CONTENT_TYPE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::signal;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;
use uuid::Uuid;

use crate::commitment::Commitment;
use crate::config::Config;
use crate::dashboard;
use crate::errors::OrlbError;
use crate::forward;
use crate::metrics::Metrics;
use crate::registry::{Provider, Registry, SharedRegistry};
use crate::secrets::SecretManager;
use crate::ws;

const MAX_PAYLOAD_BYTES: usize = 512 * 1024;
const DASHBOARD_PATH: &str = "/dashboard";
const METRICS_PATH: &str = "/metrics";
const METRICS_JSON_PATH: &str = "/metrics.json";
const RPC_PATH: &str = "/rpc";
const WS_PATH: &str = "/ws";
const ADMIN_PROVIDERS_PATH: &str = "/admin/providers";

#[derive(Deserialize)]
#[serde(untagged)]
enum ProviderUpdateInput {
    Envelope {
        providers: Vec<Provider>,
        #[serde(default)]
        persist: bool,
    },
    List(Vec<Provider>),
}

impl ProviderUpdateInput {
    fn into_payload(self) -> ProviderUpdatePayload {
        match self {
            ProviderUpdateInput::Envelope { providers, persist } => {
                ProviderUpdatePayload { providers, persist }
            }
            ProviderUpdateInput::List(providers) => ProviderUpdatePayload {
                providers,
                persist: false,
            },
        }
    }
}

struct ProviderUpdatePayload {
    providers: Vec<Provider>,
    persist: bool,
}

static MUTATING_METHODS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "sendtransaction",
        "sendrawtransaction",
        "signtransaction",
        "signandsendtransaction",
        "requestairdrop",
        "setcomputeunitlimit",
        "setcomputeunitprice",
        "setlogfilter",
        "setpriorityfee",
    ]
    .into_iter()
    .collect()
});

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: Config,
    pub(crate) metrics: Metrics,
    pub(crate) client: Client,
    concurrency: ProviderConcurrency,
    pub(crate) secrets: SecretManager,
    pub(crate) registry: SharedRegistry,
}

#[derive(Clone)]
struct ProviderConcurrency {
    limits: Arc<RwLock<HashMap<String, Arc<Semaphore>>>>,
}

impl ProviderConcurrency {
    fn new(providers: &[Provider]) -> Self {
        Self {
            limits: Arc::new(RwLock::new(Self::build_map(providers))),
        }
    }

    fn build_map(providers: &[Provider]) -> HashMap<String, Arc<Semaphore>> {
        let mut limits = HashMap::with_capacity(providers.len());
        for provider in providers {
            limits.insert(
                provider.name.clone(),
                Arc::new(Semaphore::new(Self::permits_for(provider))),
            );
        }
        limits
    }

    fn permits_for(provider: &Provider) -> usize {
        let base = usize::from(provider.weight.max(1));
        (base * 4).max(1)
    }

    fn reload(&self, providers: &[Provider]) {
        let mut guard = self
            .limits
            .write()
            .expect("provider concurrency lock poisoned");
        *guard = Self::build_map(providers);
    }

    async fn acquire(&self, provider_name: &str) -> OwnedSemaphorePermit {
        let semaphore = {
            let guard = self
                .limits
                .read()
                .expect("provider concurrency lock poisoned");
            guard.get(provider_name).cloned()
        }
        .expect("missing semaphore for provider");
        semaphore.acquire_owned().await.expect("semaphore closed")
    }
}

pub async fn start_server(
    config: Config,
    registry: SharedRegistry,
    metrics: Metrics,
    client: Client,
    secrets: SecretManager,
) -> Result<()> {
    let listen_addr = config.listen_addr;
    let provider_registry = registry.snapshot();
    let concurrency = ProviderConcurrency::new(provider_registry.providers());
    let state = Arc::new(AppState {
        config,
        registry,
        metrics,
        client,
        concurrency,
        secrets,
    });

    let make_svc = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(route(req, state).await) }
            }))
        }
    });

    let server = Server::bind(&listen_addr).serve(make_svc);

    tracing::info!(%listen_addr, "ORLB listening");

    server
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(Into::into)
}

async fn route(req: Request<Body>, state: Arc<AppState>) -> Response<Body> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, RPC_PATH) => match handle_rpc(req, state).await {
            Ok(response) => response,
            Err((err, id)) => error_response(err, id),
        },
        (&Method::GET, RPC_PATH) => method_not_allowed(),
        (&Method::GET, METRICS_PATH) => handle_metrics(&state).await,
        (&Method::GET, METRICS_JSON_PATH) => handle_metrics_json(&state).await,
        (&Method::GET, WS_PATH) => ws::handle(req, state).await,
        (&Method::GET, ADMIN_PROVIDERS_PATH) => handle_admin_providers_get(&state),
        (&Method::POST, ADMIN_PROVIDERS_PATH) => handle_admin_providers_post(req, state).await,
        (&Method::GET, DASHBOARD_PATH) | (&Method::GET, "/") => {
            match dashboard::serve(&state.config) {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(error = ?err, "dashboard rendering failed");
                    internal_server_error()
                }
            }
        }
        (&Method::GET, "/aurflow.png") => match dashboard::serve_logo(&state.config) {
            Ok(response) => response,
            Err(err) => {
                tracing::error!(error = ?err, "dashboard logo rendering failed");
                internal_server_error()
            }
        },
        _ => not_found(),
    }
}

async fn handle_rpc(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, (OrlbError, Option<Value>)> {
    let request_id = Uuid::new_v4();
    let span = tracing::info_span!("rpc", %request_id);
    let _guard = span.enter();
    let mut response_id: Option<Value> = None;

    let content_length = req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    if let Some(len) = content_length {
        if len > MAX_PAYLOAD_BYTES {
            return Err((
                OrlbError::InvalidPayload(format!("payload exceeds {} bytes", MAX_PAYLOAD_BYTES)),
                None,
            ));
        }
    }

    let body: Bytes = to_bytes(req.into_body()).await.map_err(|err| {
        (
            OrlbError::InvalidPayload(format!("failed to read request body: {err}")),
            None,
        )
    })?;
    if body.len() > MAX_PAYLOAD_BYTES {
        return Err((
            OrlbError::InvalidPayload(format!("payload exceeds {} bytes", MAX_PAYLOAD_BYTES)),
            None,
        ));
    }

    let payload: Value = serde_json::from_slice(&body).map_err(|err| {
        (
            OrlbError::InvalidPayload(format!("invalid JSON payload: {err}")),
            None,
        )
    })?;

    let metadata =
        RequestMetadata::from_payload(&payload).map_err(|err| (err, response_id.clone()))?;
    response_id = metadata.id.clone();

    if metadata.contains_mutating_method {
        tracing::warn!(
            methods = ?metadata.method_names,
            "mutating method rejected"
        );
        return Err((
            OrlbError::MutatingMethod(metadata.method_names.join(", ")),
            response_id,
        ));
    }

    let required_commitment = metadata.commitment();

    let method_label = metadata.method_label();
    let ranked = state
        .metrics
        .provider_ranked_list(required_commitment)
        .await;
    let mut healthy_candidates = Vec::new();
    let mut all_candidates = Vec::new();
    for (provider, _score, healthy) in ranked {
        if healthy {
            healthy_candidates.push(provider.clone());
        }
        all_candidates.push(provider);
    }
    let mut providers: Vec<Provider> = if healthy_candidates.is_empty() {
        all_candidates
    } else {
        healthy_candidates
    };
    if providers.is_empty() {
        return Err((OrlbError::NoHealthyProviders, response_id));
    }

    if let Some(id_seed) = metadata
        .id
        .as_ref()
        .and_then(|id| request_id_seed(id, providers.len()))
    {
        if id_seed > 0 {
            providers.rotate_left(id_seed);
        }
    }

    let max_attempts = if state.config.retry_read_requests && metadata.retryable {
        2
    } else {
        1
    };
    let hedge_enabled = state.config.hedge_requests && max_attempts > 1 && providers.len() > 1;

    let mut attempts = 0usize;
    let mut last_error = None;
    let mut last_status = None;
    let mut last_provider: Option<String> = None;

    if hedge_enabled {
        let primary_provider_name = providers.first().map(|p| p.name.clone());
        let mut hedge_outcome_recorded = false;
        let mut record_outcome = |outcome: &str| {
            if !hedge_outcome_recorded {
                state.metrics.record_hedge_outcome(outcome);
                hedge_outcome_recorded = true;
            }
        };
        let mut join_set: JoinSet<(
            Provider,
            std::time::Duration,
            anyhow::Result<reqwest::Response>,
        )> = JoinSet::new();
        for (idx, provider) in providers.iter().take(max_attempts).cloned().enumerate() {
            let delay = if idx == 0 {
                std::time::Duration::from_millis(0)
            } else if state.config.adaptive_hedging && idx == 1 {
                // Calculate adaptive delay based on primary provider's latency
                let primary_provider = &providers[0];
                let adaptive_delay_ms = state
                    .metrics
                    .calculate_adaptive_hedge_delay_ms(
                        &primary_provider.name,
                        state.config.hedge_delay.as_secs_f64() * 1000.0,
                        state.config.hedge_min_delay_ms,
                        state.config.hedge_max_delay_ms,
                    )
                    .await;
                let delay_ms = adaptive_delay_ms.max(0.0).round().min(u64::MAX as f64) as u64;
                state.metrics.record_hedge("adaptive");
                std::time::Duration::from_millis(delay_ms)
            } else {
                state.config.hedge_delay
            };
            if delay > std::time::Duration::from_millis(0) {
                if !state.config.adaptive_hedging || idx != 1 {
                    state.metrics.record_hedge("timer");
                }
            }
            let permit = state.concurrency.acquire(&provider.name).await;
            let client = state.client.clone();
            let payload = body.clone();
            let state_clone = state.clone();
            join_set.spawn(async move {
                let _permit = permit;
                if delay > std::time::Duration::from_millis(0) {
                    sleep(delay).await;
                }
                let start = Instant::now();
                let result =
                    forward::send_request(&client, &provider, payload, &state_clone.secrets).await;
                (provider, start.elapsed(), result)
            });
        }

        let mut last_http_failure: Option<(Provider, reqwest::Response, std::time::Duration)> =
            None;

        while let Some(result) = join_set.join_next().await {
            attempts += 1;
            match result {
                Ok((provider, latency, Ok(upstream))) => {
                    let status = upstream.status();
                    if status.is_success() {
                        let headers = upstream.headers().clone();
                        let response_body = upstream.bytes().await.map_err(|err| {
                            (
                                OrlbError::Upstream(format!("failed to read upstream body: {err}")),
                                response_id.clone(),
                            )
                        })?;

                        state
                            .metrics
                            .record_request_success(&provider.name, &method_label, latency)
                            .await;

                        let outcome = if primary_provider_name
                            .as_ref()
                            .map(|name| name == &provider.name)
                            .unwrap_or(false)
                        {
                            "primary"
                        } else {
                            "hedged"
                        };
                        record_outcome(outcome);

                        tracing::info!(
                            provider = provider.name,
                            latency_ms = latency.as_secs_f64() * 1000.0,
                            status = %status,
                            attempt = attempts,
                            hedged = attempts > 1,
                            "request served"
                        );
                        join_set.abort_all();
                        return Ok(build_upstream_response(
                            status,
                            &headers,
                            &provider,
                            response_body,
                            latency,
                        ));
                    }

                    state
                        .metrics
                        .record_request_failure(
                            &provider.name,
                            &method_label,
                            &format!("status_{}", status.as_u16()),
                        )
                        .await;
                    last_status = Some(status);
                    last_provider = Some(provider.name.clone());
                    last_error = Some(format!("{} responded {}", provider.name, status));

                    if forward::is_retryable_status(status)
                        && attempts < max_attempts
                        && !join_set.is_empty()
                    {
                        state.metrics.record_retry("status").await;
                        last_http_failure = Some((provider, upstream, latency));
                        continue;
                    }

                    record_outcome("exhausted");

                    let headers = upstream.headers().clone();
                    let body_bytes = upstream.bytes().await.map_err(|err| {
                        (
                            OrlbError::Upstream(format!("failed to read upstream body: {err}")),
                            response_id.clone(),
                        )
                    })?;
                    join_set.abort_all();
                    return Ok(build_upstream_response(
                        status, &headers, &provider, body_bytes, latency,
                    ));
                }
                Ok((provider, _latency, Err(err))) => {
                    state
                        .metrics
                        .record_request_failure(&provider.name, &method_label, "transport")
                        .await;
                    last_error = Some(format!("{} transport error: {err}", provider.name));
                    last_provider = Some(provider.name.clone());
                    if attempts < max_attempts && !join_set.is_empty() {
                        state.metrics.record_retry("transport").await;
                        continue;
                    }
                    record_outcome("exhausted");
                }
                Err(join_err) => {
                    last_error = Some(format!("hedged task join error: {join_err}"));
                }
            }
        }

        if let Some((provider, upstream, latency)) = last_http_failure {
            let status = upstream.status();
            let headers = upstream.headers().clone();
            let body_bytes = upstream.bytes().await.map_err(|err| {
                (
                    OrlbError::Upstream(format!("failed to read upstream body: {err}")),
                    response_id.clone(),
                )
            })?;
            record_outcome("exhausted");
            return Ok(build_upstream_response(
                status, &headers, &provider, body_bytes, latency,
            ));
        }

        record_outcome("exhausted");
    } else {
        for provider in providers {
            attempts += 1;
            let _permit = state.concurrency.acquire(&provider.name).await;
            let start = Instant::now();

            match forward::send_request(&state.client, &provider, body.clone(), &state.secrets)
                .await
            {
                Ok(upstream) => {
                    let status = upstream.status();
                    let latency = start.elapsed();

                    if status.is_success() {
                        let headers = upstream.headers().clone();
                        let response_body = upstream.bytes().await.map_err(|err| {
                            (
                                OrlbError::Upstream(format!("failed to read upstream body: {err}")),
                                response_id.clone(),
                            )
                        })?;

                        state
                            .metrics
                            .record_request_success(&provider.name, &method_label, latency)
                            .await;

                        tracing::info!(
                            provider = provider.name,
                            latency_ms = latency.as_secs_f64() * 1000.0,
                            status = %status,
                            attempt = attempts,
                            "request served"
                        );

                        return Ok(build_upstream_response(
                            status,
                            &headers,
                            &provider,
                            response_body,
                            latency,
                        ));
                    }

                    state
                        .metrics
                        .record_request_failure(
                            &provider.name,
                            &method_label,
                            &format!("status_{}", status.as_u16()),
                        )
                        .await;

                    if forward::is_retryable_status(status) && attempts < max_attempts {
                        state.metrics.record_retry("status").await;
                        last_error = Some(format!("{} responded {}", provider.name, status));
                        last_status = Some(status);
                        last_provider = Some(provider.name.clone());
                        continue;
                    }

                    let headers = upstream.headers().clone();
                    let body_bytes = upstream.bytes().await.map_err(|err| {
                        (
                            OrlbError::Upstream(format!("failed to read upstream body: {err}")),
                            response_id.clone(),
                        )
                    })?;
                    return Ok(build_upstream_response(
                        status, &headers, &provider, body_bytes, latency,
                    ));
                }
                Err(err) => {
                    state
                        .metrics
                        .record_request_failure(&provider.name, &method_label, "transport")
                        .await;
                    last_error = Some(format!("{} transport error: {err}", provider.name));
                    last_provider = Some(provider.name.clone());
                    if attempts < max_attempts {
                        state.metrics.record_retry("transport").await;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    let message = last_error.unwrap_or_else(|| "no providers available".to_string());
    tracing::error!(
        attempts,
        last_status = last_status.map(|s| s.as_u16()),
        last_provider = last_provider.as_deref(),
        "request failed after retries"
    );
    Err((OrlbError::Upstream(message), response_id))
}

async fn handle_metrics(state: &AppState) -> Response<Body> {
    match state.metrics.encode_prometheus() {
        Ok(metrics) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
            .header(hyper::header::CACHE_CONTROL, "no-store")
            .header(
                HeaderName::from_static("refresh"),
                HeaderValue::from_static("5"),
            )
            .body(Body::from(metrics))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = ?err, "failed to encode metrics");
            internal_server_error()
        }
    }
}

async fn handle_metrics_json(state: &AppState) -> Response<Body> {
    let snapshot = state.metrics.dashboard_snapshot().await;
    match serde_json::to_vec(&snapshot) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .header(hyper::header::CACHE_CONTROL, "no-store")
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = ?err, "failed to serialize metrics snapshot");
            internal_server_error()
        }
    }
}

fn handle_admin_providers_get(state: &Arc<AppState>) -> Response<Body> {
    let snapshot = state.registry.snapshot();
    match serde_json::to_vec_pretty(snapshot.providers()) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = ?err, "failed to serialize registry snapshot");
            internal_server_error()
        }
    }
}

async fn handle_admin_providers_post(req: Request<Body>, state: Arc<AppState>) -> Response<Body> {
    let body = match to_bytes(req.into_body()).await {
        Ok(bytes) => {
            if bytes.len() > MAX_PAYLOAD_BYTES {
                return bad_request("payload exceeds maximum size");
            }
            bytes
        }
        Err(err) => {
            tracing::warn!(error = ?err, "failed to read admin payload");
            return bad_request("unable to read request body");
        }
    };

    let payload = match serde_json::from_slice::<ProviderUpdateInput>(&body) {
        Ok(value) => value.into_payload(),
        Err(err) => {
            tracing::warn!(error = ?err, "invalid provider update payload");
            return bad_request("invalid provider payload");
        }
    };

    if payload.providers.is_empty() {
        return bad_request("provider list cannot be empty");
    }

    let registry = match Registry::from_providers(payload.providers) {
        Ok(registry) => registry,
        Err(err) => {
            tracing::warn!(error = ?err, "failed to parse provider registry");
            return bad_request(&format!("invalid provider registry: {err}"));
        }
    };

    if payload.persist {
        if let Err(err) = persist_registry_to_disk(state.config.providers_path.as_path(), &registry)
        {
            tracing::error!(error = ?err, "failed to persist provider registry");
            return internal_server_error();
        }
    }

    let provider_count = registry.len();
    state.concurrency.reload(registry.providers());
    state.metrics.reload_registry(registry).await;

    respond_with_json(
        StatusCode::OK,
        json!({
            "providers": provider_count,
            "persisted": payload.persist
        }),
    )
}

fn bad_request(message: &str) -> Response<Body> {
    respond_with_json(
        StatusCode::BAD_REQUEST,
        json!({
            "error": message
        }),
    )
}

fn respond_with_json(status: StatusCode, payload: Value) -> Response<Body> {
    match serde_json::to_vec(&payload) {
        Ok(body) => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = ?err, "failed to serialize admin response");
            internal_server_error()
        }
    }
}

fn persist_registry_to_disk(path: &Path, registry: &Registry) -> Result<()> {
    let json =
        serde_json::to_vec_pretty(registry.providers()).context("serializing provider registry")?;
    fs::write(path, json)
        .with_context(|| format!("failed to write provider registry to {}", path.display()))?;
    Ok(())
}

fn build_upstream_response(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
    provider: &Provider,
    body: bytes::Bytes,
    latency: std::time::Duration,
) -> Response<Body> {
    let mut response = Response::builder()
        .status(status)
        .body(Body::from(body))
        .expect("failed to build upstream response");

    {
        let headers_mut = response.headers_mut();
        if let Some(content_type) = headers.get(CONTENT_TYPE) {
            headers_mut.insert(CONTENT_TYPE, content_type.clone());
        } else {
            headers_mut.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        headers_mut.insert(
            HeaderName::from_static("x-orlb-provider"),
            HeaderValue::from_str(provider.name.as_str())
                .unwrap_or_else(|_| HeaderValue::from_static("unknown")),
        );
        headers_mut.insert(
            HeaderName::from_static("x-orlb-upstream"),
            HeaderValue::from_str(provider.url.as_str())
                .unwrap_or_else(|_| HeaderValue::from_static("unspecified")),
        );
        headers_mut.insert(
            HeaderName::from_static("x-orlb-latency-ms"),
            HeaderValue::from_str(&format!("{:.2}", latency.as_secs_f64() * 1000.0))
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        );
    }

    response
}

fn internal_server_error() -> Response<Body> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "error": {
                    "message": "internal server error"
                }
            })
            .to_string(),
        ))
        .unwrap()
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "error": {
                    "message": "not found"
                }
            })
            .to_string(),
        ))
        .unwrap()
}

fn method_not_allowed() -> Response<Body> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(CONTENT_TYPE, "application/json")
        .header(ALLOW, "POST")
        .body(Body::from(
            json!({
                "error": {
                    "message": "use POST with a JSON-RPC payload",
                    "sample_curl": "curl -X POST http://localhost:8080/rpc -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSlot\",\"params\":[]}'"
                }
            })
            .to_string(),
        ))
        .unwrap()
}

fn error_response(err: OrlbError, id: Option<Value>) -> Response<Body> {
    let body = err.to_body();
    let payload = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": body.code,
            "message": body.message,
        },
        "id": id.unwrap_or(Value::Null),
    });

    Response::builder()
        .status(err.status_code())
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

fn is_mutating_method(method: &str) -> bool {
    let normalized = method.trim().to_ascii_lowercase();
    MUTATING_METHODS.contains(normalized.as_str())
        || normalized.starts_with("send")
        || normalized.starts_with("sign")
        || normalized.starts_with("requestairdrop")
}

struct RequestMetadata {
    method_names: Vec<String>,
    contains_mutating_method: bool,
    retryable: bool,
    id: Option<Value>,
    is_batch: bool,
    required_commitment: Commitment,
}

impl RequestMetadata {
    fn from_payload(payload: &Value) -> Result<Self, OrlbError> {
        match payload {
            Value::Object(map) => {
                let method = map
                    .get("method")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OrlbError::InvalidPayload("missing method".into()))?;
                let id = map.get("id").cloned();
                let method = forward::normalize_method_name(method).to_string();
                let contains_mutating_method = is_mutating_method(&method);
                let mut required_commitment = Commitment::Finalized;
                if let Some(params) = map.get("params") {
                    required_commitment = required_commitment
                        .merge_requirement(extract_commitment_from_params(params));
                }
                Ok(Self {
                    method_names: vec![method.clone()],
                    contains_mutating_method,
                    retryable: !contains_mutating_method,
                    id,
                    is_batch: false,
                    required_commitment,
                })
            }
            Value::Array(items) => {
                if items.is_empty() {
                    return Err(OrlbError::InvalidPayload(
                        "batch requests must contain at least one entry".into(),
                    ));
                }
                let mut methods = Vec::with_capacity(items.len());
                let mut mutating = false;
                let mut required_commitment = Commitment::Finalized;
                for item in items {
                    let method = item.get("method").and_then(Value::as_str).ok_or_else(|| {
                        OrlbError::InvalidPayload("batch entry missing method field".into())
                    })?;
                    let method = forward::normalize_method_name(method).to_string();
                    if is_mutating_method(&method) {
                        mutating = true;
                    }
                    methods.push(method);
                    if let Some(params) = item.get("params") {
                        required_commitment = required_commitment
                            .merge_requirement(extract_commitment_from_params(params));
                    }
                }
                Ok(Self {
                    method_names: methods,
                    contains_mutating_method: mutating,
                    retryable: !mutating,
                    id: None,
                    is_batch: true,
                    required_commitment,
                })
            }
            _ => Err(OrlbError::InvalidPayload(
                "JSON-RPC payload must be an object or array".into(),
            )),
        }
    }

    fn method_label(&self) -> String {
        if self.is_batch {
            "batch".to_string()
        } else {
            self.method_names
                .first()
                .cloned()
                .unwrap_or_else(|| "unknown".to_string())
        }
    }

    fn commitment(&self) -> Commitment {
        self.required_commitment
    }
}

fn extract_commitment_from_params(value: &Value) -> Option<Commitment> {
    match value {
        Value::Array(items) => items.iter().find_map(extract_commitment_from_params),
        Value::Object(map) => {
            if let Some(Value::String(level)) = map.get("commitment") {
                let normalized = level.to_ascii_lowercase();
                Commitment::from_str(&normalized)
            } else {
                map.values().find_map(extract_commitment_from_params)
            }
        }
        _ => None,
    }
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use hyper::body::to_bytes;
    use hyper::Body;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::config::{Config, OtelConfig, OtelExporter, SecretBackend, SecretsConfig};
    use crate::forward::build_http_client;
    use crate::metrics::CommitmentSlots;
    use crate::registry::{Provider, Registry, SharedRegistry};
    use crate::secrets::SecretManager;

    fn test_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            providers_path: PathBuf::from("providers.json"),
            probe_interval: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            retry_read_requests: true,
            dashboard_assets_dir: None,
            slot_lag_penalty_ms: 5.0,
            slot_lag_alert_slots: 50,
            hedge_requests: false,
            hedge_delay: Duration::from_millis(60),
            adaptive_hedging: true,
            hedge_min_delay_ms: 10.0,
            hedge_max_delay_ms: 200.0,
            slo_target: 0.995,
            otel: OtelConfig {
                exporter: OtelExporter::None,
                service_name: "orlb-test".into(),
            },
            tag_weights: HashMap::new(),
            secrets: SecretsConfig {
                backend: SecretBackend::None,
            },
        }
    }

    async fn build_state(providers: Vec<Provider>) -> Arc<AppState> {
        build_state_with_config(providers, test_config()).await
    }

    async fn build_state_with_config(providers: Vec<Provider>, config: Config) -> Arc<AppState> {
        let registry = Registry::from_providers(providers).unwrap();
        let shared_registry = SharedRegistry::new(registry);
        let metrics = Metrics::new(shared_registry.clone(), &config).unwrap();
        let provider_registry = shared_registry.snapshot();
        let concurrency = ProviderConcurrency::new(provider_registry.providers());
        let client = build_http_client(Duration::from_secs(5)).unwrap();
        let secrets = SecretManager::new(&config.secrets).unwrap();

        Arc::new(AppState {
            config,
            registry: shared_registry,
            metrics,
            client,
            concurrency,
            secrets,
        })
    }

    fn make_request(payload: Value) -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .uri("http://localhost/rpc")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap()
    }

    fn find_request_id_with_seed(seed: usize, count: usize) -> Value {
        for candidate in 0u64..10_000 {
            let value = Value::from(candidate);
            if request_id_seed(&value, count) == Some(seed) {
                return value;
            }
        }
        panic!("unable to find request id with seed {seed} for count {count}");
    }

    #[tokio::test]
    async fn forwards_successful_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","result":123,"id":1})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = Provider {
            name: "Primary".into(),
            url: server.uri(),
            weight: 1,
            headers: None,
            tags: Vec::new(),
            ws_url: None,
            sample_signature: None,
            parsed_headers: None,
        };

        let state = build_state(vec![provider.clone()]).await;
        let payload = json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]});
        let request = make_request(payload);

        let response = handle_rpc(request, state.clone()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers().clone();
        let body = to_bytes(response.into_body()).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.get("result"), Some(&Value::from(123)));
        assert_eq!(
            headers
                .get("x-orlb-provider")
                .and_then(|value| value.to_str().ok()),
            Some("Primary")
        );

        let snapshot = state.metrics.dashboard_snapshot().await;
        let entry = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Primary")
            .expect("provider snapshot");
        assert_eq!(entry.success, 1);
    }

    #[tokio::test]
    async fn retries_on_retryable_failure() {
        let primary = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&primary)
            .await;

        let request_id = find_request_id_with_seed(0, 2);

        let secondary = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","result":99,"id": request_id.clone()})),
            )
            .expect(1)
            .mount(&secondary)
            .await;

        let providers = vec![
            Provider {
                name: "Primary".into(),
                url: primary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
            Provider {
                name: "Secondary".into(),
                url: secondary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
        ];

        let state = build_state(providers).await;
        let payload = json!({"jsonrpc":"2.0","id": request_id,"method":"getSlot","params":[]});
        let request = make_request(payload);

        let response = handle_rpc(request, state.clone()).await.unwrap();
        let headers = response.headers().clone();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            headers
                .get("x-orlb-provider")
                .and_then(|value| value.to_str().ok()),
            Some("Secondary")
        );

        let snapshot = state.metrics.dashboard_snapshot().await;
        let primary_entry = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Primary")
            .expect("primary snapshot");
        let secondary_entry = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Secondary")
            .expect("secondary snapshot");

        assert!(primary_entry.errors >= 1);
        assert_eq!(secondary_entry.success, 1);
    }

    #[tokio::test]
    async fn hedges_on_slow_primary() {
        let primary = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(200))
                    .set_body_json(json!({"jsonrpc":"2.0","result":42,"id":1})),
            )
            .expect(1)
            .mount(&primary)
            .await;

        let secondary = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","result":9001,"id":1})),
            )
            .expect(1)
            .mount(&secondary)
            .await;

        let providers = vec![
            Provider {
                name: "Slow".into(),
                url: primary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
            Provider {
                name: "Fast".into(),
                url: secondary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
        ];

        let mut config = test_config();
        config.hedge_requests = true;
        config.hedge_delay = Duration::from_millis(40);
        config.adaptive_hedging = false; // Use fixed delay for test consistency

        let state = build_state_with_config(providers, config).await;
        let payload = json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]});
        let request = make_request(payload);

        let response = handle_rpc(request, state.clone()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers().clone();
        assert_eq!(
            headers
                .get("x-orlb-provider")
                .and_then(|value| value.to_str().ok()),
            Some("Fast")
        );

        let body = to_bytes(response.into_body()).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.get("result"), Some(&Value::from(9001)));

        let snapshot = state.metrics.dashboard_snapshot().await;
        let fast = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Fast")
            .expect("fast provider snapshot");
        assert_eq!(fast.success, 1);
        let slow = snapshot
            .providers
            .iter()
            .find(|p| p.name == "Slow")
            .expect("slow provider snapshot");
        assert_eq!(slow.success, 0);
        assert_eq!(slow.errors, 0);
    }

    #[tokio::test]
    async fn adaptive_hedging_uses_shorter_delay_for_slow_provider() {
        // Test that adaptive hedging reduces delay for slow providers
        let slow_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(150))
                    .set_body_json(json!({"jsonrpc":"2.0","result":42,"id":1})),
            )
            .mount(&slow_server)
            .await;

        let fast_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","result":9001,"id":1})),
            )
            .expect(1)
            .mount(&fast_server)
            .await;

        let providers = vec![
            Provider {
                name: "SlowProvider".into(),
                url: slow_server.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
            Provider {
                name: "FastProvider".into(),
                url: fast_server.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
        ];

        let mut config = test_config();
        config.hedge_requests = true;
        config.hedge_delay = Duration::from_millis(60);
        config.adaptive_hedging = true;
        config.hedge_min_delay_ms = 10.0;
        config.hedge_max_delay_ms = 200.0;

        let state = build_state_with_config(providers, config).await;

        // Prime metrics to simulate the slow provider accumulating high latency.
        let metrics = state.metrics.clone();
        metrics
            .test_seed_latency_samples("SlowProvider", "getSlot", Duration::from_millis(180), 5)
            .await;
        metrics
            .test_seed_health_sample(
                "SlowProvider",
                Duration::from_millis(180),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(1_000_000)),
            )
            .await;
        metrics
            .test_seed_latency_samples("FastProvider", "getSlot", Duration::from_millis(35), 5)
            .await;
        metrics
            .test_seed_health_sample(
                "FastProvider",
                Duration::from_millis(35),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(1_000_000)),
            )
            .await;
        metrics
            .test_override_provider_state("SlowProvider", 220.0, 0, 10)
            .await;
        metrics
            .test_override_provider_state("FastProvider", 40.0, 0, 10)
            .await;

        // Now test that adaptive hedging kicks in with shorter delay
        let payload = json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]});
        let request = make_request(payload);
        let start = std::time::Instant::now();
        let response = handle_rpc(request, state.clone()).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);

        // The fast provider should win due to adaptive hedging
        let headers = response.headers().clone();
        assert_eq!(
            headers
                .get("x-orlb-provider")
                .and_then(|value| value.to_str().ok()),
            Some("FastProvider")
        );

        // Response time should be faster than if we waited for the slow provider
        // Adaptive hedging should have triggered early enough
        assert!(
            elapsed.as_millis() < 150,
            "Adaptive hedging should reduce wait time"
        );

        let body = to_bytes(response.into_body()).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.get("result"), Some(&Value::from(9001)));
    }

    #[tokio::test]
    async fn rejects_mutating_methods() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let provider = Provider {
            name: "Primary".into(),
            url: server.uri(),
            weight: 1,
            headers: None,
            tags: Vec::new(),
            ws_url: None,
            sample_signature: None,
            parsed_headers: None,
        };

        let state = build_state(vec![provider]).await;
        let payload =
            json!({"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["deadbeef"]});
        let request = make_request(payload);

        let result = handle_rpc(request, state.clone()).await;
        match result {
            Err((OrlbError::MutatingMethod(method), _)) => {
                assert!(method.contains("sendTransaction"));
            }
            other => panic!("expected mutating method error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn different_ids_rotate_providers_with_skewed_slots() {
        let first = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "result": "first",
                "id": 1
            })))
            .expect(1)
            .mount(&first)
            .await;

        let second = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "result": "second",
                "id": 2
            })))
            .expect(1)
            .mount(&second)
            .await;

        let providers = vec![
            Provider {
                name: "First".into(),
                url: first.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
            Provider {
                name: "Second".into(),
                url: second.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
                ws_url: None,
                sample_signature: None,
                parsed_headers: None,
            },
        ];

        let state = build_state(providers).await;
        state
            .metrics
            .record_health_success(
                "First",
                Duration::from_millis(100),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(418_000_000)),
            )
            .await;
        state
            .metrics
            .record_health_success(
                "Second",
                Duration::from_millis(100),
                CommitmentSlots::from_slot(Commitment::Finalized, Some(2_000_000)),
            )
            .await;

        assert_ne!(
            request_id_seed(&Value::from(1), 2),
            request_id_seed(&Value::from(2), 2)
        );

        let resp_one = handle_rpc(
            make_request(json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]})),
            state.clone(),
        )
        .await
        .unwrap();
        let provider_one = resp_one
            .headers()
            .get("x-orlb-provider")
            .and_then(|value| value.to_str().ok())
            .unwrap();

        let resp_two = handle_rpc(
            make_request(json!({"jsonrpc":"2.0","id":2,"method":"getSlot","params":[]})),
            state.clone(),
        )
        .await
        .unwrap();
        let provider_two = resp_two
            .headers()
            .get("x-orlb-provider")
            .and_then(|value| value.to_str().ok())
            .unwrap();

        assert_ne!(provider_one, provider_two);
    }
}

fn request_id_seed(id: &Value, count: usize) -> Option<usize> {
    if count == 0 {
        return None;
    }
    if let Some(num) = id.as_u64() {
        let modulo = count as u64;
        if modulo == 0 {
            return Some(0);
        }
        return Some(((num.saturating_sub(1)) % modulo) as usize);
    }
    if let Some(num) = id.as_i64() {
        if count == 1 {
            return Some(0);
        }
        let adjusted = (num - 1).rem_euclid(count as i64);
        return Some(adjusted as usize);
    }
    if let Some(text) = id.as_str() {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        return Some((hasher.finish() as usize) % count);
    }
    let serialized = serde_json::to_string(id).ok()?;
    let mut hasher = DefaultHasher::new();
    serialized.hash(&mut hasher);
    Some((hasher.finish() as usize) % count)
}
