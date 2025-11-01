use std::collections::{hash_map::DefaultHasher, HashSet};
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use hyper::body::to_bytes;
use hyper::header::{HeaderName, HeaderValue, ALLOW, CONTENT_TYPE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::signal;
use tokio::task::JoinSet;
use tokio::time::sleep;
use uuid::Uuid;

use crate::commitment::Commitment;
use crate::config::Config;
use crate::dashboard;
use crate::errors::OrlbError;
use crate::forward;
use crate::metrics::Metrics;
use crate::registry::Provider;

const MAX_PAYLOAD_BYTES: usize = 512 * 1024;
const DASHBOARD_PATH: &str = "/dashboard";
const METRICS_PATH: &str = "/metrics";
const METRICS_JSON_PATH: &str = "/metrics.json";
const RPC_PATH: &str = "/rpc";

static MUTATING_METHODS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "sendTransaction",
        "sendRawTransaction",
        "signTransaction",
        "signAndSendTransaction",
        "requestAirdrop",
        "setComputeUnitLimit",
        "setComputeUnitPrice",
        "setLogFilter",
        "setPriorityFee",
    ]
    .into_iter()
    .collect()
});

#[derive(Clone)]
struct AppState {
    config: Config,
    metrics: Metrics,
    client: Client,
}

pub async fn start_server(config: Config, metrics: Metrics, client: Client) -> Result<()> {
    let listen_addr = config.listen_addr;
    let state = Arc::new(AppState {
        config,
        metrics,
        client,
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
        let mut join_set: JoinSet<(
            Provider,
            std::time::Duration,
            anyhow::Result<reqwest::Response>,
        )> = JoinSet::new();
        for (idx, provider) in providers.iter().take(max_attempts).cloned().enumerate() {
            let delay = if idx == 0 {
                std::time::Duration::from_millis(0)
            } else {
                state.config.hedge_delay
            };
            if delay > std::time::Duration::from_millis(0) {
                state.metrics.record_hedge("timer");
            }
            let client = state.client.clone();
            let payload = body.clone();
            join_set.spawn(async move {
                if delay > std::time::Duration::from_millis(0) {
                    sleep(delay).await;
                }
                let start = Instant::now();
                let result = forward::send_request(&client, &provider, payload.as_ref()).await;
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
            return Ok(build_upstream_response(
                status, &headers, &provider, body_bytes, latency,
            ));
        }
    } else {
        for provider in providers {
            attempts += 1;
            let start = Instant::now();

            match forward::send_request(&state.client, &provider, &body).await {
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
    let trimmed = method.trim();
    MUTATING_METHODS.contains(trimmed)
        || trimmed.starts_with("send")
        || trimmed.starts_with("sign")
        || trimmed.starts_with("requestAirdrop")
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

    use crate::config::{Config, OtelConfig, OtelExporter};
    use crate::forward::build_http_client;
    use crate::metrics::CommitmentSlots;
    use crate::registry::{Provider, Registry};

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
            slo_target: 0.995,
            otel: OtelConfig {
                exporter: OtelExporter::None,
                service_name: "orlb-test".into(),
            },
            tag_weights: HashMap::new(),
        }
    }

    async fn build_state(providers: Vec<Provider>) -> Arc<AppState> {
        build_state_with_config(providers, test_config()).await
    }

    async fn build_state_with_config(providers: Vec<Provider>, config: Config) -> Arc<AppState> {
        let registry = Registry::from_providers(providers).unwrap();
        let metrics = Metrics::new(registry, &config).unwrap();
        let client = build_http_client(Duration::from_secs(5)).unwrap();

        Arc::new(AppState {
            config,
            metrics,
            client,
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
            },
            Provider {
                name: "Secondary".into(),
                url: secondary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
            },
        ];

        let state = build_state(providers).await;
        let payload =
            json!({"jsonrpc":"2.0","id": request_id,"method":"getSlot","params":[]});
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
            },
            Provider {
                name: "Fast".into(),
                url: secondary.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
            },
        ];

        let mut config = test_config();
        config.hedge_requests = true;
        config.hedge_delay = Duration::from_millis(40);

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
            },
            Provider {
                name: "Second".into(),
                url: second.uri(),
                weight: 1,
                headers: None,
                tags: Vec::new(),
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
    let serialized = serde_json::to_string(id).ok()?;
    let mut hasher = DefaultHasher::new();
    serialized.hash(&mut hasher);
    Some((hasher.finish() as usize) % count)
}
