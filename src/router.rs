use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use hyper::body::to_bytes;
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::signal;
use uuid::Uuid;

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

    let body = to_bytes(req.into_body()).await.map_err(|err| {
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

    let method_label = metadata.method_label();
    let ranked = state.metrics.provider_ranked_list().await;
    let mut providers: Vec<_> = ranked
        .iter()
        .filter(|(_, _, healthy)| *healthy)
        .cloned()
        .collect();
    if providers.is_empty() {
        providers = ranked.clone();
    }
    if providers.is_empty() {
        return Err((OrlbError::NoHealthyProviders, response_id));
    }

    let max_attempts = if state.config.retry_read_requests && metadata.retryable {
        2
    } else {
        1
    };

    let mut attempts = 0usize;
    let mut last_error = None;
    let mut last_status = None;
    let mut last_provider: Option<String> = None;

    for (provider, _score, _healthy) in providers {
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
                Ok(Self {
                    method_names: vec![method.clone()],
                    contains_mutating_method,
                    retryable: !contains_mutating_method,
                    id,
                    is_batch: false,
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
                for item in items {
                    let method = item.get("method").and_then(Value::as_str).ok_or_else(|| {
                        OrlbError::InvalidPayload("batch entry missing method field".into())
                    })?;
                    let method = forward::normalize_method_name(method).to_string();
                    if is_mutating_method(&method) {
                        mutating = true;
                    }
                    methods.push(method);
                }
                Ok(Self {
                    method_names: methods,
                    contains_mutating_method: mutating,
                    retryable: !mutating,
                    id: None,
                    is_batch: true,
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
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use hyper::body::to_bytes;
    use hyper::Body;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::forward::build_http_client;
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
        }
    }

    async fn build_state(providers: Vec<Provider>) -> Arc<AppState> {
        let registry = Registry::from_providers(providers).unwrap();
        let config = test_config();
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

        let secondary = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","result":99,"id":1})),
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
            },
            Provider {
                name: "Secondary".into(),
                url: secondary.uri(),
                weight: 1,
                headers: None,
            },
        ];

        let state = build_state(providers).await;
        let payload = json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]});
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
}
