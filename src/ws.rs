use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures::{stream::SplitStream, Sink, SinkExt, StreamExt};
use hyper::{http, Body, Request, Response, StatusCode};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, MaybeTlsStream};

use crate::commitment::Commitment;
use crate::forward::USER_AGENT_VALUE;
use crate::registry::{HeaderValueKind, Provider};
use crate::router::AppState;

const WS_FANOUT_LIMIT: usize = 3;

pub async fn handle(req: Request<Body>, state: Arc<AppState>) -> Response<Body> {
    match hyper_tungstenite::upgrade(req, None) {
        Ok((response, websocket)) => {
            tokio::spawn(async move {
                if let Err(err) = connection_task(state, websocket).await {
                    tracing::debug!(error = ?err, "websocket session ended");
                }
            });
            response
        }
        Err(err) => {
            tracing::warn!(error = ?err, "websocket upgrade failed");
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("invalid websocket upgrade request"))
                .unwrap()
        }
    }
}

async fn connection_task(state: Arc<AppState>, websocket: HyperWebsocket) -> Result<()> {
    let ws_stream = websocket.await.context("accepting websocket")?;
    let (client_sink, mut client_stream) = ws_stream.split();
    let client_sink = Arc::new(Mutex::new(client_sink));

    let ranked = state
        .metrics
        .provider_ranked_list(Commitment::Finalized)
        .await;
    let mut upstreams = Vec::new();

    for (provider, _, _) in ranked.into_iter().filter(|(_, _, healthy)| *healthy) {
        if upstreams.len() >= WS_FANOUT_LIMIT {
            break;
        }
        if let Some((upstream, mut upstream_stream)) = connect_upstream(&state, &provider).await? {
            let client_sink_clone = client_sink.clone();
            let upstream_name = upstream.name.clone();
            tokio::spawn(async move {
                while let Some(message) = upstream_stream.next().await {
                    match message {
                        Ok(msg) => {
                            if let Err(err) = send_to_client(&client_sink_clone, msg).await {
                                tracing::debug!(
                                    error = ?err,
                                    provider = upstream_name,
                                    "failed to forward upstream message"
                                );
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::debug!(
                                error = ?err,
                                provider = upstream_name,
                                "upstream websocket errored"
                            );
                            break;
                        }
                    }
                }
            });
            upstreams.push(upstream);
        }
    }

    if upstreams.is_empty() {
        let _ = send_to_client(&client_sink, Message::Close(None)).await;
        return Err(anyhow!("no websocket-capable providers available"));
    }

    while let Some(message) = client_stream.next().await {
        let message = match message {
            Ok(msg) => msg,
            Err(err) => {
                tracing::debug!(error = ?err, "client websocket error");
                break;
            }
        };

        match &message {
            Message::Text(_) | Message::Binary(_) => {
                let mut idx = 0;
                while idx < upstreams.len() {
                    if let Err(err) = upstreams[idx].send(message.clone()).await {
                        tracing::debug!(
                            error = ?err,
                            provider = upstreams[idx].name,
                            "upstream send failed"
                        );
                        upstreams.remove(idx);
                    } else {
                        idx += 1;
                    }
                }
                if upstreams.is_empty() {
                    tracing::warn!("all upstream websocket connections failed");
                    break;
                }
            }
            Message::Ping(payload) => {
                let _ = send_to_client(&client_sink, Message::Pong(payload.clone())).await;
                continue;
            }
            Message::Close(frame) => {
                for upstream in upstreams.iter_mut() {
                    let _ = upstream.send(Message::Close(frame.clone())).await;
                }
                break;
            }
            Message::Pong(_) | Message::Frame(_) => {
                // ignore
            }
        }
    }

    Ok(())
}

async fn send_to_client<S>(sink: &Arc<Mutex<S>>, message: Message) -> Result<()>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    let mut guard = sink.lock().await;
    guard
        .send(message)
        .await
        .map_err(|err: WsError| anyhow!(err))
}

type UpstreamStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Upstream {
    name: String,
    sink: Arc<Mutex<futures::stream::SplitSink<UpstreamStream, Message>>>,
}

impl Upstream {
    async fn send(&mut self, message: Message) -> Result<()> {
        let mut guard = self.sink.lock().await;
        guard
            .send(message)
            .await
            .map_err(|err: WsError| anyhow!(err))
    }
}

async fn connect_upstream(
    state: &Arc<AppState>,
    provider: &Provider,
) -> Result<Option<(Upstream, SplitStream<UpstreamStream>)>> {
    let ws_url = match provider_ws_url(provider) {
        Some(url) => url,
        None => return Ok(None),
    };

    let request = build_ws_request(provider, &ws_url, &state.secrets).await?;
    let (stream, _) = connect_async(request)
        .await
        .with_context(|| format!("websocket connection to {} failed", provider.name))?;
    let (sink, stream) = stream.split();
    let upstream = Upstream {
        name: provider.name.clone(),
        sink: Arc::new(Mutex::new(sink)),
    };
    Ok(Some((upstream, stream)))
}

async fn build_ws_request(
    provider: &Provider,
    ws_url: &str,
    secrets: &crate::secrets::SecretManager,
) -> Result<http::Request<()>> {
    let mut builder = http::Request::builder().uri(ws_url);
    builder = builder.header("User-Agent", USER_AGENT_VALUE);

    if let Some(extra) = provider.parsed_headers.as_ref() {
        for parsed in extra.iter() {
            match &parsed.value {
                HeaderValueKind::Static(value) => {
                    builder = builder.header(parsed.name.clone(), value.clone());
                }
                HeaderValueKind::Secret(reference) => {
                    let secret = secrets.resolve(reference).await?;
                    let header_value = http::header::HeaderValue::from_str(secret.as_str())
                        .map_err(|err| anyhow!(err))?;
                    builder = builder.header(parsed.name.clone(), header_value);
                }
            }
        }
    }

    builder
        .body(())
        .map_err(|err| anyhow!("failed to build websocket request: {err}"))
}

fn provider_ws_url(provider: &Provider) -> Option<String> {
    if let Some(ws) = provider.ws_url.as_ref() {
        return Some(ws.clone());
    }
    if let Some(rest) = provider.url.strip_prefix("https://") {
        return Some(format!("wss://{}", rest));
    }
    if let Some(rest) = provider.url.strip_prefix("http://") {
        return Some(format!("ws://{}", rest));
    }
    None
}
