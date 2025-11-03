use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, StatusCode};

use crate::registry::{HeaderValueKind, Provider};
use crate::secrets::SecretManager;

const USER_AGENT_VALUE: &str = "orlb/0.1 (https://github.com/open-rpc-lb)";

pub fn build_http_client(timeout: Duration) -> Result<Client> {
    let client = Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .pool_max_idle_per_host(10)
        .use_rustls_tls()
        .build()
        .context("building HTTP client")?;
    Ok(client)
}

pub async fn send_request(
    client: &Client,
    provider: &Provider,
    payload: Bytes,
    secrets: &SecretManager,
) -> Result<reqwest::Response> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));

    if let Some(extra) = provider.parsed_headers.as_ref() {
        for parsed in extra.iter() {
            match &parsed.value {
                HeaderValueKind::Static(value) => {
                    headers.insert(parsed.name.clone(), value.clone());
                }
                HeaderValueKind::Secret(reference) => {
                    let secret = secrets
                        .resolve(reference)
                        .await
                        .with_context(|| format!("resolving secret for header {}", parsed.name))?;
                    let value = HeaderValue::try_from(secret.as_str()).map_err(|err| {
                        anyhow!(
                            "secret value for header {} is not a valid header: {err}",
                            parsed.name
                        )
                    })?;
                    headers.insert(parsed.name.clone(), value);
                }
            }
        }
    }

    let response = client
        .post(&provider.url)
        .headers(headers)
        .body(payload)
        .send()
        .await
        .with_context(|| format!("request to {} failed", provider.name))?;

    Ok(response)
}

pub fn is_retryable_status(status: StatusCode) -> bool {
    status.as_u16() == 429 || status.is_server_error()
}

pub fn normalize_method_name(method: &str) -> &str {
    method.trim()
}
