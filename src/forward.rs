use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, StatusCode};

use crate::registry::{Header, Provider};

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
    payload: &[u8],
) -> Result<reqwest::Response> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));

    if let Some(extra) = provider.headers.as_ref() {
        apply_custom_headers(&mut headers, extra)?;
    }

    let response = client
        .post(&provider.url)
        .headers(headers)
        .body(payload.to_vec())
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

fn apply_custom_headers(map: &mut HeaderMap, headers: &[Header]) -> Result<()> {
    for header in headers {
        let name = HeaderName::try_from(header.name.as_str())
            .with_context(|| format!("invalid header name `{}`", header.name))?;
        let value = HeaderValue::try_from(header.value.as_str())
            .with_context(|| format!("invalid header value for `{}`", header.name))?;
        map.insert(name, value);
    }
    Ok(())
}
