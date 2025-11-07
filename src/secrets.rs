use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use gcp_auth;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::time::SystemTime;
use tokio::sync::{OnceCell, RwLock};

use crate::config::{AwsConfig, GcpConfig, SecretBackend, SecretsConfig, VaultConfig};

static SECRET_REF_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?P<scheme>[a-z]+)://(?P<path>[^#]+?)(?:#(?P<key>.+))?$").unwrap());

type DynGcpProvider = Arc<dyn gcp_auth::TokenProvider>;

#[derive(Clone)]
pub struct SecretManager {
    backend: SecretBackend,
    client: Client,
    cache: Arc<RwLock<HashMap<String, String>>>,
    gcp: Arc<GcpState>,
}

#[derive(Default)]
struct GcpState {
    provider: OnceCell<DynGcpProvider>,
}

impl SecretManager {
    pub fn new(config: &SecretsConfig) -> Result<Self> {
        Ok(Self {
            backend: config.backend.clone(),
            client: Client::builder()
                .use_rustls_tls()
                .build()
                .context("building secret manager HTTP client")?,
            cache: Arc::new(RwLock::new(HashMap::new())),
            gcp: Arc::new(GcpState::default()),
        })
    }

    pub async fn resolve(&self, reference: &SecretReference) -> Result<String> {
        let cache_key = reference.cache_key();
        if let Some(value) = self.cache.read().await.get(&cache_key).cloned() {
            return Ok(value);
        }

        let resolved = match (&self.backend, reference.scheme) {
            (SecretBackend::None, _) => {
                return Err(anyhow!(
                    "secret backend is not configured (needed for `{}`)",
                    reference.path
                ))
            }
            (SecretBackend::Vault(config), SecretReferenceScheme::Generic)
            | (SecretBackend::Vault(config), SecretReferenceScheme::VaultOnly) => {
                self.ensure_vault_key(reference)?;
                self.fetch_vault_secret(config, reference).await?
            }
            (SecretBackend::Gcp(config), SecretReferenceScheme::Generic) => {
                self.fetch_gcp_secret(config, reference).await?
            }
            (SecretBackend::Aws(config), SecretReferenceScheme::Generic) => {
                self.fetch_aws_secret(config, reference).await?
            }
            (_, SecretReferenceScheme::VaultOnly) => {
                return Err(anyhow!(
                    "secret backend mismatch for reference `{}`",
                    reference.path
                ))
            }
        };

        self.cache
            .write()
            .await
            .insert(cache_key.clone(), resolved.clone());
        Ok(resolved)
    }

    fn ensure_vault_key(&self, reference: &SecretReference) -> Result<()> {
        if reference.key.is_none() {
            return Err(anyhow!(
                "vault references must include `#field` (offending value `{}`)",
                reference.original
            ));
        }
        Ok(())
    }

    async fn fetch_vault_secret(
        &self,
        config: &VaultConfig,
        reference: &SecretReference,
    ) -> Result<String> {
        let url = format!(
            "{}/v1/{}",
            config.addr.trim_end_matches('/'),
            reference.path.trim_start_matches('/')
        );
        let mut request = self.client.get(url);
        request = request.header("X-Vault-Token", &config.token);
        if let Some(ns) = &config.namespace {
            request = request.header("X-Vault-Namespace", ns);
        }
        let response = request.send().await.context("vault request failed")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "vault responded with status {} for {}",
                response.status(),
                reference.path
            ));
        }
        let payload: VaultResponse = response
            .json()
            .await
            .context("failed to decode vault response")?;
        let key = reference
            .key
            .as_ref()
            .expect("vault references validated to include key");
        payload
            .get(key)
            .ok_or_else(|| anyhow!("vault secret `{}` missing key `{}`", reference.path, key))
    }

    async fn fetch_gcp_secret(
        &self,
        config: &GcpConfig,
        reference: &SecretReference,
    ) -> Result<String> {
        let provider = self
            .gcp
            .provider
            .get_or_try_init(|| async {
                gcp_auth::provider()
                    .await
                    .map_err(|err| anyhow!("failed to initialize GCP auth provider: {err}"))
            })
            .await?;
        let token = provider
            .token(&["https://www.googleapis.com/auth/cloud-platform"])
            .await
            .map_err(|err| anyhow!("failed to fetch GCP access token: {err}"))?;
        let resource = build_gcp_resource_path(&reference.path, config)?;
        let url = format!(
            "https://secretmanager.googleapis.com/v1/{}:access",
            resource
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(token.as_str())
            .send()
            .await
            .context("gcp secret manager request failed")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "gcp secret manager responded with status {} for {}",
                response.status(),
                reference.path
            ));
        }
        let payload: GcpSecretResponse = response
            .json()
            .await
            .context("failed to decode gcp secret response")?;
        let encoded = payload
            .payload
            .and_then(|payload| payload.data)
            .ok_or_else(|| anyhow!("gcp secret `{}` missing payload", reference.path))?;
        let decoded = BASE64_STANDARD
            .decode(encoded.as_bytes())
            .context("gcp secret payload was not valid base64")?;
        let string =
            String::from_utf8(decoded).context("gcp secret payload was not valid utf-8 data")?;
        apply_optional_key(string, reference, "gcp")
    }

    async fn fetch_aws_secret(
        &self,
        config: &AwsConfig,
        reference: &SecretReference,
    ) -> Result<String> {
        let region = resolve_aws_region(config)?;
        let AwsRuntimeCredentials {
            access_key,
            secret_key,
            session_token,
        } = load_aws_credentials()?;
        let endpoint = format!("https://secretsmanager.{region}.amazonaws.com");
        let (secret_id, version_stage) = split_version_spec(&reference.path);
        let mut payload = serde_json::json!({ "SecretId": secret_id });
        if let Some(stage) = version_stage {
            payload["VersionStage"] = stage.into();
        }
        let body =
            serde_json::to_vec(&payload).context("failed to encode aws secrets request body")?;
        let host = format!("secretsmanager.{region}.amazonaws.com");
        let header_values = vec![
            ("content-type", "application/x-amz-json-1.1"),
            ("x-amz-target", "secretsmanager.GetSecretValue"),
            ("host", host.as_str()),
        ];
        let signable_request = SignableRequest::new(
            "POST",
            &endpoint,
            header_values.iter().copied(),
            SignableBody::Bytes(&body),
        )?;
        let identity: Identity =
            Credentials::new(access_key, secret_key, session_token, None, "orlb").into();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&region)
            .name("secretsmanager")
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()
            .map_err(|err| anyhow!("failed to build aws signing params: {err}"))?
            .into();
        let (instructions, _) = sign(signable_request, &signing_params)
            .map_err(|err| anyhow!("failed to sign aws request: {err}"))?
            .into_parts();
        let mut request = self
            .client
            .post(endpoint)
            .header("content-type", "application/x-amz-json-1.1")
            .header("x-amz-target", "secretsmanager.GetSecretValue")
            .body(body);
        for (name, value) in instructions.headers() {
            request = request.header(name, value);
        }
        let response = request
            .send()
            .await
            .context("aws secrets manager request failed")?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response
                .text()
                .await
                .unwrap_or_else(|_| "<empty body>".to_string());
            return Err(anyhow!(
                "aws secrets manager responded with status {}: {}",
                status,
                text
            ));
        }
        let payload: AwsSecretResponse = response
            .json()
            .await
            .context("failed to decode aws secret response")?;
        let secret = if let Some(string) = payload.secret_string {
            string
        } else if let Some(binary) = payload.secret_binary {
            let decoded = BASE64_STANDARD
                .decode(binary.as_bytes())
                .context("aws secret binary payload was not base64 data")?;
            String::from_utf8(decoded).context("aws secret binary payload was not valid utf-8")?
        } else {
            return Err(anyhow!(
                "aws secret `{}` did not return a payload",
                reference.path
            ));
        };
        apply_optional_key(secret, reference, "aws")
    }
}

fn apply_optional_key(value: String, reference: &SecretReference, backend: &str) -> Result<String> {
    if let Some(key) = &reference.key {
        let parsed: Value = serde_json::from_str(&value).with_context(|| {
            format!(
                "{backend} secret `{}` must be JSON to extract key `{}`",
                reference.path, key
            )
        })?;
        let candidate = parsed.get(key).ok_or_else(|| {
            anyhow!(
                "{backend} secret `{}` missing key `{}`",
                reference.path,
                key
            )
        })?;
        if let Some(as_str) = candidate.as_str() {
            Ok(as_str.to_string())
        } else {
            Ok(candidate.to_string())
        }
    } else {
        Ok(value)
    }
}

fn load_aws_credentials() -> Result<AwsRuntimeCredentials> {
    let access_key = env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| anyhow!("AWS_ACCESS_KEY_ID must be set when ORLB_SECRET_BACKEND=aws"))?;
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| anyhow!("AWS_SECRET_ACCESS_KEY must be set when ORLB_SECRET_BACKEND=aws"))?;
    let session_token = env::var("AWS_SESSION_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Ok(AwsRuntimeCredentials {
        access_key,
        secret_key,
        session_token,
    })
}

fn resolve_aws_region(config: &AwsConfig) -> Result<String> {
    if let Some(region) = &config.region {
        return Ok(region.clone());
    }
    for key in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }
    Err(anyhow!(
        "could not determine AWS region; set ORLB_AWS_REGION or AWS_REGION"
    ))
}

#[derive(Clone, Debug)]
struct AwsRuntimeCredentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

fn build_gcp_resource_path(path: &str, config: &GcpConfig) -> Result<String> {
    let (raw_path, version) = split_version_spec(path);
    let trimmed = raw_path.trim_matches('/');
    if trimmed.starts_with("projects/") {
        if trimmed.contains("/versions/") {
            if version.is_some() {
                return Err(anyhow!(
                    "secret `{path}` already includes a version; remove the `@version` suffix"
                ));
            }
            Ok(trimmed.to_string())
        } else {
            Ok(format!(
                "{}/versions/{}",
                trimmed,
                version.unwrap_or_else(|| "latest".to_string())
            ))
        }
    } else {
        let project = config.project.as_ref().ok_or_else(|| {
            anyhow!(
                "secret `{path}` must either provide a fully-qualified `projects/...` path or set ORLB_GCP_PROJECT"
            )
        })?;
        Ok(format!(
            "projects/{}/secrets/{}/versions/{}",
            project.trim_matches('/'),
            trimmed,
            version.unwrap_or_else(|| "latest".to_string())
        ))
    }
}

fn split_version_spec(input: &str) -> (String, Option<String>) {
    if let Some((base, suffix)) = input.rsplit_once('@') {
        if !base.is_empty() && !suffix.is_empty() {
            return (base.to_string(), Some(suffix.to_string()));
        }
    }
    (input.to_string(), None)
}

pub fn parse_secret_reference(raw: &str) -> Result<Option<SecretReference>> {
    let trimmed = raw.trim();
    if let Some(captures) = SECRET_REF_PATTERN.captures(trimmed) {
        let scheme = captures
            .name("scheme")
            .map(|m| m.as_str().to_ascii_lowercase())
            .unwrap_or_default();
        let path = captures
            .name("path")
            .map(|m| m.as_str().trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing secret path"))?;
        let key = captures
            .name("key")
            .map(|m| m.as_str().trim().to_string())
            .filter(|value| !value.is_empty());
        let scheme = match scheme.as_str() {
            "secret" => SecretReferenceScheme::Generic,
            "vault" => SecretReferenceScheme::VaultOnly,
            other => {
                return Err(anyhow!(
                    "unsupported secret backend `{other}` in value `{raw}`"
                ))
            }
        };
        if matches!(scheme, SecretReferenceScheme::VaultOnly) && key.is_none() {
            return Err(anyhow!("missing secret key for vault reference `{raw}`"));
        }
        return Ok(Some(SecretReference {
            scheme,
            path,
            key,
            original: trimmed.to_string(),
        }));
    }
    Ok(None)
}

#[derive(Clone, Debug)]
pub struct SecretReference {
    scheme: SecretReferenceScheme,
    path: String,
    key: Option<String>,
    original: String,
}

impl SecretReference {
    fn cache_key(&self) -> String {
        match &self.key {
            Some(key) => format!("{}://{}#{}", self.scheme.as_str(), self.path, key),
            None => format!("{}://{}", self.scheme.as_str(), self.path),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SecretReferenceScheme {
    Generic,
    VaultOnly,
}

impl SecretReferenceScheme {
    fn as_str(self) -> &'static str {
        match self {
            SecretReferenceScheme::Generic => "secret",
            SecretReferenceScheme::VaultOnly => "vault",
        }
    }
}

#[derive(Debug, Deserialize)]
struct GcpSecretResponse {
    payload: Option<GcpSecretPayload>,
}

#[derive(Debug, Deserialize)]
struct GcpSecretPayload {
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VaultResponse {
    data: VaultData,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum VaultData {
    V1(HashMap<String, String>),
    V2 {
        data: HashMap<String, String>,
        #[serde(default)]
        _metadata: Value,
    },
}

impl VaultResponse {
    fn get(&self, key: &str) -> Option<String> {
        match &self.data {
            VaultData::V1(map) => map.get(key).cloned(),
            VaultData::V2 { data, .. } => data.get(key).cloned(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct AwsSecretResponse {
    #[serde(rename = "SecretString")]
    secret_string: Option<String>,
    #[serde(rename = "SecretBinary")]
    secret_binary: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_secret_scheme_without_key() {
        let reference = parse_secret_reference("secret://projects/demo/secrets/api-key").unwrap();
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert!(reference.key.is_none());
        assert_eq!(reference.path, "projects/demo/secrets/api-key");
    }

    #[test]
    fn rejects_vault_without_key() {
        let err = parse_secret_reference("vault://kv/data/foo").unwrap_err();
        assert!(err.to_string().contains("missing secret key"));
    }

    #[test]
    fn parses_vault_with_key() {
        let reference = parse_secret_reference("vault://kv/data/foo#bar").unwrap();
        assert!(reference.is_some());
        let reference = reference.unwrap();
        assert!(reference.key.is_some());
        assert_eq!(reference.cache_key(), "vault://kv/data/foo#bar");
    }
}
