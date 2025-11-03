use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;

use crate::secrets::{parse_secret_reference, SecretReference};

#[derive(Clone, Debug, Deserialize)]
pub struct Provider {
    pub name: String,
    pub url: String,
    #[serde(default = "default_weight")]
    pub weight: u16,
    #[serde(default)]
    pub headers: Option<Vec<Header>>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub sample_signature: Option<String>,
    #[serde(skip, default)]
    pub parsed_headers: Option<Arc<Vec<ParsedHeader>>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct Registry {
    providers: Arc<Vec<Provider>>,
}

impl Registry {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read provider registry from {}", path.display()))?;

        let mut providers: Vec<Provider> = serde_json::from_str(&raw).with_context(|| {
            format!(
                "failed to parse provider registry JSON from {}",
                path.display()
            )
        })?;

        normalise_providers(&mut providers)?;

        if providers.is_empty() {
            bail!("provider registry is empty");
        }

        Ok(Self {
            providers: Arc::new(providers),
        })
    }

    #[cfg(test)]
    pub fn from_providers(providers: Vec<Provider>) -> Result<Self> {
        if providers.is_empty() {
            bail!("provider registry cannot be empty");
        }
        let mut providers = providers;
        normalise_providers(&mut providers)?;
        Ok(Self {
            providers: Arc::new(providers),
        })
    }

    pub fn providers(&self) -> &[Provider] {
        &self.providers
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }
}

fn default_weight() -> u16 {
    1
}

fn normalise_providers(providers: &mut [Provider]) -> Result<()> {
    for provider in providers.iter_mut() {
        if let Some(headers) = provider.headers.as_mut() {
            headers.retain(|header| !header.name.trim().is_empty());
        }
        let mut tags: Vec<String> = provider
            .tags
            .iter()
            .map(|tag| tag.trim().to_ascii_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect();
        tags.sort();
        tags.dedup();
        provider.tags = tags;
        provider.sample_signature = provider.sample_signature.as_ref().and_then(|sig| {
            let trimmed = sig.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        provider.parsed_headers = match provider.headers.as_ref() {
            Some(headers) if !headers.is_empty() => {
                let mut parsed = Vec::with_capacity(headers.len());
                for header in headers {
                    let name = HeaderName::try_from(header.name.as_str()).with_context(|| {
                        format!(
                            "invalid header name `{}` for provider `{}`",
                            header.name, provider.name
                        )
                    })?;
                    if let Some(secret) = parse_secret_reference(&header.value)? {
                        parsed.push(ParsedHeader {
                            name,
                            value: HeaderValueKind::Secret(secret),
                        });
                    } else {
                        let value =
                            HeaderValue::try_from(header.value.as_str()).with_context(|| {
                                format!(
                                    "invalid header value for `{}` on provider `{}`",
                                    header.name, provider.name
                                )
                            })?;
                        parsed.push(ParsedHeader {
                            name,
                            value: HeaderValueKind::Static(value),
                        });
                    }
                }
                Some(Arc::new(parsed))
            }
            _ => None,
        };
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ParsedHeader {
    pub name: HeaderName,
    pub value: HeaderValueKind,
}

#[derive(Clone, Debug)]
pub enum HeaderValueKind {
    Static(HeaderValue),
    Secret(SecretReference),
}
