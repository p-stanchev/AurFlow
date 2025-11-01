use anyhow::{anyhow, Context, Result};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::trace::{self, SdkTracerProvider};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::config::{Config, OtelExporter};

/// Guard that flushes OpenTelemetry exporters on drop.
#[derive(Default)]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(error) = provider.shutdown() {
                tracing::warn!(?error, "failed to shutdown OpenTelemetry tracer provider");
            }
        }
    }
}

/// Initialise tracing subscribers and optional OpenTelemetry exporters.
pub fn init(config: &Config) -> Result<TelemetryGuard> {
    match &config.otel.exporter {
        OtelExporter::None => {
            Registry::default()
                .with(build_env_filter()?)
                .with(tracing_subscriber::fmt::layer().with_target(true).compact())
                .try_init()
                .map_err(|err| anyhow!("initialising tracing subscriber failed: {err}"))?;
            Ok(TelemetryGuard::default())
        }
        OtelExporter::Stdout => {
            let (provider, tracer) = build_stdout_tracer(&config.otel.service_name)
                .context("initialising stdout tracer")?;
            Registry::default()
                .with(build_env_filter()?)
                .with(tracing_subscriber::fmt::layer().with_target(true).compact())
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|err| anyhow!("initialising tracing subscriber failed: {err}"))?;
            let _ = global::set_tracer_provider(provider.clone());
            Ok(TelemetryGuard {
                provider: Some(provider),
            })
        }
        OtelExporter::OtlpHttp { endpoint } => {
            let (provider, tracer) = build_otlp_tracer(endpoint, &config.otel.service_name)
                .with_context(|| {
                    format!("initialising OTLP exporter with endpoint `{endpoint}`")
                })?;
            Registry::default()
                .with(build_env_filter()?)
                .with(tracing_subscriber::fmt::layer().with_target(true).compact())
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|err| anyhow!("initialising tracing subscriber failed: {err}"))?;
            let _ = global::set_tracer_provider(provider.clone());
            Ok(TelemetryGuard {
                provider: Some(provider),
            })
        }
    }
}

fn build_env_filter() -> Result<EnvFilter> {
    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("orlb=info,hyper=warn,reqwest=warn"))
        .map_err(|err| anyhow!("building tracing filter failed: {err}"))
}

fn build_stdout_tracer(service_name: &str) -> Result<(SdkTracerProvider, trace::Tracer)> {
    let exporter = opentelemetry_stdout::SpanExporter::default();
    let provider = trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .with_resource(service_resource(service_name))
        .build();
    let tracer = provider.tracer("orlb");
    Ok((provider, tracer))
}

fn build_otlp_tracer(
    endpoint: &str,
    service_name: &str,
) -> Result<(SdkTracerProvider, trace::Tracer)> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint.to_string())
        .build()
        .context("building OTLP span exporter")?;

    let provider = trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(service_resource(service_name))
        .build();

    let tracer = provider.tracer("orlb");
    Ok((provider, tracer))
}

fn service_resource(service_name: &str) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}
