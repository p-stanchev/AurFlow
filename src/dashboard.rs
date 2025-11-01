use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use hyper::{Body, Response, StatusCode};

use crate::config::Config;

const EMBEDDED_DASHBOARD: &str = include_str!("../static/dashboard.html");

pub fn serve(config: &Config) -> Result<Response<Body>> {
    if let Some(dir) = config.dashboard_assets_dir.as_ref() {
        let path = dir.join("dashboard.html");
        let html = load_dashboard_from_disk(&path)?;
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/html; charset=utf-8")
            .header("cache-control", "no-store")
            .body(Body::from(html))
            .expect("dashboard response build failed"));
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-store")
        .body(Body::from(EMBEDDED_DASHBOARD))
        .expect("dashboard response build failed"))
}

fn load_dashboard_from_disk(path: &PathBuf) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("failed to read dashboard from {}", path.display()))
}
