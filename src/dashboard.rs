use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use hyper::{Body, Response, StatusCode};

use crate::config::Config;

const EMBEDDED_DASHBOARD: &str = include_str!("../static/dashboard.html");
const EMBEDDED_LOGO: &[u8] = include_bytes!("../static/aurflow.png");

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

pub fn serve_logo(config: &Config) -> Result<Response<Body>> {
    if let Some(dir) = config.dashboard_assets_dir.as_ref() {
        let path = dir.join("aurflow.png");
        let bytes = load_asset_from_disk(&path)?;
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "image/png")
            .header("cache-control", "no-store")
            .body(Body::from(bytes))
            .expect("dashboard logo response build failed"));
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "image/png")
        .header("cache-control", "no-store")
        .body(Body::from(EMBEDDED_LOGO.as_ref()))
        .expect("dashboard logo response build failed"))
}

fn load_dashboard_from_disk(path: &PathBuf) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("failed to read dashboard from {}", path.display()))
}

fn load_asset_from_disk(path: &PathBuf) -> Result<Vec<u8>> {
    fs::read(path)
        .with_context(|| format!("failed to read dashboard asset from {}", path.display()))
}
