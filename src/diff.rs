use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};

use crate::registry::{Provider, Registry};

pub fn run(baseline_path: &Path, candidate_path: &Path) -> Result<()> {
    println!("Comparing provider registries:");
    println!("    Base     : {}", baseline_path.display());
    println!("    Candidate: {}", candidate_path.display());

    let baseline = Registry::load(baseline_path).with_context(|| {
        format!(
            "failed to load baseline registry from {}",
            baseline_path.display()
        )
    })?;
    let candidate = Registry::load(candidate_path).with_context(|| {
        format!(
            "failed to load candidate registry from {}",
            candidate_path.display()
        )
    })?;

    let baseline_map = by_name(baseline.providers());
    let candidate_map = by_name(candidate.providers());

    let mut added = Vec::new();
    for (name, provider) in &candidate_map {
        if !baseline_map.contains_key(name) {
            added.push(*provider);
        }
    }

    let mut removed = Vec::new();
    for (name, provider) in &baseline_map {
        if !candidate_map.contains_key(name) {
            removed.push(*provider);
        }
    }

    let mut changed = Vec::new();
    for (name, baseline_provider) in &baseline_map {
        if let Some(candidate_provider) = candidate_map.get(name) {
            let diffs = diff_provider(baseline_provider, candidate_provider);
            if !diffs.is_empty() {
                changed.push((*name, diffs));
            }
        }
    }

    if added.is_empty() && removed.is_empty() && changed.is_empty() {
        println!("No differences detected. Provider weights and metadata match.");
        return Ok(());
    }

    if !added.is_empty() {
        println!("Providers added ({}):", added.len());
        for provider in added {
            println!(
                "    - {} (url: {}, weight: {})",
                provider.name, provider.url, provider.weight
            );
        }
    }

    if !removed.is_empty() {
        println!("Providers removed ({}):", removed.len());
        for provider in removed {
            println!(
                "    - {} (url: {}, weight: {})",
                provider.name, provider.url, provider.weight
            );
        }
    }

    if !changed.is_empty() {
        println!("Providers changed ({}):", changed.len());
        for (name, diffs) in changed {
            println!("    - {}", name);
            for diff in diffs {
                println!("        * {}", diff);
            }
        }
    }

    Ok(())
}

fn by_name<'a>(providers: &'a [Provider]) -> BTreeMap<&'a str, &'a Provider> {
    let mut map = BTreeMap::new();
    for provider in providers {
        map.insert(provider.name.as_str(), provider);
    }
    map
}

fn diff_provider(baseline: &Provider, candidate: &Provider) -> Vec<String> {
    let mut diffs = Vec::new();

    if baseline.url != candidate.url {
        diffs.push(format!(
            "url changed: {} -> {}",
            baseline.url, candidate.url
        ));
    }

    if baseline.weight != candidate.weight {
        diffs.push(format!(
            "weight changed: {} -> {}",
            baseline.weight, candidate.weight
        ));
    }

    if baseline.ws_url != candidate.ws_url {
        diffs.push(format!(
            "ws_url changed: {} -> {}",
            format_option(baseline.ws_url.as_deref()),
            format_option(candidate.ws_url.as_deref())
        ));
    }

    if baseline.sample_signature != candidate.sample_signature {
        diffs.push(format!(
            "sample_signature changed: {} -> {}",
            format_option(baseline.sample_signature.as_deref()),
            format_option(candidate.sample_signature.as_deref())
        ));
    }

    if baseline.tags != candidate.tags {
        diffs.push(format!(
            "tags changed: [{}] -> [{}]",
            baseline.tags.join(", "),
            candidate.tags.join(", ")
        ));
    }

    if headers_repr(baseline) != headers_repr(candidate) {
        diffs.push(format!(
            "headers changed: {} -> {}",
            headers_display(baseline),
            headers_display(candidate)
        ));
    }

    diffs
}

fn headers_repr(provider: &Provider) -> Vec<(String, String)> {
    let mut repr = provider
        .headers
        .as_ref()
        .map(|headers| {
            headers
                .iter()
                .map(|header| (header.name.clone(), header.value.clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    repr.sort();
    repr
}

fn headers_display(provider: &Provider) -> String {
    let repr = headers_repr(provider);
    if repr.is_empty() {
        "none".to_string()
    } else {
        repr.into_iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_option(value: Option<&str>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}
