use std::io::{self, Write};

use anyhow::{Context, Result};
use tinystoragedrivers::{Scope, StorageConfig};

const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_CHUNK_LIMIT: usize = 300;
const MAX_CHUNK_LIMIT: usize = 10_000;

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        // Backend errors can include connection details; keep them out of logs.
        eprintln!("viewer snapshot failed; check the configured storage URL and scope locally");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let workspace = required_env("TINYCORTEX_WORKSPACE")?;
    let storage_url = required_env("TINYCORTEX_STORAGE_URL")?;
    let scope_name = required_env("TINYCORTEX_STORAGE_SCOPE")?;
    let chunk_limit = std::env::var("TINYCORTEX_INSPECTOR_CHUNK_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CHUNK_LIMIT)
        .clamp(1, MAX_CHUNK_LIMIT);

    let config = StorageConfig::parse(&storage_url).context("parse configured storage URL")?;
    let backend = tinystoragedrivers::open(&config)
        .await
        .context("open configured storage backend")?;
    let scope = Scope::new(&scope_name).context("validate configured storage scope")?;
    let storage = backend
        .for_scope(&scope)
        .context("open configured storage scope")?;
    let snapshot = tinycortex::memory::inspection::read_snapshot(workspace, storage, chunk_limit)
        .await
        .context("read owner inspection snapshot")?;

    let bytes = serde_json::to_vec(&snapshot).context("encode inspection snapshot")?;
    anyhow::ensure!(
        bytes.len() <= MAX_OUTPUT_BYTES,
        "inspection snapshot exceeds the output limit"
    );
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(&bytes)
        .context("write inspection snapshot")?;
    stdout
        .write_all(b"\n")
        .context("finish inspection snapshot")?;
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
    Ok(value)
}
