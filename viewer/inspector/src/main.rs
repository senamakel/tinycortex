use std::io::{self, Write};

use anyhow::{Context, Result};
use tinystoragedrivers::{Scope, StorageConfig, StorageBackend};

const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_LIMIT: usize = 300;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Do not print the configured URL: Mongo URLs may contain credentials.
        eprintln!("viewer snapshot failed: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let workspace = required_env("TINYCORTEX_WORKSPACE")?;
    let storage_url = required_env("TINYCORTEX_STORAGE_URL")?;
    let scope_name = required_env("TINYCORTEX_STORAGE_SCOPE")?;

    let config = StorageConfig::parse(&storage_url).context("parse configured storage URL")?;
    let backend = tinystoragedrivers::open(&config)
        .await
        .context("open configured storage backend")?;
    let scope = Scope::new(&scope_name).context("validate configured storage scope")?;
    let storage = backend
        .for_scope(&scope)
        .context("open configured storage scope")?;
    let snapshot = tinycortex::memory::inspection::read_snapshot(
        workspace,
        storage,
        CHUNK_LIMIT,
    )
    .await
    .context("read owner inspection snapshot")?;

    let bytes = serde_json::to_vec(&snapshot).context("encode inspection snapshot")?;
    anyhow::ensure!(
        bytes.len() <= MAX_OUTPUT_BYTES,
        "inspection snapshot exceeds the output limit"
    );
    let mut stdout = io::stdout().lock();
    stdout.write_all(&bytes).context("write inspection snapshot")?;
    stdout.write_all(b"\n").context("finish inspection snapshot")?;
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
    Ok(value)
}
