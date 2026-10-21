//! Generate an authentic vector-store fixture with the v0.1.3 public writer.
//!
//! Usage: `cargo run --locked --example write_vector_fixture -- <db-path>`

use std::{path::PathBuf, sync::Arc};

use serde_json::json;
use tinycortex::memory::store::vectors::{EmbeddingBackend, VectorStore};

struct FixtureEmbedding;

#[async_trait::async_trait]
impl EmbeddingBackend for FixtureEmbedding {
    fn name(&self) -> &str {
        "fixture-provider"
    }

    fn model_id(&self) -> &str {
        "fixture-model-v1"
    }

    fn dimensions(&self) -> usize {
        3
    }

    async fn embed(&self, _texts: &[&str]) -> anyhow::Result<Vec<Vec<f32>>> {
        anyhow::bail!("fixture uses the public precomputed-vector writer")
    }
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("expected output SQLite path"))?;
    anyhow::ensure!(!path.exists(), "refusing to replace existing fixture: {}", path.display());
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let store = VectorStore::open(&path, Arc::new(FixtureEmbedding))?;
    store.insert_with_vector(
        "legacy-vector-a",
        "notes",
        "Historical vector fixture alpha.",
        &[1.0, -0.0, 0.25],
        json!({"origin": "v0.1.3-public-writer", "rank": 7, "nested": {"ok": true}}),
    )?;
    store.insert_with_vector(
        "legacy-vector-b",
        "notes",
        "Historical vector fixture beta.",
        &[0.0, 1.0, 0.5],
        json!(["legacy", 2, null]),
    )?;
    store.insert_with_vector(
        "same-id-other-namespace",
        "archive",
        "Same logical id in another namespace.",
        &[0.5, 0.5, 0.0],
        json!({"namespace": "archive"}),
    )?;

    anyhow::ensure!(store.count(None)? == 3, "fixture writer did not persist all vectors");
    anyhow::ensure!(store.count(Some("notes"))? == 2, "notes namespace count mismatch");
    anyhow::ensure!(store.count(Some("archive"))? == 1, "archive namespace count mismatch");
    println!("wrote 3 vectors to {}", path.display());
    Ok(())
}
