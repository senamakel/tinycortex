use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::json;
use tinycortex::memory::store::KvStore;

fn main() -> Result<()> {
    let workspace = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_kv_fixture <output-workspace>")?;
    std::fs::create_dir_all(&workspace)
        .with_context(|| format!("create output workspace {}", workspace.display()))?;
    anyhow::ensure!(
        std::fs::read_dir(&workspace)?.next().is_none(),
        "output workspace must be empty: {}",
        workspace.display()
    );

    let store = KvStore::open(&workspace.join("memory_tree/skill_docs.db"))?;
    store
        .set_global("same-key", &json!("global value"))
        .map_err(anyhow::Error::msg)?;
    store
        .set_global("nullable", &json!(null))
        .map_err(anyhow::Error::msg)?;
    store
        .set_global("json-value", &json!({"count": 3, "enabled": true}))
        .map_err(anyhow::Error::msg)?;
    store
        .set_namespace("team alpha/#1", "same-key", &json!("namespace value"))
        .map_err(anyhow::Error::msg)?;
    store.set_namespace(
        "team alpha/#1",
        "structured",
        &json!(["first", null, {"ok": true}]),
    )
    .map_err(anyhow::Error::msg)?;

    println!("wrote 3 global and 2 namespace KV records to {}", workspace.display());
    Ok(())
}
