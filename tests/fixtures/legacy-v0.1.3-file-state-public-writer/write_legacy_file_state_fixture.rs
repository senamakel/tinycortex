use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::persona::state::{record_watermark, FileStateStore};
use tinycortex::memory::sources::{MemorySourceEntry, SourceKind, SourceRegistry};
use tinycortex::memory::sync::audit::{append_audit_entry, SyncAuditEntry};
use tinycortex::memory::sync::composio::connect::EntityStore;

#[tokio::main]
async fn main() -> Result<()> {
    let workspace = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_file_state_fixture <output-workspace>")?;
    std::fs::create_dir_all(&workspace)?;
    anyhow::ensure!(
        std::fs::read_dir(&workspace)?.next().is_none(),
        "fixture output must be empty: {}",
        workspace.display()
    );

    let config = MemoryConfig::new(&workspace);
    let first = audit_entry("legacy-first", 1_750_000_000_000, 41);
    let second = audit_entry("legacy-second", 1_600_000_000_000, 19);
    append_audit_entry(&config, &first)?;
    append_audit_entry(&config, &second)?;

    let persona = FileStateStore::open_in_workspace(&workspace)?;
    record_watermark(
        &persona,
        "transcript:/home/alice@example.test/.agent/sessions/run.jsonl",
        "cursor:opaque-value@example.test",
    )
    .await?;
    record_watermark(&persona, "git_history:/work/project", "0123456789abcdef").await?;

    let mut entities = EntityStore::load(workspace.join(".composio-harness.json"));
    entities.entity_id_for("slack", Some("legacy-user@example.test"))?;
    entities.entity_id_for("gmail", Some("legacy-gmail-user"))?;

    let registry = SourceRegistry::new(workspace.join("config.toml"));
    registry.add(folder_source("legacy-folder", "/legacy/notes"))?;
    registry.add(composio_source(
        "legacy-slack",
        "slack",
        "legacy-connection",
    ))?;

    println!(
        "wrote public legacy audit, persona, Composio entity, and source-registry data under {}",
        workspace.display()
    );
    Ok(())
}

fn audit_entry(source_id: &str, timestamp_ms: i64, tokens: u64) -> SyncAuditEntry {
    SyncAuditEntry {
        timestamp: Utc.timestamp_millis_opt(timestamp_ms).single().unwrap(),
        source_id: source_id.to_owned(),
        source_kind: "composio".into(),
        scope: "workspace".into(),
        items_fetched: 2,
        batches: 1,
        input_tokens: tokens,
        output_tokens: 3,
        estimated_cost_usd: 0.0001,
        composio_actions_called: 1,
        composio_cost_usd: 0.0002,
        actual_charged_usd: Some(0.0003),
        duration_ms: 120,
        success: true,
        error: None,
    }
}

fn folder_source(id: &str, path: &str) -> MemorySourceEntry {
    MemorySourceEntry {
        id: id.into(),
        kind: SourceKind::Folder,
        label: "Legacy notes".into(),
        enabled: true,
        toolkit: None,
        connection_id: None,
        path: Some(path.into()),
        glob: Some("**/*.md".into()),
        url: None,
        branch: None,
        paths: Vec::new(),
        max_commits: None,
        max_issues: None,
        max_prs: None,
        query: None,
        since_days: None,
        max_items: None,
        selector: None,
        max_tokens_per_sync: Some(987),
        max_cost_per_sync_usd: Some(0.42),
        sync_depth_days: Some(45),
    }
}

fn composio_source(id: &str, toolkit: &str, connection_id: &str) -> MemorySourceEntry {
    MemorySourceEntry {
        id: id.into(),
        kind: SourceKind::Composio,
        label: "Legacy Slack".into(),
        enabled: false,
        toolkit: Some(toolkit.into()),
        connection_id: Some(connection_id.into()),
        path: None,
        glob: None,
        url: None,
        branch: None,
        paths: Vec::new(),
        max_commits: None,
        max_issues: None,
        max_prs: None,
        query: None,
        since_days: None,
        max_items: Some(17),
        selector: None,
        max_tokens_per_sync: Some(321),
        max_cost_per_sync_usd: Some(0.09),
        sync_depth_days: Some(12),
    }
}
