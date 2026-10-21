use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::ingest::{ingest_chat, ChatBatch, ChatMessage, QueueJobSink};
use tinycortex::memory::score::ScoringConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let workspace = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_storage_fixture <output-workspace>")?;
    std::fs::create_dir_all(&workspace)
        .with_context(|| format!("create fixture workspace {}", workspace.display()))?;
    anyhow::ensure!(
        std::fs::read_dir(&workspace)?.next().is_none(),
        "fixture workspace must be empty: {}",
        workspace.display()
    );

    let config = MemoryConfig::new(&workspace);
    let timestamp = |millis| Utc.timestamp_millis_opt(millis).single().unwrap();
    let batch = ChatBatch {
        platform: "slack".into(),
        channel_label: "#migration-fixture".into(),
        messages: vec![
            ChatMessage {
                author: "Ada".into(),
                timestamp: timestamp(1_750_000_000_000),
                text: "The atlas import should preserve the original chunk and its owner metadata.".into(),
                source_ref: Some("slack://migration-fixture/001".into()),
            },
            ChatMessage {
                author: "Lin".into(),
                timestamp: timestamp(1_750_000_060_000),
                text: "I will verify the imported record and keep the historical source timestamp.".into(),
                source_ref: Some("slack://migration-fixture/002".into()),
            },
        ],
    };
    let result = ingest_chat(
        &config,
        "slack:#migration-fixture",
        "fixture-owner",
        vec!["migration_fixture".into()],
        batch,
        &QueueJobSink,
        &ScoringConfig::default_regex_only(),
    )
    .await?;
    anyhow::ensure!(result.chunks_written > 0, "public writer produced no chunks");
    println!(
        "wrote {} chunks and {} extract jobs to {}",
        result.chunks_written,
        result.extract_jobs_enqueued,
        workspace.display()
    );
    Ok(())
}
