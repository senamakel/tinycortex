use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use tinycortex::memory::chunks::mark_chunk_reembed_skipped;
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::ingest::{ingest_chat, ChatBatch, ChatMessage, QueueJobSink};
use tinycortex::memory::score::ScoringConfig;
use tinycortex::memory::tree::{
    append_leaf_deferred, ingest_summary, store::HotnessCounters, ConcatSummariser, LeafRef,
    SummaryIngestInput, TreeFactory,
};

#[tokio::main]
async fn main() -> Result<()> {
    let workspace = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_state_fixture <output-workspace>")?;
    std::fs::create_dir_all(&workspace)
        .with_context(|| format!("create fixture workspace {}", workspace.display()))?;
    anyhow::ensure!(
        std::fs::read_dir(&workspace)?.next().is_none(),
        "fixture workspace must be empty: {}",
        workspace.display()
    );

    let config = MemoryConfig::new(&workspace);
    let at = Utc
        .timestamp_millis_opt(1_750_000_000_000)
        .single()
        .unwrap();
    let pending = ingest(
        &config,
        "slack:#pending-frontier",
        "#pending-frontier",
        "A public-writer chunk remains in its unsealed frontier.",
    )
    .await?;
    let pending_chunk_id = pending
        .chunk_ids
        .first()
        .context("public chat writer produced no pending chunk")?
        .clone();
    let pending_tree = TreeFactory::source("slack:#pending-frontier").get_or_create(&config)?;
    let pending_chunk = tinycortex::memory::chunks::get_chunk(&config, &pending_chunk_id)?
        .context("public chat writer chunk could not be read back")?;
    append_leaf_deferred(
        &config,
        &pending_tree,
        &LeafRef {
            chunk_id: pending_chunk_id.clone(),
            token_count: pending_chunk.token_count,
            timestamp: pending_chunk.metadata.timestamp,
            content: pending_chunk.content.clone(),
            entities: Vec::new(),
            topics: Vec::new(),
            score: 0.8,
        },
    )?;

    let summary_ingest = ingest(
        &config,
        "slack:#pending-summary-frontier",
        "#pending-summary-frontier",
        "A separate public-writer summary keeps a durable retry marker.",
    )
    .await?;
    let summary_chunk_id = summary_ingest
        .chunk_ids
        .first()
        .context("public chat writer produced no summary child")?
        .clone();
    let summary_tree =
        TreeFactory::source("slack:#pending-summary-frontier").get_or_create(&config)?;
    let summary = ingest_summary(
        &config,
        &summary_tree,
        SummaryIngestInput {
            content: "A separate public-writer summary keeps a durable retry marker.".into(),
            token_count: 12,
            entities: vec!["topic:durable-migration".into()],
            topics: vec!["migration".into()],
            time_range_start: at,
            time_range_end: at,
            score: 0.9,
            child_labels: vec![summary_chunk_id.clone()],
            child_basenames: vec![None],
        },
        &ConcatSummariser,
    )
    .await?;

    tinycortex::memory::tree::store::hotness::upsert(
        &config,
        &HotnessCounters {
            entity_id: "topic:durable-migration".into(),
            mention_count_30d: 19,
            distinct_sources: 2,
            last_seen_ms: Some(at.timestamp_millis()),
            query_hits_30d: 5,
            graph_centrality: Some(0.625),
            ingests_since_check: 3,
            last_hotness: Some(12.25),
            last_updated_ms: at.timestamp_millis() + 321,
        },
    )?;
    tinycortex::memory::chunks::set_chunk_embedding_for_signature(
        &config,
        &summary_chunk_id,
        "legacy/provider@1536",
        &[0.125, 0.25, 0.5],
    )?;
    mark_chunk_reembed_skipped(
        &config,
        &pending_chunk_id,
        "legacy/provider@1536",
        "legacy public-writer chunk skip",
    )?;
    tinycortex::memory::chunks::mark_summary_reembed_skipped(
        &config,
        &summary.summary_id,
        "legacy/provider@1536",
        "legacy public-writer summary skip",
    )?;

    println!(
        "wrote pending chunk {} in tree {}, summary {} in tree {}, hotness and both re-embed skip rows to {}",
        pending_chunk_id,
        pending_tree.id,
        summary.summary_id,
        summary_tree.id,
        workspace.display()
    );
    Ok(())
}

async fn ingest(
    config: &MemoryConfig,
    source: &str,
    channel: &str,
    text: &str,
) -> Result<tinycortex::memory::ingest::IngestSummary> {
    Ok(ingest_chat(
        config,
        source,
        "fixture-owner",
        vec!["state_family_fixture".into()],
        ChatBatch {
            platform: "slack".into(),
            channel_label: channel.into(),
            messages: vec![ChatMessage {
                author: "Ada".into(),
                timestamp: Utc
                    .timestamp_millis_opt(1_750_000_000_000)
                    .single()
                    .unwrap(),
                text: text.into(),
                source_ref: Some(format!("slack://{channel}/001")),
            }],
        },
        &QueueJobSink,
        &ScoringConfig::default_regex_only(),
    )
    .await?)
}
