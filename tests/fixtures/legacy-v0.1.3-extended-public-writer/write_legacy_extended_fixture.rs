use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::ingest::{ingest_chat, ChatBatch, ChatMessage, QueueJobSink};
use tinycortex::memory::score::ScoringConfig;
use tinycortex::memory::store::entity_index::{CanonicalEntity, EntityIndex, EntityKind};
use tinycortex::memory::tree::{ingest_summary, ConcatSummariser, SummaryIngestInput, TreeFactory};

#[tokio::main]
async fn main() -> Result<()> {
    let workspace = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_extended_fixture <output-workspace>")?;
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
    let ingest = ingest_chat(
        &config,
        "slack:#extended-migration-fixture",
        "fixture-owner",
        vec!["extended_migration_fixture".into()],
        ChatBatch {
            platform: "slack".into(),
            channel_label: "#extended-migration-fixture".into(),
            messages: vec![ChatMessage {
                author: "Ada".into(),
                timestamp: at,
                text: "Ada and Lin reviewed the migrated atlas record together.".into(),
                source_ref: Some("slack://extended-migration-fixture/001".into()),
            }],
        },
        &QueueJobSink,
        &ScoringConfig::default_regex_only(),
    )
    .await?;
    let chunk_id = ingest
        .chunk_ids
        .first()
        .context("public ingest writer produced no chunk")?
        .clone();

    let tree = TreeFactory::source("slack:#extended-migration-fixture").get_or_create(&config)?;
    let entity_specs = [
        ("person:ada", EntityKind::Person, "Ada"),
        ("person:lin", EntityKind::Person, "Lin"),
    ];
    let entity_ids: Vec<String> = entity_specs
        .iter()
        .map(|(id, _, _)| (*id).to_owned())
        .collect();
    let summary = ingest_summary(
        &config,
        &tree,
        SummaryIngestInput {
            content: "Ada and Lin reviewed the migrated atlas record together.".into(),
            token_count: 10,
            entities: entity_ids.clone(),
            topics: vec!["migration".into()],
            time_range_start: at,
            time_range_end: at,
            score: 0.9,
            child_labels: vec![chunk_id.clone()],
            child_basenames: vec![None],
        },
        &ConcatSummariser,
    )
    .await?;

    let index = EntityIndex::for_memory_config(&config)?;
    for (canonical_id, kind, surface) in entity_specs {
        let span_end = surface.len() as u32;
        index.index_entity(
            &CanonicalEntity {
                canonical_id: canonical_id.into(),
                kind,
                surface: surface.into(),
                span_start: 0,
                span_end,
                score: 1.0,
            },
            &summary.summary_id,
            "summary",
            at.timestamp_millis(),
            Some(&tree.id),
        )?;
    }
    let edge_ids = entity_ids;
    let edge_pairs = tinycortex::memory::graph::pairs_from_entities(&edge_ids);
    tinycortex::memory::graph::upsert_edges(&config, &edge_pairs, at.timestamp_millis())?;

    println!(
        "wrote {} chunks, one tree, one summary, two summary entities and {} graph edges to {}",
        ingest.chunks_written,
        edge_pairs.len(),
        workspace.display()
    );
    Ok(())
}
