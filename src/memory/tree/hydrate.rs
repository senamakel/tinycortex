//! Input hydration for seals and raw-leaf fetches.
//!
//! At L0 a buffer holds chunk ids; at L≥1 it holds summary ids. These helpers
//! resolve those ids into [`SummaryInput`]s the summariser folds. Unlike
//! Tree inputs are resolved through the host-owned chunk/summary stores. A
//! summary's durable body is held in its versioned owner record; optional
//! markdown projection belongs to a host after commit.

use anyhow::Result;

use crate::memory::chunks::{get_chunks_batch, get_chunks_batch_async, Chunk};
use crate::memory::config::MemoryConfig;
use crate::memory::score::store::{get_score_async, list_entity_ids_for_node_async};
use crate::memory::tree::store::get_summaries_batch_async;
use crate::memory::tree::summarise::SummaryInput;

/// Hydrate inputs for a seal. Level 0 pulls from chunk, score, and entity
/// owner state; levels ≥1 pull from versioned summary records.
pub(crate) async fn hydrate_inputs(
    config: &MemoryConfig,
    level: u32,
    item_ids: &[String],
) -> Result<Vec<SummaryInput>> {
    if level == 0 {
        hydrate_leaf_inputs_async(config, item_ids).await
    } else {
        hydrate_summary_inputs_async(config, item_ids).await
    }
}

async fn hydrate_leaf_inputs_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<Vec<SummaryInput>> {
    let chunks = get_chunks_batch_async(config, chunk_ids).await?;
    let mut out = Vec::with_capacity(chunk_ids.len());
    for id in chunk_ids {
        let Some(chunk) = chunks.get(id) else {
            continue;
        };
        let score = get_score_async(config, id)
            .await?
            .map(|row| row.total)
            .unwrap_or(0.0);
        let entities = list_entity_ids_for_node_async(config, id).await?;
        out.push(SummaryInput {
            id: chunk.id.clone(),
            content: chunk.content.clone(),
            token_count: chunk.token_count,
            entities,
            topics: chunk.metadata.tags.clone(),
            time_range_start: chunk.metadata.time_range.0,
            time_range_end: chunk.metadata.time_range.1,
            score,
        });
    }
    Ok(out)
}

pub(crate) async fn hydrate_summary_inputs_async(
    config: &MemoryConfig,
    summary_ids: &[String],
) -> Result<Vec<SummaryInput>> {
    // One batched `SELECT … WHERE id IN (?,…)`. Walk the caller's slice (not the
    // map) so input order is preserved; missing ids are silently skipped.
    let node_by_id = get_summaries_batch_async(config, summary_ids).await?;
    let mut out: Vec<SummaryInput> = Vec::with_capacity(summary_ids.len());
    for id in summary_ids {
        let Some(node) = node_by_id.get(id) else {
            continue;
        };
        let content = crate::memory::store::content::read_summary_body_async(config, &node.id)
            .await
            .unwrap_or_else(|error| {
                log::warn!(
                    "[memory_tree:hydrate] staged summary read failed id={}: {error}; using owner-state content",
                    node.id
                );
                node.content.clone()
            });
        out.push(SummaryInput {
            id: node.id.clone(),
            content,
            token_count: node.token_count,
            entities: node.entities.clone(),
            topics: node.topics.clone(),
            time_range_start: node.time_range_start,
            time_range_end: node.time_range_end,
            score: node.score,
        });
    }
    Ok(out)
}

/// Hydrate raw chunk leaves by id, capped at `cap` results. Returns the chunks
/// in the order requested, skipping any that no longer exist. A `cap` of `0`
/// returns an empty vec.
pub fn fetch_leaves(config: &MemoryConfig, chunk_ids: &[String], cap: usize) -> Result<Vec<Chunk>> {
    if cap == 0 || chunk_ids.is_empty() {
        return Ok(Vec::new());
    }
    let by_id = get_chunks_batch(config, chunk_ids)?;
    let mut out = Vec::with_capacity(chunk_ids.len().min(cap));
    for id in chunk_ids {
        if out.len() >= cap {
            break;
        }
        if let Some(c) = by_id.get(id) {
            out.push(c.clone());
        }
    }
    Ok(out)
}
