//! Sealed summary records and per-signature embedding sidecars in the
//! host-bound versioned document owner.
//!
//! Summary rows are immutable once emitted. Embeddings are separate records so
//! multiple model signatures can coexist without rewriting the summary.

use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;

use super::types::SummaryNode;
use crate::memory::chunks::{signature_variants, tree_active_signature};
use crate::memory::config::MemoryConfig;
use crate::memory::store::content::StagedSummary;

/// Fetch one summary by id. Soft-deleted rows are returned with `deleted = true`.
pub fn get_summary(config: &MemoryConfig, id: &str) -> Result<Option<SummaryNode>> {
    crate::memory::chunks::versioned::read_state(
        config,
        crate::memory::chunks::collections::SUMMARIES,
        &summary_key(id),
    )?
    .map(serde_json::from_value)
    .transpose()
    .map_err(Into::into)
}

/// Async counterpart to [`get_summary`].
pub async fn get_summary_async(config: &MemoryConfig, id: &str) -> Result<Option<SummaryNode>> {
    crate::memory::chunks::versioned::read_state_async(
        config,
        crate::memory::chunks::collections::SUMMARIES,
        &summary_key(id),
    )
    .await?
    .map(serde_json::from_value)
    .transpose()
    .map_err(Into::into)
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StagedSummaryChanges {
    pub(crate) expected: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) changes: Vec<crate::memory::chunks::versioned::StateChange>,
}

/// Build owner-state changes for an immutable summary and its staged-content
/// and per-model embedding records. Callers combine these with tree, buffer,
/// child-link, and entity-index mutations in one owner-head commit.
pub(crate) fn stage_summary_changes(
    node: &SummaryNode,
    staged: Option<&StagedSummary>,
    model_signature: &str,
) -> Result<StagedSummaryChanges> {
    use crate::memory::chunks::versioned::StateChange;

    let key = summary_key(&node.id);
    let mut stored_node = node.clone();
    // Preserve the old row shape: embeddings are per-signature sidecar data,
    // never part of the summary record itself.
    stored_node.embedding = None;
    let mut expected = vec![StateChange {
        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
        key: key.clone(),
        value: None,
    }];
    let mut changes = vec![StateChange {
        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
        key,
        value: Some(serde_json::to_value(stored_node)?),
    }];
    if let Some(staged) = staged {
        let key = content_pointer_key(&node.id);
        expected.push(StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_CONTENT_POINTERS.to_owned(),
            key: key.clone(),
            value: None,
        });
        changes.push(StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_CONTENT_POINTERS.to_owned(),
            key,
            value: Some(serde_json::json!({
                "summary_id": node.id,
                "content_path": staged.content_path,
                "content_sha256": staged.content_sha256,
            })),
        });
    }
    if let Some(embedding) = &node.embedding {
        let key = summary_embedding_key(&node.id, model_signature);
        expected.push(StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key: key.clone(),
            value: None,
        });
        changes.push(StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key,
            value: Some(serde_json::json!({
                "summary_id": node.id,
                "model_signature": model_signature,
                "vector": embedding,
                "dim": embedding.len(),
                "created_at_ms": Utc::now().timestamp_millis(),
            })),
        });
    }
    Ok(StagedSummaryChanges { expected, changes })
}

fn summary_key(id: &str) -> String {
    format!("summary:{id}")
}

fn summary_embedding_key(id: &str, signature: &str) -> String {
    format!("summary-embedding:{}:{id}:{signature}", id.len())
}

fn content_pointer_key(id: &str) -> String {
    format!("summary:{id}")
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct SummaryEmbeddingRecord {
    summary_id: String,
    model_signature: String,
    vector: Vec<f32>,
    dim: usize,
    created_at_ms: i64,
}

/// Fetch many summaries by id from one committed owner snapshot. Missing ids
/// are silently absent.
pub fn get_summaries_batch(
    config: &MemoryConfig,
    summary_ids: &[String],
) -> Result<HashMap<String, SummaryNode>> {
    if summary_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: std::collections::HashSet<_> = summary_ids.iter().map(String::as_str).collect();
    Ok(all_summaries(config)?
        .into_iter()
        .filter(|summary| wanted.contains(summary.id.as_str()))
        .map(|summary| (summary.id.clone(), summary))
        .collect())
}

/// Async counterpart to [`get_summaries_batch`].
pub async fn get_summaries_batch_async(
    config: &MemoryConfig,
    summary_ids: &[String],
) -> Result<HashMap<String, SummaryNode>> {
    if summary_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: std::collections::HashSet<_> = summary_ids.iter().map(String::as_str).collect();
    Ok(all_summaries_async(config)
        .await?
        .into_iter()
        .filter(|summary| wanted.contains(summary.id.as_str()))
        .map(|summary| (summary.id.clone(), summary))
        .collect())
}

/// List sealed summaries for a tree at a level, ordered by `sealed_at` ASC.
/// Skips tombstoned rows.
pub fn list_summaries_at_level(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries(config)?
        .into_iter()
        .filter(|summary| summary.tree_id == tree_id && summary.level == level && !summary.deleted)
        .collect();
    rows.sort_by_key(|summary| summary.sealed_at);
    Ok(rows)
}

/// Async counterpart to [`list_summaries_at_level`].
pub async fn list_summaries_at_level_async(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries_async(config)
        .await?
        .into_iter()
        .filter(|summary| summary.tree_id == tree_id && summary.level == level && !summary.deleted)
        .collect();
    rows.sort_by_key(|summary| summary.sealed_at);
    Ok(rows)
}

/// List every non-deleted summary whose time-range envelope is fully contained
/// in `[since_ms, until_ms]` (inclusive), across all levels.
pub fn list_summaries_in_window(
    config: &MemoryConfig,
    tree_id: &str,
    since_ms: i64,
    until_ms: i64,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries(config)?
        .into_iter()
        .filter(|summary| {
            summary.tree_id == tree_id
                && !summary.deleted
                && summary.time_range_start.timestamp_millis() >= since_ms
                && summary.time_range_end.timestamp_millis() <= until_ms
        })
        .collect();
    rows.sort_by_key(|summary| (summary.level, summary.time_range_start));
    Ok(rows)
}

/// Async counterpart to [`list_summaries_in_window`].
pub async fn list_summaries_in_window_async(
    config: &MemoryConfig,
    tree_id: &str,
    since_ms: i64,
    until_ms: i64,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries_async(config)
        .await?
        .into_iter()
        .filter(|summary| {
            summary.tree_id == tree_id
                && !summary.deleted
                && summary.time_range_start.timestamp_millis() >= since_ms
                && summary.time_range_end.timestamp_millis() <= until_ms
        })
        .collect();
    rows.sort_by_key(|summary| (summary.level, summary.time_range_start));
    Ok(rows)
}

/// List non-deleted summaries whose time envelope overlaps the inclusive
/// `[since_ms, until_ms]` window, across every summary level in one tree.
///
/// Unlike [`list_summaries_in_window`], which requires full containment for
/// cover construction, this overlap query is intended for retrieval.
pub fn list_summaries_overlapping_window(
    config: &MemoryConfig,
    tree_id: &str,
    since_ms: i64,
    until_ms: i64,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries(config)?
        .into_iter()
        .filter(|summary| {
            summary.tree_id == tree_id
                && !summary.deleted
                && summary.level >= 1
                && summary.time_range_end.timestamp_millis() >= since_ms
                && summary.time_range_start.timestamp_millis() <= until_ms
        })
        .collect();
    rows.sort_by_key(|summary| (summary.level, summary.time_range_start));
    Ok(rows)
}

/// Async counterpart to [`list_summaries_overlapping_window`].
pub async fn list_summaries_overlapping_window_async(
    config: &MemoryConfig,
    tree_id: &str,
    since_ms: i64,
    until_ms: i64,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries_async(config)
        .await?
        .into_iter()
        .filter(|summary| {
            summary.tree_id == tree_id
                && !summary.deleted
                && summary.level >= 1
                && summary.time_range_end.timestamp_millis() >= since_ms
                && summary.time_range_start.timestamp_millis() <= until_ms
        })
        .collect();
    rows.sort_by_key(|summary| (summary.level, summary.time_range_start));
    Ok(rows)
}

/// List non-deleted summaries whose `parent_id` is `parent` (its direct
/// children). Ordered by `sealed_at` ASC. Used by the tree-walk read path.
pub fn list_children_of_summary(
    config: &MemoryConfig,
    parent_id: &str,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries(config)?
        .into_iter()
        .filter(|summary| summary.parent_id.as_deref() == Some(parent_id) && !summary.deleted)
        .collect();
    rows.sort_by_key(|summary| summary.sealed_at);
    Ok(rows)
}

/// Async counterpart to [`list_children_of_summary`].
pub async fn list_children_of_summary_async(
    config: &MemoryConfig,
    parent_id: &str,
) -> Result<Vec<SummaryNode>> {
    let mut rows: Vec<_> = all_summaries_async(config)
        .await?
        .into_iter()
        .filter(|summary| summary.parent_id.as_deref() == Some(parent_id) && !summary.deleted)
        .collect();
    rows.sort_by_key(|summary| summary.sealed_at);
    Ok(rows)
}

/// Count non-deleted summaries in a tree (diagnostic helper).
pub fn count_summaries(config: &MemoryConfig, tree_id: &str) -> Result<u64> {
    Ok(all_summaries(config)?
        .iter()
        .filter(|summary| summary.tree_id == tree_id && !summary.deleted)
        .count() as u64)
}

/// Async counterpart to [`count_summaries`].
pub async fn count_summaries_async(config: &MemoryConfig, tree_id: &str) -> Result<u64> {
    Ok(all_summaries_async(config)
        .await?
        .iter()
        .filter(|summary| summary.tree_id == tree_id && !summary.deleted)
        .count() as u64)
}

fn all_summaries(config: &MemoryConfig) -> Result<Vec<SummaryNode>> {
    crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::SUMMARIES,
    )?
    .into_iter()
    .filter(|(key, _)| key.starts_with("summary:"))
    .map(|(_, value)| serde_json::from_value(value).map_err(Into::into))
    .collect()
}

async fn all_summaries_async(config: &MemoryConfig) -> Result<Vec<SummaryNode>> {
    crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::SUMMARIES,
    )
    .await?
    .into_iter()
    .filter(|(key, _)| key.starts_with("summary:"))
    .map(|(_, value)| serde_json::from_value(value).map_err(Into::into))
    .collect()
}

// ── Embedding sidecar ───────────────────────────────────────────────────────

/// Store a summary embedding for a specific provider/model/dimension signature.
pub fn set_summary_embedding_for_signature(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
    embedding: &[f32],
) -> Result<()> {
    let key = summary_embedding_key(summary_id, model_signature);
    for _ in 0..8 {
        let current = crate::memory::chunks::versioned::read_state(
            config,
            crate::memory::chunks::collections::SUMMARY_EMBEDDINGS,
            &key,
        )?;
        let expected = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key: key.clone(),
            value: current,
        }];
        let record = SummaryEmbeddingRecord {
            summary_id: summary_id.to_owned(),
            model_signature: model_signature.to_owned(),
            vector: embedding.to_vec(),
            dim: embedding.len(),
            created_at_ms: Utc::now().timestamp_millis(),
        };
        let changes = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key: key.clone(),
            value: Some(serde_json::to_value(record)?),
        }];
        if crate::memory::chunks::versioned::commit_if_state_matches(
            config,
            &expected,
            &[],
            &changes,
        )? {
            return Ok(());
        }
    }
    anyhow::bail!("summary embedding changed repeatedly during update")
}

/// Store a summary embedding under the active model signature.
pub fn set_summary_embedding(
    config: &MemoryConfig,
    summary_id: &str,
    embedding: &[f32],
) -> Result<usize> {
    let signature = tree_active_signature(config);
    set_summary_embedding_for_signature(config, summary_id, &signature, embedding)?;
    Ok(1)
}

/// Fetch a summary embedding for one signature, under any of its spellings
/// (see `chunks::signature_variants`).
pub fn get_summary_embedding_for_signature(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
) -> Result<Option<Vec<f32>>> {
    let variants = signature_variants(model_signature);
    let keys: Vec<_> = variants
        .iter()
        .map(|signature| crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key: summary_embedding_key(summary_id, signature),
        })
        .collect();
    let values = crate::memory::chunks::versioned::read_state_keys(config, &keys)?;
    for value in values.into_iter().flatten() {
        let record: SummaryEmbeddingRecord = serde_json::from_value(value)?;
        anyhow::ensure!(
            record.dim == record.vector.len(),
            "summary embedding dimension mismatch"
        );
        return Ok(Some(record.vector));
    }
    Ok(None)
}

/// Async counterpart to [`get_summary_embedding_for_signature`].
pub async fn get_summary_embedding_for_signature_async(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
) -> Result<Option<Vec<f32>>> {
    let keys: Vec<_> = signature_variants(model_signature)
        .into_iter()
        .map(|signature| crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::SUMMARY_EMBEDDINGS.to_owned(),
            key: summary_embedding_key(summary_id, &signature),
        })
        .collect();
    let values = crate::memory::chunks::versioned::read_state_keys_async(config, &keys).await?;
    for value in values.into_iter().flatten() {
        let record: SummaryEmbeddingRecord = serde_json::from_value(value)?;
        anyhow::ensure!(
            record.dim == record.vector.len(),
            "summary embedding dimension mismatch"
        );
        return Ok(Some(record.vector));
    }
    Ok(None)
}

/// Fetch a summary embedding under the active model signature.
pub fn get_summary_embedding(config: &MemoryConfig, summary_id: &str) -> Result<Option<Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_summary_embedding_for_signature(config, summary_id, &signature)
}

/// Async counterpart to [`get_summary_embedding`].
pub async fn get_summary_embedding_async(
    config: &MemoryConfig,
    summary_id: &str,
) -> Result<Option<Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_summary_embedding_for_signature_async(config, summary_id, &signature).await
}

/// Batched read of summary embeddings under one signature. The map contains
/// only ids with a non-null vector under `model_signature`.
pub fn get_summary_embeddings_for_signature_batch(
    config: &MemoryConfig,
    summary_ids: &[String],
    model_signature: &str,
) -> Result<HashMap<String, Vec<f32>>> {
    if summary_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let ids: std::collections::HashSet<_> = summary_ids.iter().map(String::as_str).collect();
    let signatures: std::collections::HashSet<_> =
        signature_variants(model_signature).into_iter().collect();
    let rows = crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::SUMMARY_EMBEDDINGS,
    )?;
    let mut out = HashMap::with_capacity(summary_ids.len());
    for (key, value) in rows {
        if !key.starts_with("summary-embedding:") {
            continue;
        }
        let record: SummaryEmbeddingRecord = serde_json::from_value(value)?;
        if ids.contains(record.summary_id.as_str())
            && signatures.contains(record.model_signature.as_str())
        {
            anyhow::ensure!(
                record.dim == record.vector.len(),
                "summary embedding dimension mismatch"
            );
            out.insert(record.summary_id, record.vector);
        }
    }
    Ok(out)
}

/// Async counterpart to [`get_summary_embeddings_for_signature_batch`].
pub async fn get_summary_embeddings_for_signature_batch_async(
    config: &MemoryConfig,
    summary_ids: &[String],
    model_signature: &str,
) -> Result<HashMap<String, Vec<f32>>> {
    if summary_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let ids: std::collections::HashSet<_> = summary_ids.iter().map(String::as_str).collect();
    let signatures: std::collections::HashSet<_> =
        signature_variants(model_signature).into_iter().collect();
    let rows = crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::SUMMARY_EMBEDDINGS,
    )
    .await?;
    let mut out = HashMap::with_capacity(summary_ids.len());
    for (key, value) in rows {
        if !key.starts_with("summary-embedding:") {
            continue;
        }
        let record: SummaryEmbeddingRecord = serde_json::from_value(value)?;
        if ids.contains(record.summary_id.as_str())
            && signatures.contains(record.model_signature.as_str())
        {
            anyhow::ensure!(
                record.dim == record.vector.len(),
                "summary embedding dimension mismatch"
            );
            out.insert(record.summary_id, record.vector);
        }
    }
    Ok(out)
}

/// Batched read under the active signature.
pub fn get_summary_embeddings_batch(
    config: &MemoryConfig,
    summary_ids: &[String],
) -> Result<HashMap<String, Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_summary_embeddings_for_signature_batch(config, summary_ids, &signature)
}

/// Async counterpart to [`get_summary_embeddings_batch`].
pub async fn get_summary_embeddings_batch_async(
    config: &MemoryConfig,
    summary_ids: &[String],
) -> Result<HashMap<String, Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_summary_embeddings_for_signature_batch_async(config, summary_ids, &signature).await
}

// ── Row decoding ────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "summaries_tests.rs"]
mod tests;
