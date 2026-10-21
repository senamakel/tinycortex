//! Per-(chunk, embedding-model) vector accessors and re-embed tombstones.
//!
//! Chunk embeddings and re-embed skips are versioned host-document records
//! keyed by `(chunk_id, model_signature)` so multiple vector spaces can
//! coexist. This module does not compute embeddings — callers pass vectors in.
//! Summary embeddings use their own owner-state collection. Signature-aware
//! chunk reads live in the
//! [`super::embeddings_query`] sibling.

use super::collections::{CHUNK_EMBEDDINGS, CHUNK_REEMBED_SKIPS, SUMMARY_REEMBED_SKIPS};
use super::signature::format_signature;
use super::versioned::{list_state, write_chunk_state_changes, write_state_changes, StateChange};
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::memory::config::MemoryConfig;

/// Resolve the active embedding signature — the canonical key every per-model
/// sidecar read/write is scoped by. Derived from the configured provider, model
/// and dim so a provider/model/dimension switch becomes a query-time filter
/// rather than a destructive rewrite.
///
/// This is the canonical `provider=…;model=…;dims=…` spelling, shared with the
/// namespace store. Rows written under the tree's older `{model}@{dims}`
/// spelling are still found, because per-signature reads match every variant
/// (see [`super::signature::signature_variants`]) rather than one exact string.
pub fn tree_active_signature(config: &MemoryConfig) -> String {
    format_signature(
        &config.embedding.provider,
        &config.embedding.model,
        config.embedding.dim,
    )
}

/// Store a chunk's embedding under the active model signature (see
/// [`tree_active_signature`]).
///
/// Upserts: calling this again for the same `chunk_id` (under the same active
/// signature) replaces the stored vector rather than erroring or duplicating.
///
/// # Errors
/// Returns `Err` if `embedding.len()` does not fit in an `i64` (practically
/// unreachable), or if the underlying `INSERT ... ON CONFLICT` fails.
pub fn set_chunk_embedding(config: &MemoryConfig, chunk_id: &str, embedding: &[f32]) -> Result<()> {
    let signature = tree_active_signature(config);
    set_chunk_embedding_for_signature(config, chunk_id, &signature, embedding)
}

/// Store a chunk embedding for a specific provider/model/dimension signature.
///
/// Upserts by `(chunk_id, model_signature)` — safe to call repeatedly for the
/// same key; the latest call wins.
///
/// # Errors
/// Returns an error if the dimension cannot be represented or the host store
/// rejects the write. The target chunk must exist at the committed head.
pub fn set_chunk_embedding_for_signature(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
    embedding: &[f32],
) -> Result<()> {
    let dim = i64::try_from(embedding.len()).context("embedding dimension does not fit i64")?;
    let key = chunk_embedding_key(chunk_id, model_signature);
    let record = ChunkEmbeddingRecord {
        chunk_id: chunk_id.to_owned(),
        model_signature: model_signature.to_owned(),
        vector: embedding_to_blob(embedding),
        dim,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    if !write_chunk_state_changes(
        config,
        chunk_id,
        &[
            StateChange {
                collection: CHUNK_EMBEDDINGS.into(),
                key: key.clone(),
                value: Some(serde_json::to_value(record)?),
            },
            StateChange {
                collection: CHUNK_REEMBED_SKIPS.into(),
                key,
                value: None,
            },
        ],
    )? {
        anyhow::bail!("cannot store an embedding for missing chunk {chunk_id}");
    }
    Ok(())
}

/// Plan one queue-owned embedding write without publishing it.
///
/// The lifecycle row is carried as a guarded no-op mutation so a concurrent
/// chunk deletion fences the vector write during the same lease settlement.
/// The existing vector is replaced and any matching re-embed skip is cleared.
pub async fn plan_chunk_embedding_for_queue(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
    embedding: &[f32],
) -> Result<crate::memory::queue::DelegateOutput<()>> {
    let chunk_id = validate_reembed_skip_key("chunk_id", chunk_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    let dim = i64::try_from(embedding.len()).context("embedding dimension does not fit i64")?;
    let key = chunk_embedding_key(chunk_id, model_signature);
    let _chunk = super::versioned::get_chunk_async(config, chunk_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("cannot plan an embedding for missing chunk {chunk_id}"))?;
    let keys = [
        super::versioned::StateKey {
            collection: super::collections::CHUNK_LIFECYCLE.to_owned(),
            key: chunk_id.to_owned(),
        },
        super::versioned::StateKey {
            collection: CHUNK_EMBEDDINGS.to_owned(),
            key: key.clone(),
        },
        super::versioned::StateKey {
            collection: CHUNK_REEMBED_SKIPS.to_owned(),
            key: key.clone(),
        },
    ];
    let values = super::versioned::read_state_keys_async(config, &keys).await?;
    anyhow::ensure!(
        values[0]
            .as_ref()
            .and_then(|value| value.get("status"))
            .and_then(serde_json::Value::as_str)
            != Some("deleted"),
        "cannot plan an embedding for deleted chunk {chunk_id}"
    );
    let record = ChunkEmbeddingRecord {
        chunk_id: chunk_id.to_owned(),
        model_signature: model_signature.to_owned(),
        vector: embedding_to_blob(embedding),
        dim,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    use crate::memory::queue::{JobMutation, JobStateCollection as Collection};
    Ok(crate::memory::queue::DelegateOutput {
        result: (),
        mutations: vec![
            JobMutation {
                collection: Collection::ChunkLifecycle,
                key: chunk_id.to_owned(),
                expected: values[0].clone(),
                value: values[0].clone(),
            },
            JobMutation {
                collection: Collection::ChunkEmbeddings,
                key: key.clone(),
                expected: values[1].clone(),
                value: Some(serde_json::to_value(record)?),
            },
            JobMutation {
                collection: Collection::ChunkReembedSkips,
                key,
                expected: values[2].clone(),
                value: None,
            },
        ],
    })
}

/// Persistently record that `(chunk_id, signature)` cannot be re-embedded so a
/// future backfill worklist can exclude it instead of looping on the same row.
///
/// Upserts by `(chunk_id, model_signature)`: a repeat call for the same key
/// overwrites `reason` and `skipped_at_ms` rather than erroring.
///
/// # Errors
/// Returns `Err` if `chunk_id` or `model_signature` fails
/// `validate_reembed_skip_key` (empty after trimming, over
/// `REEMBED_SKIP_KEY_MAX_LEN`, or contains a NUL byte), or if the `INSERT`
/// fails.
pub fn mark_chunk_reembed_skipped(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
    reason: &str,
) -> Result<()> {
    let chunk_id = validate_reembed_skip_key("chunk_id", chunk_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    let key = chunk_embedding_key(chunk_id, model_signature);
    if !write_chunk_state_changes(
        config,
        chunk_id,
        &[StateChange {
            collection: CHUNK_REEMBED_SKIPS.into(),
            key,
            value: Some(serde_json::to_value(ChunkReembedSkip {
                chunk_id: chunk_id.into(),
                model_signature: model_signature.into(),
                reason: reason.into(),
                skipped_at_ms: Utc::now().timestamp_millis(),
            })?),
        }],
    )? {
        anyhow::bail!("cannot mark re-embedding skip for missing chunk {chunk_id}");
    }
    Ok(())
}

/// Remove a single chunk tombstone so re-embed backfill can retry the row.
/// Idempotent — deleting a non-existent tombstone is a successful no-op.
///
/// # Errors
/// Returns `Err` if key validation fails (see
/// [`mark_chunk_reembed_skipped`]) or the `DELETE` fails.
pub fn clear_chunk_reembed_skipped(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
) -> Result<()> {
    let chunk_id = validate_reembed_skip_key("chunk_id", chunk_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    write_state_changes(
        config,
        &[StateChange {
            collection: CHUNK_REEMBED_SKIPS.into(),
            key: chunk_embedding_key(chunk_id, model_signature),
            value: None,
        }],
    )
}

pub fn mark_summary_reembed_skipped(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
    reason: &str,
) -> Result<()> {
    let summary_id = validate_reembed_skip_key("summary_id", summary_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    write_state_changes(
        config,
        &[StateChange {
            collection: SUMMARY_REEMBED_SKIPS.into(),
            key: summary_embedding_key(summary_id, model_signature),
            value: Some(serde_json::to_value(SummaryReembedSkip {
                summary_id: summary_id.into(),
                model_signature: model_signature.into(),
                reason: reason.into(),
                skipped_at_ms: Utc::now().timestamp_millis(),
            })?),
        }],
    )
}

/// Async current-thread-safe counterpart to [`mark_summary_reembed_skipped`].
pub async fn mark_summary_reembed_skipped_async(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
    reason: &str,
) -> Result<()> {
    let summary_id = validate_reembed_skip_key("summary_id", summary_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    super::versioned::commit_if_state_matches_async(
        config,
        &[],
        &[],
        &[StateChange {
            collection: SUMMARY_REEMBED_SKIPS.into(),
            key: summary_reembed_skip_key(summary_id, model_signature),
            value: Some(serde_json::to_value(SummaryReembedSkip {
                summary_id: summary_id.into(),
                model_signature: model_signature.into(),
                reason: reason.into(),
                skipped_at_ms: Utc::now().timestamp_millis(),
            })?),
        }],
    )
    .await?;
    Ok(())
}

pub fn clear_summary_reembed_skipped(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
) -> Result<()> {
    let summary_id = validate_reembed_skip_key("summary_id", summary_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    write_state_changes(
        config,
        &[StateChange {
            collection: SUMMARY_REEMBED_SKIPS.into(),
            key: summary_embedding_key(summary_id, model_signature),
            value: None,
        }],
    )
}

/// Async current-thread-safe counterpart to [`clear_summary_reembed_skipped`].
pub async fn clear_summary_reembed_skipped_async(
    config: &MemoryConfig,
    summary_id: &str,
    model_signature: &str,
) -> Result<()> {
    let summary_id = validate_reembed_skip_key("summary_id", summary_id)?;
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    super::versioned::commit_if_state_matches_async(
        config,
        &[],
        &[],
        &[StateChange {
            collection: SUMMARY_REEMBED_SKIPS.into(),
            key: summary_reembed_skip_key(summary_id, model_signature),
            value: None,
        }],
    )
    .await?;
    Ok(())
}

/// Clear all chunk and summary tombstones for a model signature. Returns the
/// total number of rows removed across both tombstone tables. Idempotent —
/// calling again with nothing left to clear returns `Ok(0)`.
///
/// # Errors
/// Returns `Err` if `model_signature` fails `validate_reembed_skip_key` or
/// either `DELETE` fails.
pub fn clear_reembed_skipped_for_signature(
    config: &MemoryConfig,
    model_signature: &str,
) -> Result<usize> {
    let model_signature = validate_reembed_skip_key("model_signature", model_signature)?;
    let chunk_deletes = list_state(config, CHUNK_REEMBED_SKIPS)?
        .into_iter()
        .map(|(key, value)| {
            let skip: ChunkReembedSkip = serde_json::from_value(value)
                .with_context(|| format!("decode chunk re-embed skip {key}"))?;
            Ok::<_, anyhow::Error>(skip.model_signature.eq(model_signature).then_some(
                StateChange {
                    collection: CHUNK_REEMBED_SKIPS.into(),
                    key,
                    value: None,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let chunk_deleted = chunk_deletes.len();
    write_state_changes(config, &chunk_deletes)?;
    let summary_deletes = list_state(config, SUMMARY_REEMBED_SKIPS)?
        .into_iter()
        .map(|(key, value)| {
            let skip: SummaryReembedSkip = serde_json::from_value(value)
                .with_context(|| format!("decode summary re-embed skip {key}"))?;
            Ok::<_, anyhow::Error>(skip.model_signature.eq(model_signature).then_some(
                StateChange {
                    collection: SUMMARY_REEMBED_SKIPS.into(),
                    key,
                    value: None,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let summary_deleted = summary_deletes.len();
    write_state_changes(config, &summary_deletes)?;
    Ok(chunk_deleted + summary_deleted)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ChunkEmbeddingRecord {
    pub(super) chunk_id: String,
    pub(super) model_signature: String,
    pub(super) vector: Vec<u8>,
    pub(super) dim: i64,
    pub(super) created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ChunkReembedSkip {
    pub(super) chunk_id: String,
    pub(super) model_signature: String,
    pub(super) reason: String,
    pub(super) skipped_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SummaryReembedSkip {
    pub(super) summary_id: String,
    pub(super) model_signature: String,
    pub(super) reason: String,
    pub(super) skipped_at_ms: i64,
}

pub(super) fn chunk_embedding_key(chunk_id: &str, model_signature: &str) -> String {
    serde_json::to_string(&(chunk_id, model_signature))
        .expect("a pair of strings always serializes")
}

pub(super) fn summary_embedding_key(summary_id: &str, model_signature: &str) -> String {
    serde_json::to_string(&(summary_id, model_signature))
        .expect("a pair of strings always serializes")
}

pub(crate) fn summary_reembed_skip_key(summary_id: &str, model_signature: &str) -> String {
    serde_json::to_string(&(summary_id, model_signature))
        .expect("a pair of strings always serializes")
}

/// Bounds attacker-controlled ids/signatures passed to reembed-skipped admin
/// helpers. Rejects NUL bytes so record keys cannot be ambiguous.
pub const REEMBED_SKIP_KEY_MAX_LEN: usize = 2048;

/// Validate and trim a `chunk_id` / `model_signature` value before it reaches
/// a reembed-skipped table query. `label` is only used to make the error
/// message identify which argument failed.
///
/// # Errors
/// Returns `Err` if `value`, after trimming, is empty, exceeds
/// `REEMBED_SKIP_KEY_MAX_LEN` bytes, or contains a NUL byte.
pub(crate) fn validate_reembed_skip_key<'a>(label: &str, value: &'a str) -> Result<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{label} must be non-empty");
    }
    if trimmed.len() > REEMBED_SKIP_KEY_MAX_LEN {
        anyhow::bail!("{label} exceeds maximum length ({REEMBED_SKIP_KEY_MAX_LEN})");
    }
    if trimmed.as_bytes().contains(&0) {
        anyhow::bail!("{label} must not contain NUL bytes");
    }
    Ok(trimmed)
}

/// Little-endian `f32` vector → `BLOB`. The inverse of `embedding_from_blob`.
pub fn embedding_to_blob(embedding: &[f32]) -> Vec<u8> {
    embedding.iter().flat_map(|f| f.to_le_bytes()).collect()
}
