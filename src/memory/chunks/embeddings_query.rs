//! Signature-aware host-document embedding **read** queries for chunk vectors.
//!
//! Split out of `embeddings` so that module stays a focused writer-side
//! accessor and re-embed tombstone store, and this one owns the per-signature
//! read paths. Every read matches *any* spelling of a vector space (see
//! [`super::signature::signature_variants`]) so a store that changed
//! conventions can still see its own prior vectors.
//!
//! No embeddings are computed here — embeddings.rs takes vectors on the write
//! side; this module only reads them back. Re-embed coverage uses one host
//! commit snapshot for chunks, summaries, vectors, and skip records.

use super::collections::{
    CHUNK_EMBEDDINGS, CHUNK_REEMBED_SKIPS, SUMMARIES, SUMMARY_EMBEDDINGS, SUMMARY_REEMBED_SKIPS,
};
use super::embeddings::{chunk_embedding_key, tree_active_signature, ChunkEmbeddingRecord};
use super::signature::signature_variants;
use super::versioned::{
    list_chunk_snapshot_with_head, list_chunk_snapshot_with_head_async, read_state, read_states,
    read_states_async,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

use crate::memory::config::MemoryConfig;
use crate::memory::store::content::read::{
    LEGACY_EMPTY_CONTENT_POINTER_REASON_PREFIX, LEGACY_NO_CONTENT_POINTER_REASON_PREFIX,
};

/// Fetch a chunk embedding for one provider/model/dimension signature — under
/// any of its spellings (see [`signature_variants`]).
///
/// Returns `Ok(None)` when no row exists for `(chunk_id, model_signature)` —
/// absence is not an error.
///
/// # Errors
/// Returns `Err` if the query fails, or if `embedding_from_blob` rejects
/// the stored blob (negative/zero-remainder-mismatched dim, or a blob length
/// not a multiple of 4 bytes — both indicate on-disk corruption of this row,
/// not a normal "no embedding" state).
pub fn get_chunk_embedding_for_signature(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
) -> Result<Option<Vec<f32>>> {
    let variants = signature_variants(model_signature);
    for variant in variants {
        let Some(value) = read_state(
            config,
            CHUNK_EMBEDDINGS,
            &chunk_embedding_key(chunk_id, &variant),
        )?
        else {
            continue;
        };
        let record: ChunkEmbeddingRecord = serde_json::from_value(value)?;
        if record.chunk_id == chunk_id && record.model_signature == variant {
            return embedding_from_blob(&record.vector, record.dim, "chunk embedding");
        }
    }
    Ok(None)
}

/// Async counterpart to [`get_chunk_embedding_for_signature`].
pub async fn get_chunk_embedding_for_signature_async(
    config: &MemoryConfig,
    chunk_id: &str,
    model_signature: &str,
) -> Result<Option<Vec<f32>>> {
    let variants = signature_variants(model_signature);
    let keys = variants
        .iter()
        .map(|variant| chunk_embedding_key(chunk_id, variant))
        .collect::<Vec<_>>();
    let values = read_states_async(config, CHUNK_EMBEDDINGS, &keys).await?;
    for (variant, key) in variants.iter().zip(keys) {
        let Some(value) = values.get(&key) else {
            continue;
        };
        let record: ChunkEmbeddingRecord = serde_json::from_value(value.clone())?;
        if record.chunk_id == chunk_id && record.model_signature.as_str() == variant.as_str() {
            return embedding_from_blob(&record.vector, record.dim, "chunk embedding");
        }
    }
    Ok(None)
}

/// Fetch a chunk's embedding for the active model signature (see
/// [`tree_active_signature`]). See [`get_chunk_embedding_for_signature`] for
/// the return/error contract.
pub fn get_chunk_embedding(config: &MemoryConfig, chunk_id: &str) -> Result<Option<Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_chunk_embedding_for_signature(config, chunk_id, &signature)
}

/// Async counterpart to [`get_chunk_embedding`].
pub async fn get_chunk_embedding_async(
    config: &MemoryConfig,
    chunk_id: &str,
) -> Result<Option<Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_chunk_embedding_for_signature_async(config, chunk_id, &signature).await
}

/// Decode a little-endian `f32` vector `BLOB` back into `Vec<f32>`, validating
/// it against the DB's own recorded `dim` column. `label` only qualifies the
/// error message (e.g. `"chunk embedding"` vs. a future summary-embedding
/// caller).
///
/// Always returns `Ok(Some(_))` on success — the `Option` in the return type
/// exists purely so callers can `?`-propagate this directly from inside a
/// `match row { None => Ok(None), Some(..) => embedding_from_blob(..) }` arm
/// without an extra `.map`.
///
/// # Errors
/// Returns `Err` if `dim` is negative, if `bytes.len()` is not a multiple of
/// 4, or if the decoded float count does not equal `dim` — all three
/// indicate the stored row is internally inconsistent (corruption or a bug
/// upstream), not a normal "different embedding space" mismatch (that case is
/// handled by comparing signatures/dims *before* calling this, e.g. in
/// [`super::migrations::migrate_legacy_embeddings_to_sidecar`]).
fn embedding_from_blob(bytes: &[u8], dim: i64, label: &str) -> Result<Option<Vec<f32>>> {
    if dim < 0 {
        anyhow::bail!("{label} has negative dimension {dim}");
    }
    if !bytes.len().is_multiple_of(4) {
        anyhow::bail!("{label} blob length {} not a multiple of 4", bytes.len());
    }
    let floats: Vec<f32> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect();
    if floats.len() != dim as usize {
        anyhow::bail!(
            "{label} dimension mismatch: dim column says {dim}, blob contains {} floats",
            floats.len()
        );
    }
    Ok(Some(floats))
}

/// Whether any live chunk or summary lacks both an embedding and terminal
/// tombstone. Alias spellings represent the same vector space, while legacy
/// content-pointer read failures remain retryable.
pub fn has_uncovered_reembed_work(config: &MemoryConfig, model_signature: &str) -> Result<bool> {
    let (_, chunks, states) = list_chunk_snapshot_with_head(
        config,
        &[
            CHUNK_EMBEDDINGS,
            CHUNK_REEMBED_SKIPS,
            SUMMARY_EMBEDDINGS,
            SUMMARY_REEMBED_SKIPS,
            SUMMARIES,
        ],
    )?;
    uncovered_reembed_work(&chunks, &states, model_signature)
}

/// Async counterpart for current-thread queue workers.
pub async fn has_uncovered_reembed_work_async(
    config: &MemoryConfig,
    model_signature: &str,
) -> Result<bool> {
    let (_, chunks, states) = list_chunk_snapshot_with_head_async(
        config,
        &[
            CHUNK_EMBEDDINGS,
            CHUNK_REEMBED_SKIPS,
            SUMMARY_EMBEDDINGS,
            SUMMARY_REEMBED_SKIPS,
            SUMMARIES,
        ],
    )
    .await?;
    uncovered_reembed_work(&chunks, &states, model_signature)
}

fn uncovered_reembed_work(
    chunks: &[super::types::Chunk],
    states: &HashMap<String, Vec<(String, serde_json::Value)>>,
    model_signature: &str,
) -> Result<bool> {
    let variants = signature_variants(model_signature)
        .into_iter()
        .collect::<HashSet<_>>();
    let ids_for_signature = |collection: &str, id_field: &str| {
        states
            .get(collection)
            .into_iter()
            .flatten()
            .filter_map(|(_, value)| {
                let signature = value.get("model_signature")?.as_str()?;
                if !variants.contains(signature) {
                    return None;
                }
                value.get(id_field)?.as_str().map(str::to_owned)
            })
            .collect::<HashSet<_>>()
    };
    let chunk_embeddings = ids_for_signature(CHUNK_EMBEDDINGS, "chunk_id");
    let summary_embeddings = ids_for_signature(SUMMARY_EMBEDDINGS, "summary_id");
    let chunk_skips = states
        .get(CHUNK_REEMBED_SKIPS)
        .into_iter()
        .flatten()
        .filter_map(|(_, value)| {
            let signature = value.get("model_signature")?.as_str()?;
            let chunk_id = value.get("chunk_id")?.as_str()?;
            let reason = value.get("reason")?.as_str()?;
            let retryable = reason.starts_with(&format!(
                "body read failed: {LEGACY_NO_CONTENT_POINTER_REASON_PREFIX}"
            )) || reason.starts_with(&format!(
                "body read failed: {LEGACY_EMPTY_CONTENT_POINTER_REASON_PREFIX}"
            ));
            (variants.contains(signature) && !retryable).then(|| chunk_id.to_owned())
        })
        .collect::<HashSet<_>>();
    let summary_skips = states
        .get(SUMMARY_REEMBED_SKIPS)
        .into_iter()
        .flatten()
        .filter_map(|(_, value)| {
            let signature = value.get("model_signature")?.as_str()?;
            let summary_id = value.get("summary_id")?.as_str()?;
            variants.contains(signature).then(|| summary_id.to_owned())
        })
        .collect::<HashSet<_>>();
    if chunks
        .iter()
        .any(|chunk| !chunk_embeddings.contains(&chunk.id) && !chunk_skips.contains(&chunk.id))
    {
        return Ok(true);
    }
    Ok(states
        .get(SUMMARIES)
        .into_iter()
        .flatten()
        .any(|(_, value)| {
            let live = value.get("deleted").and_then(serde_json::Value::as_bool) != Some(true);
            let Some(id) = value.get("id").and_then(serde_json::Value::as_str) else {
                return false;
            };
            live && !summary_embeddings.contains(id) && !summary_skips.contains(id)
        }))
}

/// Batched read of chunk embeddings under a single `model_signature`.
///
/// Returns a `HashMap<chunk_id, Vec<f32>>` containing only the chunks that have
/// a vector under `model_signature`. Missing chunks are simply absent (callers
/// treat that the same as a `None` from the single-row helper).
///
/// Each id is read against the same host-owned version chain. Missing chunks
/// are absent from the result.
///
/// # Errors
/// Returns `Err` if `chunk_ids` is non-empty and any window's query
/// preparation, execution, or blob decoding (`embedding_from_blob`) fails.
/// Returns `Ok(HashMap::new())` immediately (no DB access) when `chunk_ids`
/// is empty.
pub fn get_chunk_embeddings_for_signature_batch(
    config: &MemoryConfig,
    chunk_ids: &[String],
    model_signature: &str,
) -> Result<HashMap<String, Vec<f32>>> {
    if chunk_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let variants = signature_variants(model_signature);
    let keys = chunk_ids
        .iter()
        .flat_map(|id| {
            variants
                .iter()
                .map(|variant| chunk_embedding_key(id, variant))
        })
        .collect::<Vec<_>>();
    let records = read_states(config, CHUNK_EMBEDDINGS, &keys)?;
    let mut out = HashMap::with_capacity(chunk_ids.len());
    for id in chunk_ids {
        for variant in &variants {
            let key = chunk_embedding_key(id, variant);
            let Some(value) = records.get(&key) else {
                continue;
            };
            let record: ChunkEmbeddingRecord = serde_json::from_value(value.clone())?;
            if record.chunk_id == *id && record.model_signature == *variant {
                if let Some(vector) =
                    embedding_from_blob(&record.vector, record.dim, "chunk embedding")?
                {
                    out.insert(id.clone(), vector);
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// Async counterpart to [`get_chunk_embeddings_for_signature_batch`].
pub async fn get_chunk_embeddings_for_signature_batch_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
    model_signature: &str,
) -> Result<HashMap<String, Vec<f32>>> {
    if chunk_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let variants = signature_variants(model_signature);
    let keys = chunk_ids
        .iter()
        .flat_map(|id| {
            variants
                .iter()
                .map(|variant| chunk_embedding_key(id, variant))
        })
        .collect::<Vec<_>>();
    let records = read_states_async(config, CHUNK_EMBEDDINGS, &keys).await?;
    let mut out = HashMap::with_capacity(chunk_ids.len());
    for id in chunk_ids {
        for variant in &variants {
            let key = chunk_embedding_key(id, variant);
            let Some(value) = records.get(&key) else {
                continue;
            };
            let record: ChunkEmbeddingRecord = serde_json::from_value(value.clone())?;
            if record.chunk_id == *id && record.model_signature == *variant {
                if let Some(vector) =
                    embedding_from_blob(&record.vector, record.dim, "chunk embedding")?
                {
                    out.insert(id.clone(), vector);
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// Batched read of chunk embeddings under the **active** model signature. See
/// [`get_chunk_embeddings_for_signature_batch`] for the batching and error
/// contract.
pub fn get_chunk_embeddings_batch(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_chunk_embeddings_for_signature_batch(config, chunk_ids, &signature)
}

/// Async counterpart to [`get_chunk_embeddings_batch`].
pub async fn get_chunk_embeddings_batch_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, Vec<f32>>> {
    let signature = tree_active_signature(config);
    get_chunk_embeddings_for_signature_batch_async(config, chunk_ids, &signature).await
}
