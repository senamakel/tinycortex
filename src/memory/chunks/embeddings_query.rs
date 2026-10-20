//! Signature-aware host-document embedding **read** queries for chunk vectors.
//!
//! Split out of `embeddings` so that module stays a focused writer-side
//! accessor and re-embed tombstone store, and this one owns the per-signature
//! read paths. Every read matches *any* spelling of a vector space (see
//! [`super::signature::signature_variants`]) so a store that changed
//! conventions can still see its own prior vectors.
//!
//! No embeddings are computed here — embeddings.rs takes vectors on the write
//! side; this module only reads them back. The tree-owned re-embed coverage
//! probe still accepts a SQLite connection while that coupled migration lands.

use super::collections::CHUNK_EMBEDDINGS;
use super::embeddings::{chunk_embedding_key, tree_active_signature, ChunkEmbeddingRecord};
use super::signature::{signature_in_clause, signature_variants};
use super::versioned::{read_state, read_states, read_states_async};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;

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
/// tombstone.
///
/// Coverage counts a vector written under *any* spelling of the signature: a
/// row that only looks uncovered because the convention changed is not work,
/// and re-embedding it would spend provider quota to reproduce a vector that
/// is already on disk.
pub fn has_uncovered_reembed_work(
    conn: &Connection,
    model_signature: &str,
) -> rusqlite::Result<bool> {
    let variants = signature_variants(model_signature);
    let sig_clause = signature_in_clause(variants.len(), 1);
    // Retryable skip reasons for legacy content-pointer rows; matched after the
    // signature-variant params (which occupy `?1..=?variants.len()`).
    let retry_idx_1 = variants.len() + 1;
    let retry_idx_2 = variants.len() + 2;
    let missing_pointer_retry_like =
        format!("body read failed: {LEGACY_NO_CONTENT_POINTER_REASON_PREFIX}%");
    // Legacy-only: older readers emitted this for present-but-empty content
    // pointers. Current readers filter those pointers before returning `Some`,
    // but existing skip rows must still become retriable after the fallback fix.
    let empty_pointer_retry_like =
        format!("body read failed: {LEGACY_EMPTY_CONTENT_POINTER_REASON_PREFIX}%");
    let mut params: Vec<&dyn rusqlite::ToSql> = variants
        .iter()
        .map(|variant| variant as &dyn rusqlite::ToSql)
        .collect();
    params.push(&missing_pointer_retry_like as &dyn rusqlite::ToSql);
    params.push(&empty_pointer_retry_like as &dyn rusqlite::ToSql);
    conn.query_row(
        &format!(
            "SELECT EXISTS(
                 SELECT 1 FROM mem_tree_chunks c
                  WHERE NOT EXISTS (SELECT 1 FROM mem_tree_chunk_embeddings e
                                     WHERE e.chunk_id = c.id AND e.model_signature {sig_clause})
                    AND NOT EXISTS (SELECT 1 FROM mem_tree_chunk_reembed_skipped sk
                                     WHERE sk.chunk_id = c.id AND sk.model_signature {sig_clause}
                                       AND sk.reason NOT LIKE ?{retry_idx_1}
                                       AND sk.reason NOT LIKE ?{retry_idx_2}))
               OR EXISTS(
                 SELECT 1 FROM mem_tree_summaries s
                  WHERE s.deleted = 0
                    AND NOT EXISTS (SELECT 1 FROM mem_tree_summary_embeddings e
                                     WHERE e.summary_id = s.id AND e.model_signature {sig_clause})
                    AND NOT EXISTS (SELECT 1 FROM mem_tree_summary_reembed_skipped sk
                                     WHERE sk.summary_id = s.id AND sk.model_signature {sig_clause}))"
        ),
        params.as_slice(),
        |row| row.get(0),
    )
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
