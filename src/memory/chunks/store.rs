//! Chunk persistence and query operations.
//!
//! Chunk record writes and direct record reads use the host-supplied document
//! store. Collection listing and several related transaction paths are being
//! moved with the tree, score, queue, and entity-index group.

use anyhow::Result;
use rusqlite::{params, Transaction};
use std::collections::HashMap;

use super::connection::with_connection;
use super::types::{Chunk, SourceKind, StagedChunk};
use crate::memory::config::MemoryConfig;

/// Chunk lifecycle: freshly persisted, awaiting the async extract job.
pub const CHUNK_STATUS_PENDING_EXTRACTION: &str = "pending_extraction";
/// Chunk lifecycle: extract ran and the chunk passed admission.
pub const CHUNK_STATUS_ADMITTED: &str = "admitted";
/// Chunk lifecycle: appended to the L0 buffer of its source tree.
pub const CHUNK_STATUS_BUFFERED: &str = "buffered";
/// Chunk lifecycle: rolled into a sealed L1 summary.
pub const CHUNK_STATUS_SEALED: &str = "sealed";
/// Chunk lifecycle: rejected by the admission gate (too low signal).
pub const CHUNK_STATUS_DROPPED: &str = "dropped";

/// Upsert a batch of chunks through the host document store.
///
/// Returns the number of rows inserted or replaced (always `chunks.len()`,
/// even if some of those ids already existed — this counts attempted writes,
/// not "newly created" rows). Duplicates on `chunk.id` are replaced, making
/// the operation idempotent for re-ingest of the same raw source. Existing
/// existing embeddings remain in their independently keyed adjunct records;
/// chunk replacement does not implicitly delete them.
///
/// Returns `Ok(0)` immediately (no DB access) for an empty `chunks` slice.
///
/// The batch is staged as immutable versions, then made visible by one
/// compare-and-swap of its scope's commit head. Existing snapshots retain the
/// old version. Immutable history currently grows without compaction; safe
/// garbage collection requires a future reader-lifetime protocol.
///
/// # Errors
/// Returns `Err` if staging fails or the host storage operation fails. A
/// bounded CAS retry re-evaluates unconditional writes after another writer
/// advances the commit head. Staged records remain invisible until the head
/// compare-and-swap succeeds.
pub fn upsert_chunks(config: &MemoryConfig, chunks: &[Chunk]) -> Result<usize> {
    super::versioned::upsert_chunks(config, chunks)
}

/// Async counterpart to [`upsert_chunks`] for Tokio callers.
pub async fn upsert_chunks_async(config: &MemoryConfig, chunks: &[Chunk]) -> Result<usize> {
    super::versioned::upsert_chunks_async(config, chunks).await
}

pub fn upsert_chunks_tx(tx: &Transaction<'_>, chunks: &[Chunk]) -> Result<usize> {
    let _ = (tx, chunks);
    anyhow::bail!("chunks must be committed with host storage, not a SQLite transaction")
}

/// Upsert staged chunks and their content pointers under one host commit head.
/// The caller is responsible for writing the file and verifying its hash
/// before this call.
pub fn upsert_staged_chunks(config: &MemoryConfig, staged: &[StagedChunk]) -> Result<usize> {
    super::versioned::upsert_staged_chunks(config, staged)
}

/// Transitional transaction seam. Staged chunks must use host storage.
pub fn upsert_staged_chunks_tx(tx: &Transaction<'_>, staged: &[StagedChunk]) -> Result<usize> {
    let _ = (tx, staged);
    anyhow::bail!("staged chunks must be committed with host storage, not a SQLite transaction")
}

pub fn update_chunk_content_sha256(
    config: &MemoryConfig,
    chunk_id: &str,
    new_sha256: &str,
) -> Result<()> {
    super::versioned::update_chunk_content_sha256(config, chunk_id, new_sha256)
}

pub fn update_summary_content_sha256(
    config: &MemoryConfig,
    summary_id: &str,
    new_sha256: &str,
) -> Result<()> {
    with_connection(config, |conn| {
        conn.execute(
            "UPDATE mem_tree_summaries SET content_sha256 = ?1 WHERE id = ?2",
            params![new_sha256, summary_id],
        )?;
        Ok(())
    })
}

pub fn list_source_ids_with_prefix(
    config: &MemoryConfig,
    source_kind: SourceKind,
    prefix: &str,
) -> Result<Vec<String>> {
    let mut ids = super::versioned::list_chunks(config)?
        .into_iter()
        .filter(|chunk| {
            chunk.metadata.source_kind == source_kind
                && chunk.metadata.source_id.starts_with(prefix)
        })
        .map(|chunk| chunk.metadata.source_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ids.sort();
    Ok(ids)
}

/// Fetch one chunk by its id from one committed head snapshot.
///
/// # Errors
/// Returns `Err` if the host read fails or a stored chunk version is malformed.
/// Returns `Ok(None)`, not an error, when `id` does not exist.
pub fn get_chunk(config: &MemoryConfig, id: &str) -> Result<Option<Chunk>> {
    super::versioned::get_chunk(config, id)
}

/// Async single-record read for current-thread Tokio callers.
pub async fn get_chunk_async(config: &MemoryConfig, id: &str) -> Result<Option<Chunk>> {
    super::versioned::get_chunk_async(config, id).await
}

/// Batched read of full chunk rows by id from one commit-head snapshot. The
/// returned map contains only ids that exist; missing ids are silently absent.
///
/// # Errors
/// Returns `Err` if reading the snapshot or any matching version fails.
pub fn get_chunks_batch(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, Chunk>> {
    super::versioned::get_chunks_batch(config, chunk_ids)
}

/// Async batch read for async callers, including current-thread Tokio hosts.
pub async fn get_chunks_batch_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, Chunk>> {
    super::versioned::get_chunks_batch_async(config, chunk_ids).await
}

/// Count total chunks in the host store, regardless of lifecycle status.
///
/// # Errors
/// Returns `Err` if the host query fails or a chunk is malformed.
pub fn count_chunks(config: &MemoryConfig) -> Result<u64> {
    Ok(super::versioned::list_chunks(config)?.len() as u64)
}

/// Extraction coverage requires entity-index records committed alongside chunks.
///
/// # Errors
/// Returns an error until the host entity-index port is ready.
pub fn extraction_coverage(config: &MemoryConfig) -> Result<f32> {
    let _ = config;
    anyhow::bail!("extraction coverage requires the host entity-index port")
}

// ── Source ingest gate + lifecycle status + raw-file gate ────────────────────

pub use super::store_sources::{
    claim_source_ingest, claim_source_ingest_tx, count_chunks_by_lifecycle_status,
    count_raw_paths_ingested_with_prefix, delete_source_ingest, filter_raw_paths_not_ingested,
    get_chunk_lifecycle_status, is_source_ingested, mark_raw_paths_ingested,
    set_chunk_lifecycle_status, RAW_FILE_GATE_KIND,
};
