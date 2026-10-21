//! Chunk persistence and query operations.
//!
//! Chunk record writes and direct record reads use the host-supplied document
//! store. Collection listing and several related transaction paths are being
//! moved with the tree, score, queue, and entity-index group.

use anyhow::Result;
use std::collections::{HashMap, HashSet};

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

/// Upsert staged chunks and their content pointers under one host commit head.
/// The caller is responsible for writing the file and verifying its hash
/// before this call.
pub fn upsert_staged_chunks(config: &MemoryConfig, staged: &[StagedChunk]) -> Result<usize> {
    super::versioned::upsert_staged_chunks(config, staged)
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
    let key = format!("summary:{summary_id}");
    for _ in 0..8 {
        let Some(mut pointer) = super::versioned::read_state(
            config,
            super::collections::SUMMARY_CONTENT_POINTERS,
            &key,
        )?
        else {
            return Ok(());
        };
        let expected = super::versioned::StateChange {
            collection: super::collections::SUMMARY_CONTENT_POINTERS.to_owned(),
            key: key.clone(),
            value: Some(pointer.clone()),
        };
        pointer["content_sha256"] = serde_json::Value::String(new_sha256.to_owned());
        let change = super::versioned::StateChange {
            collection: expected.collection.clone(),
            key: key.clone(),
            value: Some(pointer),
        };
        if super::versioned::commit_if_state_matches(config, &[expected], &[], &[change])? {
            return Ok(());
        }
    }
    anyhow::bail!("summary content pointer changed too often while repairing checksum")
}

/// Async current-thread-safe counterpart to [`update_summary_content_sha256`].
pub async fn update_summary_content_sha256_async(
    config: &MemoryConfig,
    summary_id: &str,
    new_sha256: &str,
) -> Result<()> {
    let key = format!("summary:{summary_id}");
    for _ in 0..8 {
        let Some(mut pointer) = super::versioned::read_state_async(
            config,
            super::collections::SUMMARY_CONTENT_POINTERS,
            &key,
        )
        .await?
        else {
            return Ok(());
        };
        let expected = super::versioned::StateChange {
            collection: super::collections::SUMMARY_CONTENT_POINTERS.to_owned(),
            key: key.clone(),
            value: Some(pointer.clone()),
        };
        pointer["content_sha256"] = serde_json::Value::String(new_sha256.to_owned());
        let change = super::versioned::StateChange {
            collection: expected.collection.clone(),
            key: key.clone(),
            value: Some(pointer),
        };
        if super::versioned::commit_if_state_matches_async(config, &[expected], &[], &[change])
            .await?
        {
            return Ok(());
        }
    }
    anyhow::bail!("summary content pointer changed too often while repairing checksum")
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

/// Fraction of live chunks that have at least one host-owned entity occurrence.
/// An empty store has zero coverage.
///
/// # Errors
/// Returns an error if chunks or the entity index cannot be read.
pub fn extraction_coverage(config: &MemoryConfig) -> Result<f32> {
    let (_, chunks, states) = super::versioned::list_chunk_snapshot_with_head(
        config,
        &[super::collections::ENTITY_INDEX],
    )?;
    extraction_coverage_from_snapshot(&chunks, &states)
}

/// Async counterpart for current-thread Tokio callers.
pub async fn extraction_coverage_async(config: &MemoryConfig) -> Result<f32> {
    let (_, chunks, states) = super::versioned::list_chunk_snapshot_with_head_async(
        config,
        &[super::collections::ENTITY_INDEX],
    )
    .await?;
    extraction_coverage_from_snapshot(&chunks, &states)
}

fn extraction_coverage_from_snapshot(
    chunks: &[Chunk],
    states: &HashMap<String, Vec<(String, serde_json::Value)>>,
) -> Result<f32> {
    if chunks.is_empty() {
        return Ok(0.0);
    }
    let live_ids = chunks
        .iter()
        .map(|chunk| chunk.id.as_str())
        .collect::<HashSet<_>>();
    let mut indexed_ids = HashSet::new();
    for (key, value) in states
        .get(super::collections::ENTITY_INDEX)
        .into_iter()
        .flatten()
    {
        if !key.starts_with("occ:") {
            continue;
        }
        let occurrence: crate::memory::store::entity_index::transaction::EntityOccurrence =
            serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            crate::memory::store::entity_index::transaction::occurrence_key(
                &occurrence.entity_id,
                &occurrence.node_id,
            ) == *key,
            "entity occurrence key does not match its record"
        );
        if live_ids.contains(occurrence.node_id.as_str()) {
            indexed_ids.insert(occurrence.node_id);
        }
    }
    Ok(indexed_ids.len() as f32 / chunks.len() as f32)
}

// ── Source ingest gate + lifecycle status + raw-file gate ────────────────────

pub use super::store_sources::{
    claim_source_ingest, count_chunks_by_lifecycle_status,
    count_raw_paths_ingested_with_prefix, delete_source_ingest, filter_raw_paths_not_ingested,
    get_chunk_lifecycle_status, is_source_ingested, mark_raw_paths_ingested,
    set_chunk_lifecycle_status, RAW_FILE_GATE_KIND,
};
