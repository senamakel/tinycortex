//! Append-only chunk versions committed through one host-scoped CAS head.
//!
//! A committed head names an immutable operation manifest. Each manifest
//! points to its predecessor, so readers capture one head and resolve only
//! versions in that committed prefix. Staged versions never replace a live
//! chunk and are invisible until the head compare-and-swap succeeds.
//!
//! Versions and manifests are deliberately retained. Safe compaction requires
//! read leases or an equivalent snapshot-lifetime protocol and is not yet
//! implemented.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinystoragedrivers_types::{Filter, Precondition, Query, Sort, Version, WriteOptions};

use super::collections::{
    CHUNKS, CHUNK_CONTENT_POINTERS, CHUNK_LIFECYCLE, CHUNK_VERSIONS, COMMIT_HEADS, OPERATIONS,
    STATE_VERSIONS,
};
use super::types::{Chunk, StagedChunk};
use crate::memory::config::MemoryConfig;

const RECORD_SCHEMA: u32 = 1;
const PAGE_SIZE: usize = 256;
const CAS_RETRIES: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeadSnapshot {
    pub(crate) op_id: Option<String>,
    pub(crate) version: Option<Version>,
    pub(crate) revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CommitHead {
    op_id: String,
    revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct OperationManifest {
    op_id: String,
    parent_op: Option<String>,
    revision: u64,
    #[serde(default)]
    ancestors: Vec<String>,
    writes: Vec<ManifestWrite>,
    #[serde(default)]
    state_writes: Vec<StateManifestWrite>,
    /// Lifecycle default for chunk versions in this operation. It is batch
    /// metadata rather than one sidecar row per chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_lifecycle_status: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ManifestWrite {
    logical_id: String,
    version_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StateManifestWrite {
    collection: String,
    key: String,
    version_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct StateChange {
    pub(crate) collection: String,
    pub(crate) key: String,
    pub(crate) value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StateKey {
    pub(crate) collection: String,
    pub(crate) key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StateVersion {
    collection: String,
    key: String,
    operation_id: String,
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ChunkVersion {
    logical_id: String,
    operation_id: String,
    revision: u64,
    chunk: Chunk,
}

struct VisibleOperation {
    revision: u64,
    writes: HashSet<(String, String)>,
    state_writes: HashSet<(String, String, String)>,
    default_lifecycle_status: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkContentPointer {
    chunk_id: String,
    content_path: String,
    content_sha256: String,
}

/// Capture the owner-scoped commit-chain head used by a logical read.
pub(crate) fn capture_head(config: &MemoryConfig) -> Result<HeadSnapshot> {
    let documents = config.storage()?.documents().clone();
    let snapshot = config
        .storage_blocking()?
        .run(async move {
            documents
                .get(COMMIT_HEADS, "head")
                .await?
                .map(|record| {
                    if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "commit head has unexpected schema or expiry metadata",
                        ));
                    }
                    let head: CommitHead = serde_json::from_value(record.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    Ok::<HeadSnapshot, tinystoragedrivers_types::StorageError>(HeadSnapshot {
                        op_id: Some(head.op_id),
                        version: Some(record.version),
                        revision: head.revision,
                    })
                })
                .transpose()
        })
        .map_err(anyhow::Error::from)??;
    Ok(snapshot.unwrap_or(HeadSnapshot {
        op_id: None,
        version: None,
        revision: 0,
    }))
}

async fn capture_head_async(config: &MemoryConfig) -> Result<HeadSnapshot> {
    let documents = config.storage()?.documents().clone();
    let snapshot = config
        .storage_blocking()?
        .run_async(async move {
            documents
                .get(COMMIT_HEADS, "head")
                .await?
                .map(|record| {
                    if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "commit head has unexpected schema or expiry metadata",
                        ));
                    }
                    let head: CommitHead = serde_json::from_value(record.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    Ok::<HeadSnapshot, tinystoragedrivers_types::StorageError>(HeadSnapshot {
                        op_id: Some(head.op_id),
                        version: Some(record.version),
                        revision: head.revision,
                    })
                })
                .transpose()
        })
        .await?
        .map_err(anyhow::Error::from)?;
    Ok(snapshot.unwrap_or(HeadSnapshot {
        op_id: None,
        version: None,
        revision: 0,
    }))
}

/// Capture one immutable owner-head token for a multi-collection read/commit
/// sequence that must reject phantoms created after enumeration.
pub(crate) async fn capture_state_snapshot_async(config: &MemoryConfig) -> Result<HeadSnapshot> {
    capture_head_async(config).await
}

/// Write a chunk batch as immutable versions and commit them in one head CAS.
///
/// # Errors
/// Returns an error if a staged value conflicts, the captured head changed,
/// or the host document store fails.
pub(crate) fn upsert_chunks(config: &MemoryConfig, chunks: &[Chunk]) -> Result<usize> {
    commit_operation(config, chunks, &[])
}

/// Async counterpart to [`upsert_chunks`] for Tokio callers.
pub(crate) async fn upsert_chunks_async(config: &MemoryConfig, chunks: &[Chunk]) -> Result<usize> {
    if chunks.is_empty() {
        return Ok(0);
    }
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head_async(config).await?;
        let changes =
            prepare_chunk_upsert_state_changes_async(config, &snapshot, chunks, &[]).await?;
        match upsert_chunks_at_async(
            config,
            &snapshot,
            chunks,
            chunks,
            &changes,
            Some(super::store::CHUNK_STATUS_ADMITTED),
        )
        .await
        {
            Ok(_) => return Ok(chunks.len()),
            Err(error) if is_storage_conflict(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    anyhow::bail!("chunk version commit did not succeed within the retry budget")
}

/// Commit staged chunk previews and their content pointers at one head.
pub(crate) fn upsert_staged_chunks(config: &MemoryConfig, staged: &[StagedChunk]) -> Result<usize> {
    if staged.is_empty() {
        return Ok(0);
    }
    let mut chunks = Vec::with_capacity(staged.len());
    let mut changes = Vec::with_capacity(staged.len());
    for item in staged {
        let mut chunk = item.chunk.clone();
        chunk.content = chunk.content.chars().take(500).collect();
        changes.push(StateChange {
            collection: CHUNK_CONTENT_POINTERS.into(),
            key: item.chunk.id.clone(),
            value: Some(serde_json::to_value(ChunkContentPointer {
                chunk_id: item.chunk.id.clone(),
                content_path: item.content_path.clone(),
                content_sha256: item.content_sha256.clone(),
            })?),
        });
        chunks.push(chunk);
    }
    commit_operation(config, &chunks, &changes)
}

/// Commit staged chunk content plus caller-provided domain records only when
/// all declared state preconditions still match the captured commit head.
pub(crate) fn commit_staged_chunks_if_state_matches(
    config: &MemoryConfig,
    staged: &[StagedChunk],
    expected: &[StateChange],
    changes: &[StateChange],
) -> Result<bool> {
    let mut chunks = Vec::with_capacity(staged.len());
    let mut all_changes = Vec::with_capacity(staged.len() + changes.len());
    for item in staged {
        let mut chunk = item.chunk.clone();
        chunk.content = chunk.content.chars().take(500).collect();
        all_changes.push(StateChange {
            collection: CHUNK_CONTENT_POINTERS.into(),
            key: item.chunk.id.clone(),
            value: Some(serde_json::to_value(ChunkContentPointer {
                chunk_id: item.chunk.id.clone(),
                content_path: item.content_path.clone(),
                content_sha256: item.content_sha256.clone(),
            })?),
        });
        chunks.push(chunk);
    }
    all_changes.extend_from_slice(changes);
    commit_if_state_matches(config, expected, &chunks, &all_changes)
}

/// Async counterpart used by async host workflows that must also work on a
/// current-thread Tokio runtime.
pub(crate) async fn commit_staged_chunks_if_state_matches_async(
    config: &MemoryConfig,
    staged: &[StagedChunk],
    expected: &[StateChange],
    changes: &[StateChange],
) -> Result<bool> {
    let mut chunks = Vec::with_capacity(staged.len());
    let mut all_changes = Vec::with_capacity(staged.len() + changes.len());
    for item in staged {
        let mut chunk = item.chunk.clone();
        chunk.content = chunk.content.chars().take(500).collect();
        all_changes.push(StateChange {
            collection: CHUNK_CONTENT_POINTERS.into(),
            key: item.chunk.id.clone(),
            value: Some(serde_json::to_value(ChunkContentPointer {
                chunk_id: item.chunk.id.clone(),
                content_path: item.content_path.clone(),
                content_sha256: item.content_sha256.clone(),
            })?),
        });
        chunks.push(chunk);
    }
    all_changes.extend_from_slice(changes);
    commit_if_state_matches_async(config, expected, &chunks, &all_changes).await
}

/// Update the hash metadata for an existing staged chunk pointer.
pub(crate) fn update_chunk_content_sha256(
    config: &MemoryConfig,
    chunk_id: &str,
    new_sha256: &str,
) -> Result<()> {
    let Some(current) = read_state(config, CHUNK_CONTENT_POINTERS, chunk_id)? else {
        return Ok(());
    };
    let mut pointer: ChunkContentPointer = serde_json::from_value(current.clone())?;
    pointer.content_sha256 = new_sha256.to_owned();
    let expected = StateChange {
        collection: CHUNK_CONTENT_POINTERS.into(),
        key: chunk_id.into(),
        value: Some(current),
    };
    let update = StateChange {
        collection: CHUNK_CONTENT_POINTERS.into(),
        key: chunk_id.into(),
        value: Some(serde_json::to_value(pointer)?),
    };
    if !commit_if_state_matches(config, &[expected], &[], &[update])? {
        anyhow::bail!("chunk content pointer changed while updating its hash");
    }
    Ok(())
}

pub(crate) async fn update_chunk_content_sha256_async(
    config: &MemoryConfig,
    chunk_id: &str,
    new_sha256: &str,
) -> Result<()> {
    let Some(current) = read_state_async(config, CHUNK_CONTENT_POINTERS, chunk_id).await? else {
        return Ok(());
    };
    let mut pointer: ChunkContentPointer = serde_json::from_value(current.clone())?;
    pointer.content_sha256 = new_sha256.to_owned();
    let expected = StateChange {
        collection: CHUNK_CONTENT_POINTERS.into(),
        key: chunk_id.into(),
        value: Some(current),
    };
    let update = StateChange {
        collection: CHUNK_CONTENT_POINTERS.into(),
        key: chunk_id.into(),
        value: Some(serde_json::to_value(pointer)?),
    };
    if !commit_if_state_matches_async(config, &[expected], &[], &[update]).await? {
        anyhow::bail!("chunk content pointer changed while updating its hash");
    }
    Ok(())
}

fn commit_operation(
    config: &MemoryConfig,
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<usize> {
    if chunks.is_empty() && changes.is_empty() {
        return Ok(0);
    }

    // Preserve SQLite's last-input-wins behavior for duplicate ids while
    // retaining the public attempted-write count.
    let mut final_chunks = Vec::new();
    let mut positions = HashMap::new();
    for chunk in chunks {
        if let Some(position) = positions.get(&chunk.id).copied() {
            final_chunks[position] = chunk.clone();
        } else {
            positions.insert(chunk.id.clone(), final_chunks.len());
            final_chunks.push(chunk.clone());
        }
    }
    let mut final_changes = Vec::new();
    let mut change_positions = HashMap::new();
    for change in changes {
        let key = format!("{}\0{}", change.collection, change.key);
        if let Some(position) = change_positions.get(&key).copied() {
            final_changes[position] = change.clone();
        } else {
            change_positions.insert(key, final_changes.len());
            final_changes.push(change.clone());
        }
    }

    let mut last_conflict = None;
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head(config)?;
        let effective_changes =
            prepare_chunk_upsert_state_changes(config, &snapshot, &final_chunks, &final_changes)?;
        let default_lifecycle_status = Some(super::store::CHUNK_STATUS_ADMITTED);
        if is_current_commit_retry(
            config,
            &snapshot,
            &final_chunks,
            &effective_changes,
            default_lifecycle_status,
        )? {
            return Ok(chunks.len());
        }
        match upsert_chunks_at(
            config,
            &snapshot,
            chunks,
            &final_chunks,
            &effective_changes,
            default_lifecycle_status,
        ) {
            Ok(count) => return Ok(count),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("chunk version commit did not succeed within the retry budget")
    }))
}

fn prepare_chunk_upsert_state_changes(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<Vec<StateChange>> {
    if chunks.is_empty() {
        return Ok(changes.to_vec());
    }
    let documents = config.storage()?.documents().clone();
    let snapshot = snapshot.clone();
    let chunks = chunks.to_vec();
    let changes = changes.to_vec();
    let result = config.storage_blocking()?.run(async move {
        prepare_chunk_upsert_state_changes_at(&*documents, &snapshot, &chunks, &changes).await
    })?;
    result.map_err(anyhow::Error::from)
}

async fn prepare_chunk_upsert_state_changes_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<Vec<StateChange>> {
    if chunks.is_empty() {
        return Ok(changes.to_vec());
    }
    let documents = config.storage()?.documents().clone();
    let snapshot = snapshot.clone();
    let chunks = chunks.to_vec();
    let changes = changes.to_vec();
    let result = config
        .storage_blocking()?
        .run_async(async move {
            prepare_chunk_upsert_state_changes_at(&*documents, &snapshot, &chunks, &changes).await
        })
        .await?;
    result.map_err(anyhow::Error::from)
}

async fn prepare_chunk_upsert_state_changes_at(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    chunks: &[Chunk],
    changes: &[StateChange],
) -> tinystoragedrivers_types::Result<Vec<StateChange>> {
    let manifests = load_operation_manifests(documents).await?;
    let visible = visible_operations_at_snapshot(snapshot, &manifests)?;
    let collections = [
        CHUNK_LIFECYCLE.to_owned(),
        CHUNK_CONTENT_POINTERS.to_owned(),
    ];
    let states = bulk_state_values_at_snapshot(documents, snapshot, &visible, &collections).await?;
    let lifecycle = states
        .get(CHUNK_LIFECYCLE)
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.as_str(), value))
        .collect::<HashMap<_, _>>();
    let pointers = states
        .get(CHUNK_CONTENT_POINTERS)
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.as_str(), value))
        .collect::<HashMap<_, _>>();
    let explicit = changes
        .iter()
        .map(|change| (change.collection.as_str(), change.key.as_str()))
        .collect::<HashSet<_>>();
    let mut prepared = changes.to_vec();
    for chunk in chunks {
        // New chunks have an implicit, never-written lifecycle value. A
        // replacement only needs an explicit admitted row when it is clearing
        // a prior lifecycle state such as dropped or sealed.
        if !explicit.contains(&(CHUNK_LIFECYCLE, chunk.id.as_str()))
            && lifecycle.contains_key(chunk.id.as_str())
        {
            prepared.push(StateChange {
                collection: CHUNK_LIFECYCLE.to_owned(),
                key: chunk.id.clone(),
                value: None,
            });
        }
        // Avoid persisting tombstones for pointers that never existed while
        // still clearing staged-file metadata on a plain replacement.
        if !explicit.contains(&(CHUNK_CONTENT_POINTERS, chunk.id.as_str()))
            && pointers.contains_key(chunk.id.as_str())
        {
            prepared.push(StateChange {
                collection: CHUNK_CONTENT_POINTERS.to_owned(),
                key: chunk.id.clone(),
                value: None,
            });
        }
    }
    Ok(prepared)
}

pub(crate) fn write_state_changes(config: &MemoryConfig, changes: &[StateChange]) -> Result<()> {
    commit_operation(config, &[], changes)?;
    Ok(())
}

/// Commit child records only while their parent chunk remains visible at the
/// same version-chain head. A head conflict retries the existence check.
pub(crate) fn write_chunk_state_changes(
    config: &MemoryConfig,
    chunk_id: &str,
    changes: &[StateChange],
) -> Result<bool> {
    let mut last_conflict = None;
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head(config)?;
        let documents = config.storage()?.documents().clone();
        let snapshot_for_read = snapshot.clone();
        let chunk_id_owned = chunk_id.to_owned();
        let exists = config.storage_blocking()?.run(async move {
            Ok::<_, tinystoragedrivers_types::StorageError>(
                read_chunk_at(&*documents, &snapshot_for_read, &chunk_id_owned)
                    .await?
                    .is_some(),
            )
        })??;
        if !exists {
            return Ok(false);
        }
        match upsert_chunks_at(config, &snapshot, &[], &[], changes, None) {
            Ok(_) => return Ok(true),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("chunk child-state commit did not succeed within the retry budget")
    }))
}

fn is_storage_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tinystoragedrivers_types::StorageError>()
            .is_some_and(|storage_error| {
                storage_error.kind() == tinystoragedrivers_types::ErrorKind::Conflict
            })
    })
}

fn upsert_chunks_at(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    chunks: &[Chunk],
    final_chunks: &[Chunk],
    changes: &[StateChange],
    default_lifecycle_status: Option<&str>,
) -> Result<usize> {
    let (revision, ancestors) = next_revision(config, snapshot)?;
    let operation = make_manifest(
        snapshot,
        revision,
        ancestors,
        final_chunks,
        changes,
        default_lifecycle_status,
    )?;
    let expected_parent = snapshot.clone();
    let final_chunks = final_chunks.to_vec();
    let changes = changes.to_vec();
    let documents = config.storage()?.documents().clone();
    let result = config.storage_blocking()?.run(async move {
        let op_value = serde_json::to_value(&operation)
            .map_err(tinystoragedrivers_types::StorageError::from)?;
        put_immutable(&*documents, OPERATIONS, &operation.op_id, op_value).await?;

        for chunk in final_chunks.iter().cloned() {
            let version_id = version_id(&chunk.id, &operation.op_id);
            let value = serde_json::to_value(ChunkVersion {
                logical_id: chunk.id.clone(),
                operation_id: operation.op_id.clone(),
                revision,
                chunk,
            })
            .map_err(tinystoragedrivers_types::StorageError::from)?;
            put_immutable(&*documents, CHUNK_VERSIONS, &version_id, value).await?;
        }
        for change in changes {
            let version_id = state_version_id(&change.collection, &change.key, &operation.op_id);
            let value = serde_json::to_value(StateVersion {
                collection: change.collection,
                key: change.key,
                operation_id: operation.op_id.clone(),
                revision,
                value: change.value,
            })
            .map_err(tinystoragedrivers_types::StorageError::from)?;
            put_immutable(&*documents, STATE_VERSIONS, &version_id, value).await?;
        }

        let head_value = serde_json::to_value(CommitHead {
            op_id: operation.op_id.clone(),
            revision,
        })
        .map_err(tinystoragedrivers_types::StorageError::from)?;
        let precondition = expected_parent
            .version
            .map(Precondition::Version)
            .unwrap_or(Precondition::Absent);
        documents
            .put_with(
                COMMIT_HEADS,
                "head",
                head_value,
                precondition,
                WriteOptions::new().schema(RECORD_SCHEMA),
            )
            .await?;
        Ok::<_, tinystoragedrivers_types::StorageError>(())
    })?;
    result.context("commit chunk version batch")?;
    Ok(chunks.len())
}

async fn upsert_chunks_at_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    chunks: &[Chunk],
    final_chunks: &[Chunk],
    changes: &[StateChange],
    default_lifecycle_status: Option<&str>,
) -> Result<usize> {
    let (revision, ancestors) = next_revision_async(config, snapshot).await?;
    let operation = make_manifest(
        snapshot,
        revision,
        ancestors,
        final_chunks,
        changes,
        default_lifecycle_status,
    )?;
    let expected_parent = snapshot.clone();
    let final_chunks = final_chunks.to_vec();
    let changes = changes.to_vec();
    let documents = config.storage()?.documents().clone();
    let result = config
        .storage_blocking()?
        .run_async(async move {
            let op_value = serde_json::to_value(&operation)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            put_immutable(&*documents, OPERATIONS, &operation.op_id, op_value).await?;

            for chunk in final_chunks.iter().cloned() {
                let id = version_id(&chunk.id, &operation.op_id);
                let body = serde_json::to_value(ChunkVersion {
                    logical_id: chunk.id.clone(),
                    operation_id: operation.op_id.clone(),
                    revision,
                    chunk,
                })
                .map_err(tinystoragedrivers_types::StorageError::from)?;
                put_immutable(&*documents, CHUNK_VERSIONS, &id, body).await?;
            }
            for change in changes {
                let id = state_version_id(&change.collection, &change.key, &operation.op_id);
                let body = serde_json::to_value(StateVersion {
                    collection: change.collection,
                    key: change.key,
                    operation_id: operation.op_id.clone(),
                    revision,
                    value: change.value,
                })
                .map_err(tinystoragedrivers_types::StorageError::from)?;
                put_immutable(&*documents, STATE_VERSIONS, &id, body).await?;
            }

            let head_value = serde_json::to_value(CommitHead {
                op_id: operation.op_id,
                revision,
            })
            .map_err(tinystoragedrivers_types::StorageError::from)?;
            let precondition = expected_parent
                .version
                .map(Precondition::Version)
                .unwrap_or(Precondition::Absent);
            documents
                .put_with(
                    COMMIT_HEADS,
                    "head",
                    head_value,
                    precondition,
                    WriteOptions::new().schema(RECORD_SCHEMA),
                )
                .await?;
            Ok::<_, tinystoragedrivers_types::StorageError>(())
        })
        .await?;
    result.context("commit chunk version batch")?;
    Ok(chunks.len())
}

fn is_current_commit_retry(
    config: &MemoryConfig,
    head: &HeadSnapshot,
    chunks: &[Chunk],
    changes: &[StateChange],
    default_lifecycle_status: Option<&str>,
) -> Result<bool> {
    let Some(snapshot) = head.op_id.clone() else {
        return Ok(false);
    };
    let documents = config.storage()?.documents().clone();
    let expected = chunks.to_vec();
    let expected_changes = changes.to_vec();
    let expected_default = default_lifecycle_status.map(str::to_owned);
    config
        .storage_blocking()?
        .run(async move {
            let Some(record) = documents.get(OPERATIONS, &snapshot).await? else {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "commit head references a missing operation manifest",
                ));
            };
            if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest has unexpected schema or expiry metadata",
                ));
            }
            let operation: OperationManifest = serde_json::from_value(record.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if operation.default_lifecycle_status != expected_default
                || operation.writes.len() != expected.len()
                || operation
                    .writes
                    .iter()
                    .zip(&expected)
                    .any(|(write, chunk)| write.logical_id != chunk.id)
                || operation.state_writes.len() != expected_changes.len()
                || operation
                    .state_writes
                    .iter()
                    .zip(&expected_changes)
                    .any(|(write, change)| {
                        write.collection != change.collection || write.key != change.key
                    })
            {
                return Ok(false);
            }
            for chunk in expected {
                let id = version_id(&chunk.id, &snapshot);
                let Some(record) = documents.get(CHUNK_VERSIONS, &id).await? else {
                    return Ok(false);
                };
                if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                    return Ok(false);
                }
                let version: ChunkVersion = serde_json::from_value(record.doc)
                    .map_err(tinystoragedrivers_types::StorageError::from)?;
                if version.chunk != chunk {
                    return Ok(false);
                }
            }
            for change in expected_changes {
                let id = state_version_id(&change.collection, &change.key, &snapshot);
                let Some(record) = documents.get(STATE_VERSIONS, &id).await? else {
                    return Ok(false);
                };
                if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                    return Ok(false);
                }
                let version: StateVersion = serde_json::from_value(record.doc)
                    .map_err(tinystoragedrivers_types::StorageError::from)?;
                if version.value != change.value {
                    return Ok(false);
                }
            }
            Ok(true)
        })?
        .map_err(anyhow::Error::from)
}

/// Read one chunk through a commit-head snapshot.
pub(crate) fn get_chunk(config: &MemoryConfig, id: &str) -> Result<Option<Chunk>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let id = id.to_owned();
    config
        .storage_blocking()?
        .run(async move { read_chunk_at(&*documents, &snapshot, &id).await })?
        .context("read chunk at commit snapshot")
}

pub(crate) async fn get_chunk_async(config: &MemoryConfig, id: &str) -> Result<Option<Chunk>> {
    let documents = config.storage()?.documents().clone();
    let blocking = config.storage_blocking()?.clone();
    let snapshot_documents = documents.clone();
    let snapshot = blocking
        .run_async(async move {
            let Some(record) = snapshot_documents.get(COMMIT_HEADS, "head").await? else {
                return Ok(HeadSnapshot {
                    op_id: None,
                    version: None,
                    revision: 0,
                });
            };
            if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "commit head has unexpected schema or expiry metadata",
                ));
            }
            let head: CommitHead = serde_json::from_value(record.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            Ok(HeadSnapshot {
                op_id: Some(head.op_id),
                version: Some(record.version),
                revision: head.revision,
            })
        })
        .await?
        .map_err(anyhow::Error::from)?;
    let id = id.to_owned();
    blocking
        .run_async(async move { read_chunk_at(&*documents, &snapshot, &id).await })
        .await?
        .map_err(anyhow::Error::from)
}

/// Read a group of chunks against one captured commit prefix.
pub(crate) fn get_chunks_batch(
    config: &MemoryConfig,
    ids: &[String],
) -> Result<HashMap<String, Chunk>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let ids = ids.to_vec();
    config
        .storage_blocking()?
        .run(async move {
            let mut chunks = HashMap::with_capacity(ids.len());
            for id in ids {
                if let Some(chunk) = read_chunk_at(&*documents, &snapshot, &id).await? {
                    chunks.insert(id, chunk);
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(chunks)
        })?
        .map_err(anyhow::Error::from)
}

/// Materialize the live chunk values visible at one captured commit head.
pub(crate) fn list_chunks(config: &MemoryConfig) -> Result<Vec<Chunk>> {
    Ok(list_chunk_snapshot(config, &[])?.0)
}

/// List chunks and owner-state collections at one captured commit head.
pub(crate) fn list_chunk_snapshot(
    config: &MemoryConfig,
    state_collections: &[&str],
) -> Result<(Vec<Chunk>, HashMap<String, Vec<(String, Value)>>)> {
    let (_, chunks, states) = list_chunk_snapshot_with_head(config, state_collections)?;
    Ok((chunks, states))
}

/// List chunks and state rows while returning the exact owner head captured
/// before enumeration. Deletion uses this token to reject phantom inserts.
pub(crate) fn list_chunk_snapshot_with_head(
    config: &MemoryConfig,
    state_collections: &[&str],
) -> Result<(
    HeadSnapshot,
    Vec<Chunk>,
    HashMap<String, Vec<(String, Value)>>,
)> {
    let snapshot = capture_head(config)?;
    let return_snapshot = snapshot.clone();
    let documents = config.storage()?.documents().clone();
    let mut state_collections = state_collections
        .iter()
        .map(|collection| (*collection).to_owned())
        .collect::<Vec<_>>();
    if !state_collections.iter().any(|name| name == CHUNK_LIFECYCLE) {
        state_collections.push(CHUNK_LIFECYCLE.to_owned());
    }
    config
        .storage_blocking()?
        .run(async move {
            // Resolve immutable versions against one in-memory copy of the
            // captured head's ancestry. Querying CHUNK_VERSIONS once per
            // logical id makes a full listing quadratic on stores whose
            // filtered query scans the collection.
            let manifests = load_operation_manifests(&*documents).await?;
            let visible = visible_operations_at_snapshot(&snapshot, &manifests)?;
            let mut latest = HashMap::<String, (u64, Chunk, Option<String>)>::new();
            let mut query = Query::all().limit(PAGE_SIZE);
            loop {
                let page = documents.query(CHUNK_VERSIONS, &query).await?;
                for record in page.items {
                    if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "chunk version has unexpected schema or expiry metadata",
                        ));
                    }
                    let version: ChunkVersion = serde_json::from_value(record.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    if version.chunk.id != version.logical_id
                        || version_id(&version.logical_id, &version.operation_id) != record.id
                    {
                        // Match the point-read behavior: immutable rows with
                        // inconsistent keys are not candidates for a logical
                        // chunk value.
                        continue;
                    }
                    let Some(operation) = visible.get(&version.operation_id) else {
                        continue;
                    };
                    if operation.revision != version.revision
                        || !operation
                            .writes
                            .contains(&(version.logical_id.clone(), record.id.clone()))
                    {
                        continue;
                    }
                    let replace = latest
                        .get(&version.logical_id)
                        .is_none_or(|(revision, _, _)| version.revision > *revision);
                    if replace {
                        latest.insert(
                            version.logical_id,
                            (
                                version.revision,
                                version.chunk,
                                operation.default_lifecycle_status.clone(),
                            ),
                        );
                    }
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }

            let mut legacy = HashMap::new();
            let mut query = Query::all().limit(PAGE_SIZE);
            loop {
                let page = documents.query(CHUNKS, &query).await?;
                for record in page.items {
                    legacy.insert(record.id, record.doc);
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }
            let implicit_lifecycle = latest
                .iter()
                .filter_map(|(id, (_, _, status))| {
                    status.clone().map(|status| (id.clone(), status))
                })
                .collect::<HashMap<_, _>>();
            let mut ids = HashSet::new();
            ids.extend(latest.keys().cloned());
            ids.extend(legacy.keys().cloned());
            let mut chunks = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some((_, chunk, _)) = latest.remove(&id) {
                    chunks.push(chunk);
                } else if let Some(value) = legacy.remove(&id) {
                    chunks.push(
                        serde_json::from_value(value)
                            .map_err(tinystoragedrivers_types::StorageError::from)?,
                    );
                }
            }
            let mut states =
                bulk_state_values_at_snapshot(&*documents, &snapshot, &visible, &state_collections)
                    .await?;
            add_implicit_lifecycle_states(&implicit_lifecycle, &mut states);
            let deleted_ids: HashSet<String> = states
                .get(CHUNK_LIFECYCLE)
                .into_iter()
                .flatten()
                .filter_map(|(key, value)| {
                    (value.get("status").and_then(Value::as_str) == Some("deleted"))
                        .then(|| key.clone())
                })
                .collect();
            chunks.retain(|chunk| !deleted_ids.contains(&chunk.id));
            Ok::<_, tinystoragedrivers_types::StorageError>((chunks, states))
        })?
        .map(|(chunks, states)| (return_snapshot, chunks, states))
        .map_err(anyhow::Error::from)
}

fn add_implicit_lifecycle_states(
    defaults: &HashMap<String, String>,
    states: &mut HashMap<String, Vec<(String, Value)>>,
) {
    let lifecycle = states.entry(CHUNK_LIFECYCLE.to_owned()).or_default();
    let explicit = lifecycle
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<HashSet<_>>();
    lifecycle.extend(defaults.iter().filter_map(|(key, status)| {
        (!explicit.contains(key)).then(|| (key.clone(), serde_json::json!({"status": status})))
    }));
}

/// Async materialization of a chunk and state snapshot for current-thread
/// Tokio callers. The returned head is the strict precondition for a later
/// multi-record delete or cascade.
pub(crate) async fn list_chunk_snapshot_with_head_async(
    config: &MemoryConfig,
    requested_state: &[&str],
) -> Result<(
    HeadSnapshot,
    Vec<Chunk>,
    HashMap<String, Vec<(String, Value)>>,
)> {
    let snapshot = capture_head_async(config).await?;
    let documents = config.storage()?.documents().clone();
    let mut state_collections = requested_state
        .iter()
        .map(|collection| (*collection).to_owned())
        .collect::<Vec<_>>();
    if !state_collections.iter().any(|name| name == CHUNK_LIFECYCLE) {
        state_collections.push(CHUNK_LIFECYCLE.to_owned());
    }
    let read_snapshot = snapshot.clone();
    let (mut chunks, states) = config
        .storage_blocking()?
        .run_async(async move {
            let manifests = load_operation_manifests(&*documents).await?;
            let visible = visible_operations_at_snapshot(&read_snapshot, &manifests)?;
            let mut latest = HashMap::<String, (u64, Chunk, Option<String>)>::new();
            let mut query = Query::all().limit(PAGE_SIZE);
            loop {
                let page = documents.query(CHUNK_VERSIONS, &query).await?;
                for record in page.items {
                    if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "chunk version has unexpected schema or expiry metadata",
                        ));
                    }
                    let version: ChunkVersion = serde_json::from_value(record.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    if version.chunk.id != version.logical_id
                        || version_id(&version.logical_id, &version.operation_id) != record.id
                    {
                        continue;
                    }
                    let Some(operation) = visible.get(&version.operation_id) else {
                        continue;
                    };
                    if operation.revision != version.revision
                        || !operation
                            .writes
                            .contains(&(version.logical_id.clone(), record.id.clone()))
                    {
                        continue;
                    }
                    if latest
                        .get(&version.logical_id)
                        .is_none_or(|(revision, _, _)| version.revision > *revision)
                    {
                        latest.insert(
                            version.logical_id,
                            (
                                version.revision,
                                version.chunk,
                                operation.default_lifecycle_status.clone(),
                            ),
                        );
                    }
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }
            let mut legacy = HashMap::new();
            let mut query = Query::all().limit(PAGE_SIZE);
            loop {
                let page = documents.query(CHUNKS, &query).await?;
                for record in page.items {
                    legacy.insert(record.id, record.doc);
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }
            let implicit_lifecycle = latest
                .iter()
                .filter_map(|(id, (_, _, status))| {
                    status.clone().map(|status| (id.clone(), status))
                })
                .collect::<HashMap<_, _>>();
            let mut ids = HashSet::new();
            ids.extend(latest.keys().cloned());
            ids.extend(legacy.keys().cloned());
            let mut chunks = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some((_, chunk, _)) = latest.remove(&id) {
                    chunks.push(chunk);
                } else if let Some(value) = legacy.remove(&id) {
                    chunks.push(
                        serde_json::from_value(value)
                            .map_err(tinystoragedrivers_types::StorageError::from)?,
                    );
                }
            }
            let mut states = bulk_state_values_at_snapshot(
                &*documents,
                &read_snapshot,
                &visible,
                &state_collections,
            )
            .await?;
            add_implicit_lifecycle_states(&implicit_lifecycle, &mut states);
            Ok::<_, tinystoragedrivers_types::StorageError>((chunks, states))
        })
        .await??;
    let deleted: HashSet<String> = states
        .get(CHUNK_LIFECYCLE)
        .into_iter()
        .flatten()
        .filter_map(|(key, value)| {
            (value.get("status").and_then(Value::as_str) == Some("deleted")).then(|| key.clone())
        })
        .collect();
    chunks.retain(|chunk| !deleted.contains(&chunk.id));
    Ok((snapshot, chunks, states))
}

/// Async batch read for async host workflows, preserving current-thread runtimes.
pub(crate) async fn get_chunks_batch_async(
    config: &MemoryConfig,
    ids: &[String],
) -> Result<HashMap<String, Chunk>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let documents = config.storage()?.documents().clone();
    let blocking = config.storage_blocking()?.clone();
    let snapshot_documents = documents.clone();
    let snapshot = blocking
        .run_async(async move {
            let Some(record) = snapshot_documents.get(COMMIT_HEADS, "head").await? else {
                return Ok(HeadSnapshot {
                    op_id: None,
                    version: None,
                    revision: 0,
                });
            };
            if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "commit head has unexpected schema or expiry metadata",
                ));
            }
            let head: CommitHead = serde_json::from_value(record.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            Ok(HeadSnapshot {
                op_id: Some(head.op_id),
                version: Some(record.version),
                revision: head.revision,
            })
        })
        .await?
        .map_err(anyhow::Error::from)?;
    let ids = ids.to_vec();
    let values = blocking
        .run_async(async move {
            let mut chunks = HashMap::with_capacity(ids.len());
            for id in ids {
                if let Some(chunk) = read_chunk_at(&*documents, &snapshot, &id).await? {
                    chunks.insert(id, chunk);
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(chunks)
        })
        .await??;
    Ok(values)
}

async fn read_chunk_at(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    logical_id: &str,
) -> tinystoragedrivers_types::Result<Option<Chunk>> {
    if read_state_at(documents, snapshot, CHUNK_LIFECYCLE, logical_id)
        .await?
        .and_then(|value| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some("deleted")
    {
        return Ok(None);
    }
    let mut query = Query::filter(Filter::eq("logical_id", logical_id))
        .sort(Sort::desc("revision"))
        .limit(PAGE_SIZE);
    loop {
        let page = documents.query(CHUNK_VERSIONS, &query).await?;
        for row in page.items {
            if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "chunk version has unexpected schema or expiry metadata",
                ));
            }
            let stored_id = row.id;
            let version: ChunkVersion = serde_json::from_value(row.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if version.logical_id == logical_id
                && version.chunk.id == logical_id
                && version_id(logical_id, &version.operation_id) == stored_id
                && operation_contains_chunk_write(
                    documents,
                    snapshot,
                    &version.operation_id,
                    version.revision,
                    &stored_id,
                )
                .await?
            {
                return Ok(Some(version.chunk));
            }
        }
        match page.next {
            Some(cursor) => query = query.clone().after(cursor),
            None => break,
        }
    }

    // Typed imports may seed the stable legacy document id. It is the depth-0
    // value until a committed version supersedes it.
    let legacy = documents
        .get(CHUNKS, logical_id)
        .await?
        .map(|record| serde_json::from_value::<Chunk>(record.doc))
        .transpose()
        .map_err(tinystoragedrivers_types::StorageError::from)?;
    Ok(legacy)
}

pub(crate) fn read_chunk_lifecycle_status(
    config: &MemoryConfig,
    chunk_id: &str,
) -> Result<Option<String>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let chunk_id = chunk_id.to_owned();
    config
        .storage_blocking()?
        .run(async move {
            let explicit =
                read_state_at(&*documents, &snapshot, CHUNK_LIFECYCLE, &chunk_id).await?;
            if let Some(value) = explicit {
                return Ok(value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned));
            }
            implicit_chunk_lifecycle_status(&*documents, &snapshot, &chunk_id).await
        })?
        .map_err(anyhow::Error::from)
}

async fn implicit_chunk_lifecycle_status(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    logical_id: &str,
) -> tinystoragedrivers_types::Result<Option<String>> {
    let mut query = Query::filter(Filter::eq("logical_id", logical_id))
        .sort(Sort::desc("revision"))
        .limit(PAGE_SIZE);
    loop {
        let page = documents.query(CHUNK_VERSIONS, &query).await?;
        for row in page.items {
            let version: ChunkVersion = serde_json::from_value(row.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if version.logical_id != logical_id
                || version.chunk.id != logical_id
                || version_id(logical_id, &version.operation_id) != row.id
            {
                continue;
            }
            let Some(manifest) =
                manifest_at_revision(documents, &version.operation_id, version.revision).await?
            else {
                continue;
            };
            if manifest.revision <= snapshot.revision
                && manifest.default_lifecycle_status.is_some()
                && operation_contains_chunk_write(
                    documents,
                    snapshot,
                    &version.operation_id,
                    version.revision,
                    &row.id,
                )
                .await?
            {
                return Ok(manifest.default_lifecycle_status);
            }
        }
        match page.next {
            Some(cursor) => query = query.clone().after(cursor),
            None => break,
        }
    }
    Ok(None)
}

pub(crate) fn read_state(
    config: &MemoryConfig,
    collection: &str,
    key: &str,
) -> Result<Option<Value>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    let key = key.to_owned();
    config
        .storage_blocking()?
        .run(async move { read_state_at(&*documents, &snapshot, &collection, &key).await })?
        .map_err(anyhow::Error::from)
}

/// Async state read through one captured commit head.
pub(crate) async fn read_state_async(
    config: &MemoryConfig,
    collection: &str,
    key: &str,
) -> Result<Option<Value>> {
    let snapshot = capture_head_async(config).await?;
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    let key = key.to_owned();
    config
        .storage_blocking()?
        .run_async(async move { read_state_at(&*documents, &snapshot, &collection, &key).await })
        .await?
        .map_err(anyhow::Error::from)
}

/// Read multiple state keys against one captured commit head.
pub(crate) async fn read_states_async(
    config: &MemoryConfig,
    collection: &str,
    keys: &[String],
) -> Result<HashMap<String, Value>> {
    let snapshot = capture_head_async(config).await?;
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    let keys = keys.to_vec();
    config
        .storage_blocking()?
        .run_async(async move {
            let mut values = HashMap::with_capacity(keys.len());
            for key in keys {
                if let Some(value) =
                    read_state_at(&*documents, &snapshot, &collection, &key).await?
                {
                    values.insert(key, value);
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(values)
        })
        .await?
        .map_err(anyhow::Error::from)
}

/// Read state keys spanning collections against one captured owner head.
pub(crate) async fn read_state_keys_async(
    config: &MemoryConfig,
    keys: &[StateKey],
) -> Result<Vec<Option<Value>>> {
    let snapshot = capture_head_async(config).await?;
    let documents = config.storage()?.documents().clone();
    let keys = keys.to_vec();
    config
        .storage_blocking()?
        .run_async(async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values
                    .push(read_state_at(&*documents, &snapshot, &key.collection, &key.key).await?);
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(values)
        })
        .await?
        .map_err(anyhow::Error::from)
}

/// Read multiple state keys against one captured head revision.
pub(crate) fn read_states(
    config: &MemoryConfig,
    collection: &str,
    keys: &[String],
) -> Result<HashMap<String, Value>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    let keys = keys.to_vec();
    config
        .storage_blocking()?
        .run(async move {
            let mut result = HashMap::with_capacity(keys.len());
            for key in keys {
                if let Some(value) =
                    read_state_at(&*documents, &snapshot, &collection, &key).await?
                {
                    result.insert(key, value);
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(result)
        })?
        .map_err(anyhow::Error::from)
}

/// Synchronous counterpart of [`read_state_keys_async`].
pub(crate) fn read_state_keys(
    config: &MemoryConfig,
    keys: &[StateKey],
) -> Result<Vec<Option<Value>>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let keys = keys.to_vec();
    config
        .storage_blocking()?
        .run(async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values
                    .push(read_state_at(&*documents, &snapshot, &key.collection, &key.key).await?);
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(values)
        })?
        .map_err(anyhow::Error::from)
}

pub(crate) fn write_state(config: &MemoryConfig, change: StateChange) -> Result<()> {
    write_state_changes(config, &[change])
}

async fn state_change_matches_at(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    condition: &StateChange,
) -> tinystoragedrivers_types::Result<bool> {
    let mut actual =
        read_state_at(documents, snapshot, &condition.collection, &condition.key).await?;
    if actual.is_none() && condition.collection == CHUNK_LIFECYCLE {
        let implicit = implicit_chunk_lifecycle_status(documents, snapshot, &condition.key)
            .await?
            .map(|status| serde_json::json!({"status": status}));
        if implicit.is_some() {
            return Ok(condition.value == implicit);
        }
    }
    Ok(actual == condition.value)
}

/// Commit domain mutations only while every declared state value still
/// matches. The values are re-read on each head-CAS retry, fencing stale
/// workers after another lease epoch has become visible.
pub(crate) fn commit_if_state_matches(
    config: &MemoryConfig,
    expected: &[StateChange],
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<bool> {
    let mut last_conflict = None;
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head(config)?;
        let documents = config.storage()?.documents().clone();
        let conditions = expected.to_vec();
        let read_snapshot = snapshot.clone();
        let matches = config.storage_blocking()?.run(async move {
            for condition in conditions {
                if !state_change_matches_at(&*documents, &read_snapshot, &condition).await? {
                    return Ok::<_, tinystoragedrivers_types::StorageError>(false);
                }
            }
            Ok(true)
        })??;
        if !matches {
            return Ok(false);
        }
        match upsert_chunks_at(config, &snapshot, chunks, chunks, changes, None) {
            Ok(_) => return Ok(true),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("conditional state commit did not succeed within the retry budget")
    }))
}

/// Async conditional commit for async host workflows, preserving current-
/// thread runtime compatibility while retaining the same captured-head CAS.
pub(crate) async fn commit_if_state_matches_async(
    config: &MemoryConfig,
    expected: &[StateChange],
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<bool> {
    let mut last_conflict = None;
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head_async(config).await?;
        let documents = config.storage()?.documents().clone();
        let conditions = expected.to_vec();
        let read_snapshot = snapshot.clone();
        let matches = config
            .storage_blocking()?
            .run_async(async move {
                for condition in conditions {
                    if !state_change_matches_at(&*documents, &read_snapshot, &condition).await? {
                        return Ok::<_, tinystoragedrivers_types::StorageError>(false);
                    }
                }
                Ok(true)
            })
            .await??;
        if !matches {
            return Ok(false);
        }
        match upsert_chunks_at_async(config, &snapshot, chunks, chunks, changes, None).await {
            Ok(_) => return Ok(true),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("conditional state commit did not succeed within the retry budget")
    }))
}

/// Commit against the exact head used to enumerate a logical snapshot. Unlike
/// the retrying conditional API, this returns `false` when any operation has
/// advanced the owner head since `snapshot` was captured, even if the changed
/// operation touched a previously absent key.
pub(crate) async fn commit_if_state_matches_at_snapshot_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    expected: &[StateChange],
    chunks: &[Chunk],
    changes: &[StateChange],
) -> Result<bool> {
    let documents = config.storage()?.documents().clone();
    let conditions = expected.to_vec();
    let read_snapshot = snapshot.clone();
    let matches = config
        .storage_blocking()?
        .run_async(async move {
            for condition in conditions {
                if !state_change_matches_at(&*documents, &read_snapshot, &condition).await? {
                    return Ok::<_, tinystoragedrivers_types::StorageError>(false);
                }
            }
            Ok(true)
        })
        .await??;
    if !matches || capture_head_async(config).await? != *snapshot {
        return Ok(false);
    }
    match upsert_chunks_at_async(config, snapshot, chunks, chunks, changes, None).await {
        Ok(_) => Ok(true),
        Err(error) if is_storage_conflict(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Synchronous counterpart for deletion APIs that enumerate through a
/// blocking host store and must reject phantom rows before committing.
pub(crate) fn commit_if_state_matches_at_snapshot(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    expected: &[StateChange],
    changes: &[StateChange],
) -> Result<bool> {
    let documents = config.storage()?.documents().clone();
    let conditions = expected.to_vec();
    let read_snapshot = snapshot.clone();
    let matches = config.storage_blocking()?.run(async move {
        for condition in conditions {
            if !state_change_matches_at(&*documents, &read_snapshot, &condition).await? {
                return Ok::<_, tinystoragedrivers_types::StorageError>(false);
            }
        }
        Ok(true)
    })??;
    if !matches || capture_head(config)? != *snapshot {
        return Ok(false);
    }
    match upsert_chunks_at(config, snapshot, &[], &[], changes, None) {
        Ok(_) => Ok(true),
        Err(error) if is_storage_conflict(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) fn insert_state_changes_if_absent(
    config: &MemoryConfig,
    changes: &[StateChange],
) -> Result<usize> {
    if changes.is_empty() {
        return Ok(0);
    }
    let mut last_conflict = None;
    for _ in 0..CAS_RETRIES {
        let snapshot = capture_head(config)?;
        let docs = config.storage()?.documents().clone();
        let input = changes.to_vec();
        let read_snapshot = snapshot.clone();
        let missing = config.storage_blocking()?.run(async move {
            let mut missing = Vec::new();
            for change in input {
                if read_state_at(&*docs, &read_snapshot, &change.collection, &change.key)
                    .await?
                    .is_none()
                {
                    missing.push(change);
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(missing)
        })??;
        if missing.is_empty() {
            return Ok(0);
        }
        let absent = missing
            .iter()
            .map(|change| StateChange {
                collection: change.collection.clone(),
                key: change.key.clone(),
                value: None,
            })
            .collect::<Vec<_>>();
        match commit_if_state_matches(config, &absent, &[], &missing) {
            Ok(true) => return Ok(missing.len()),
            Ok(false) => continue,
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("conditional state commit did not succeed within the retry budget")
    }))
}

pub(crate) fn list_state(config: &MemoryConfig, collection: &str) -> Result<Vec<(String, Value)>> {
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    config
        .storage_blocking()?
        .run(async move {
            let mut query = Query::filter(Filter::eq("collection", collection.clone()))
                .sort(Sort::asc("key"))
                .limit(PAGE_SIZE);
            let mut keys = HashSet::new();
            loop {
                let page = documents.query(STATE_VERSIONS, &query).await?;
                for row in page.items {
                    if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "state version has unexpected schema or expiry metadata",
                        ));
                    }
                    let version: StateVersion = serde_json::from_value(row.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    if version.collection == collection {
                        keys.insert(version.key);
                    }
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }
            let mut values = Vec::new();
            for key in keys {
                if let Some(value) =
                    read_state_at(&*documents, &snapshot, &collection, &key).await?
                {
                    values.push((key, value));
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(values)
        })?
        .map_err(anyhow::Error::from)
}

/// Async materialization of current values in a state collection under one
/// captured commit head.
pub(crate) async fn list_state_async(
    config: &MemoryConfig,
    collection: &str,
) -> Result<Vec<(String, Value)>> {
    let snapshot = capture_head_async(config).await?;
    list_state_at_snapshot_async(config, &snapshot, collection).await
}

/// Enumerate state values at a previously captured head. This permits callers
/// to enumerate several collections under one snapshot and use the same token
/// as a strict commit precondition, preventing missing-key phantoms.
pub(crate) async fn list_state_at_snapshot_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    collection: &str,
) -> Result<Vec<(String, Value)>> {
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
    let snapshot = snapshot.clone();
    config
        .storage_blocking()?
        .run_async(async move {
            let mut query = Query::filter(Filter::eq("collection", collection.clone()))
                .sort(Sort::asc("key"))
                .limit(PAGE_SIZE);
            let mut keys = HashSet::new();
            loop {
                let page = documents.query(STATE_VERSIONS, &query).await?;
                for row in page.items {
                    if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                        return Err(tinystoragedrivers_types::StorageError::backend(
                            "state version has unexpected schema or expiry metadata",
                        ));
                    }
                    let version: StateVersion = serde_json::from_value(row.doc)
                        .map_err(tinystoragedrivers_types::StorageError::from)?;
                    if version.collection == collection {
                        keys.insert(version.key);
                    }
                }
                match page.next {
                    Some(cursor) => query = query.clone().after(cursor),
                    None => break,
                }
            }
            let mut values = Vec::new();
            for key in keys {
                if let Some(value) =
                    read_state_at(&*documents, &snapshot, &collection, &key).await?
                {
                    values.push((key, value));
                }
            }
            Ok::<_, tinystoragedrivers_types::StorageError>(values)
        })
        .await?
        .map_err(anyhow::Error::from)
}

async fn read_state_at(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    collection: &str,
    key: &str,
) -> tinystoragedrivers_types::Result<Option<Value>> {
    let mut query = Query::filter(Filter::eq("collection", collection).and(Filter::eq("key", key)))
        .sort(Sort::desc("revision"))
        .limit(PAGE_SIZE);
    loop {
        let page = documents.query(STATE_VERSIONS, &query).await?;
        for row in page.items {
            if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "state version has unexpected schema or expiry metadata",
                ));
            }
            let version_id_value = row.id;
            let version: StateVersion = serde_json::from_value(row.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if version.collection != collection || version.key != key {
                continue;
            }
            if version.revision <= snapshot.revision
                && state_version_id(collection, key, &version.operation_id) == version_id_value
                && operation_contains_state_write(
                    documents,
                    snapshot,
                    &version.operation_id,
                    version.revision,
                    &version_id_value,
                    collection,
                    key,
                )
                .await?
            {
                return Ok(version.value);
            }
        }
        match page.next {
            Some(cursor) => query = query.clone().after(cursor),
            None => break,
        }
    }
    Ok(None)
}

async fn load_operation_manifests(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
) -> tinystoragedrivers_types::Result<HashMap<String, OperationManifest>> {
    let mut manifests = HashMap::new();
    let mut query = Query::all().limit(PAGE_SIZE);
    loop {
        let page = documents.query(OPERATIONS, &query).await?;
        for row in page.items {
            if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest has unexpected schema or expiry metadata",
                ));
            }
            let manifest: OperationManifest = serde_json::from_value(row.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            manifests.insert(row.id, manifest);
        }
        match page.next {
            Some(cursor) => query = query.clone().after(cursor),
            None => break,
        }
    }
    Ok(manifests)
}

fn visible_operations_at_snapshot(
    snapshot: &HeadSnapshot,
    manifests: &HashMap<String, OperationManifest>,
) -> tinystoragedrivers_types::Result<HashMap<String, VisibleOperation>> {
    let mut visible = HashMap::new();
    let (Some(head_id), Some(_)) = (snapshot.op_id.as_deref(), snapshot.version.as_ref()) else {
        return Ok(visible);
    };

    let mut current_id = Some(head_id.to_owned());
    let mut expected_revision = snapshot.revision;
    let mut chain = Vec::new();
    while let Some(id) = current_id {
        let manifest = manifests.get(&id).ok_or_else(|| {
            tinystoragedrivers_types::StorageError::backend(
                "commit head references a missing operation manifest",
            )
        })?;
        if manifest.op_id != id || manifest.revision != expected_revision {
            return Err(tinystoragedrivers_types::StorageError::backend(
                "commit head does not match its operation manifest",
            ));
        }
        chain.push((id.clone(), manifest.clone()));
        current_id = manifest.parent_op.clone();
        if let Some(parent) = current_id.as_ref() {
            if manifest.ancestors.first() != Some(parent) {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest parent does not match its ancestor index",
                ));
            }
            expected_revision = expected_revision.checked_sub(1).ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend(
                    "operation manifest revision underflow",
                )
            })?;
        } else if expected_revision != 1 {
            return Err(tinystoragedrivers_types::StorageError::backend(
                "operation manifest chain ended before the root revision",
            ));
        }
    }

    // Validate the binary-lifting index used by point reads. The bulk reader
    // follows parent links once, but must reject a corrupt index rather than
    // disagreeing with point-read visibility at the same snapshot.
    for (index, (_, manifest)) in chain.iter().enumerate() {
        for (level, ancestor) in manifest.ancestors.iter().enumerate() {
            let distance = 1_usize.checked_shl(level as u32).ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend(
                    "operation ancestor index is too deep",
                )
            })?;
            let expected = index.checked_add(distance).and_then(|i| chain.get(i));
            if expected.map(|(id, _)| id) != Some(ancestor) {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest has an invalid ancestor index",
                ));
            }
        }
    }
    visible.extend(chain.into_iter().map(|(id, manifest)| {
        let writes = manifest
            .writes
            .into_iter()
            .map(|write| (write.logical_id, write.version_id))
            .collect();
        (
            id,
            VisibleOperation {
                revision: manifest.revision,
                writes,
                state_writes: manifest
                    .state_writes
                    .into_iter()
                    .map(|write| (write.collection, write.key, write.version_id))
                    .collect(),
                default_lifecycle_status: manifest.default_lifecycle_status,
            },
        )
    }));
    Ok(visible)
}

/// Resolve several state collections from one already-loaded commit prefix.
/// State versions are scanned once per requested collection and matched to
/// their immutable operation manifest in memory, avoiding one query per key.
async fn bulk_state_values_at_snapshot(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    visible: &HashMap<String, VisibleOperation>,
    collections: &[String],
) -> tinystoragedrivers_types::Result<HashMap<String, Vec<(String, Value)>>> {
    let mut output = HashMap::with_capacity(collections.len());
    for collection in collections {
        let mut latest = HashMap::<String, (u64, Option<Value>)>::new();
        let mut query = Query::filter(Filter::eq("collection", collection.clone()))
            .sort(Sort::asc("key"))
            .limit(PAGE_SIZE);
        loop {
            let page = documents.query(STATE_VERSIONS, &query).await?;
            for row in page.items {
                if row.meta.schema != RECORD_SCHEMA || row.meta.expires_ms.is_some() {
                    return Err(tinystoragedrivers_types::StorageError::backend(
                        "state version has unexpected schema or expiry metadata",
                    ));
                }
                let version: StateVersion = serde_json::from_value(row.doc)
                    .map_err(tinystoragedrivers_types::StorageError::from)?;
                if version.collection != *collection
                    || version.revision > snapshot.revision
                    || state_version_id(collection, &version.key, &version.operation_id) != row.id
                {
                    continue;
                }
                let Some(operation) = visible.get(&version.operation_id) else {
                    continue;
                };
                if operation.revision != version.revision
                    || !operation.state_writes.contains(&(
                        collection.clone(),
                        version.key.clone(),
                        row.id,
                    ))
                {
                    continue;
                }
                if latest
                    .get(&version.key)
                    .is_none_or(|(revision, _)| version.revision > *revision)
                {
                    latest.insert(version.key, (version.revision, version.value));
                }
            }
            match page.next {
                Some(cursor) => query = query.clone().after(cursor),
                None => break,
            }
        }
        output.insert(
            collection.clone(),
            latest
                .into_iter()
                .filter_map(|(key, (_, value))| value.map(|value| (key, value)))
                .collect(),
        );
    }
    Ok(output)
}

async fn operation_contains_chunk_write(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    candidate_id: &str,
    candidate_revision: u64,
    version_id: &str,
) -> tinystoragedrivers_types::Result<bool> {
    if candidate_revision > snapshot.revision {
        return Ok(false);
    }
    let Some(head) = snapshot.op_id.as_deref() else {
        return Ok(false);
    };
    let Some(manifest) = manifest_at_revision(documents, head, snapshot.revision).await? else {
        return Ok(false);
    };
    let Some(candidate) = manifest_at_revision(documents, candidate_id, candidate_revision).await?
    else {
        return Ok(false);
    };
    if !manifest_is_ancestor(
        documents,
        manifest,
        &candidate,
        snapshot.revision - candidate_revision,
    )
    .await?
    {
        return Ok(false);
    }
    Ok(candidate
        .writes
        .iter()
        .any(|write| write.version_id == version_id))
}

async fn operation_contains_state_write(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    snapshot: &HeadSnapshot,
    candidate_id: &str,
    candidate_revision: u64,
    version_id: &str,
    collection: &str,
    key: &str,
) -> tinystoragedrivers_types::Result<bool> {
    if candidate_revision > snapshot.revision {
        return Ok(false);
    }
    let Some(head) = snapshot.op_id.as_deref() else {
        return Ok(false);
    };
    let Some(head_manifest) = manifest_at_revision(documents, head, snapshot.revision).await?
    else {
        return Ok(false);
    };
    let Some(candidate) = manifest_at_revision(documents, candidate_id, candidate_revision).await?
    else {
        return Ok(false);
    };
    if !manifest_is_ancestor(
        documents,
        head_manifest,
        &candidate,
        snapshot.revision - candidate_revision,
    )
    .await?
    {
        return Ok(false);
    }
    Ok(candidate.state_writes.iter().any(|write| {
        write.version_id == version_id && write.collection == collection && write.key == key
    }))
}

async fn manifest_at_revision(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    id: &str,
    revision: u64,
) -> tinystoragedrivers_types::Result<Option<OperationManifest>> {
    let Some(record) = documents.get(OPERATIONS, id).await? else {
        return Ok(None);
    };
    if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
        return Err(tinystoragedrivers_types::StorageError::backend(
            "operation manifest has unexpected schema or expiry metadata",
        ));
    }
    let operation: OperationManifest =
        serde_json::from_value(record.doc).map_err(tinystoragedrivers_types::StorageError::from)?;
    if operation.op_id != id || operation.revision != revision {
        return Err(tinystoragedrivers_types::StorageError::backend(
            "operation manifest id or revision does not match its reference",
        ));
    }
    Ok(Some(operation))
}

async fn manifest_is_ancestor(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    mut current: OperationManifest,
    target: &OperationManifest,
    mut distance: u64,
) -> tinystoragedrivers_types::Result<bool> {
    while distance > 0 {
        let jump = u64::BITS - distance.leading_zeros() - 1;
        let jump_distance = 1_u64 << jump;
        let Some(next_id) = current.ancestors.get(jump as usize) else {
            return Ok(false);
        };
        let next_revision = current.revision.checked_sub(jump_distance).ok_or_else(|| {
            tinystoragedrivers_types::StorageError::backend("invalid operation ancestor revision")
        })?;
        current = manifest_at_revision(documents, next_id, next_revision)
            .await?
            .ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend(
                    "operation manifest references a missing ancestor",
                )
            })?;
        distance -= jump_distance;
    }
    Ok(current.op_id == target.op_id)
}

fn next_revision(config: &MemoryConfig, snapshot: &HeadSnapshot) -> Result<(u64, Vec<String>)> {
    let Some(head) = snapshot.op_id.clone() else {
        return Ok((1, Vec::new()));
    };
    let documents = config.storage()?.documents().clone();
    let expected_revision = snapshot.revision;
    config
        .storage_blocking()?
        .run(async move {
            let record = documents.get(OPERATIONS, &head).await?.ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend(
                    "commit head references a missing operation manifest",
                )
            })?;
            if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest has unexpected schema or expiry metadata",
                ));
            }
            let operation: OperationManifest = serde_json::from_value(record.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if operation.op_id != head {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest id does not match its document id",
                ));
            }
            if operation.revision != expected_revision {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "commit head revision does not match its operation manifest",
                ));
            }
            let revision = operation.revision.checked_add(1).ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend("commit revision overflow")
            })?;
            let mut ancestors = vec![head];
            for level in 1..u64::BITS as usize {
                let previous_distance = 1_u64 << (level - 1);
                if previous_distance >= revision {
                    break;
                }
                let previous_id = ancestors[level - 1].clone();
                let previous_revision = revision - previous_distance;
                let previous = manifest_at_revision(&*documents, &previous_id, previous_revision)
                    .await?
                    .ok_or_else(|| {
                        tinystoragedrivers_types::StorageError::backend(
                            "operation manifest references a missing ancestor",
                        )
                    })?;
                let Some(ancestor) = previous.ancestors.get(level - 1) else {
                    break;
                };
                ancestors.push(ancestor.clone());
            }
            Ok::<_, tinystoragedrivers_types::StorageError>((revision, ancestors))
        })?
        .map_err(anyhow::Error::from)
}

async fn next_revision_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
) -> Result<(u64, Vec<String>)> {
    let Some(head) = snapshot.op_id.clone() else {
        return Ok((1, Vec::new()));
    };
    let documents = config.storage()?.documents().clone();
    let expected_revision = snapshot.revision;
    config
        .storage_blocking()?
        .run_async(async move {
            let record = documents.get(OPERATIONS, &head).await?.ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend(
                    "commit head references a missing operation manifest",
                )
            })?;
            if record.meta.schema != RECORD_SCHEMA || record.meta.expires_ms.is_some() {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "operation manifest has unexpected schema or expiry metadata",
                ));
            }
            let operation: OperationManifest = serde_json::from_value(record.doc)
                .map_err(tinystoragedrivers_types::StorageError::from)?;
            if operation.op_id != head || operation.revision != expected_revision {
                return Err(tinystoragedrivers_types::StorageError::backend(
                    "commit head does not match its operation manifest",
                ));
            }
            let revision = operation.revision.checked_add(1).ok_or_else(|| {
                tinystoragedrivers_types::StorageError::backend("commit revision overflow")
            })?;
            let mut ancestors = vec![head];
            for level in 1..u64::BITS as usize {
                let previous_distance = 1_u64 << (level - 1);
                if previous_distance >= revision {
                    break;
                }
                let previous_id = ancestors[level - 1].clone();
                let previous_revision = revision - previous_distance;
                let previous = manifest_at_revision(&*documents, &previous_id, previous_revision)
                    .await?
                    .ok_or_else(|| {
                        tinystoragedrivers_types::StorageError::backend(
                            "operation manifest references a missing ancestor",
                        )
                    })?;
                let Some(ancestor) = previous.ancestors.get(level - 1) else {
                    break;
                };
                ancestors.push(ancestor.clone());
            }
            Ok::<_, tinystoragedrivers_types::StorageError>((revision, ancestors))
        })
        .await?
        .map_err(anyhow::Error::from)
}

fn make_manifest(
    snapshot: &HeadSnapshot,
    revision: u64,
    ancestors: Vec<String>,
    chunks: &[Chunk],
    changes: &[StateChange],
    default_lifecycle_status: Option<&str>,
) -> Result<OperationManifest> {
    anyhow::ensure!(
        changes
            .iter()
            .all(|change| !change.value.as_ref().is_some_and(Value::is_null)),
        "JSON null state values are unsupported; use a tombstone or a tagged domain value"
    );
    let mut digest = Sha256::new();
    digest.update(snapshot.op_id.as_deref().unwrap_or("root").as_bytes());
    for chunk in chunks {
        digest.update(serde_json::to_vec(chunk)?);
    }
    for change in changes {
        digest.update(serde_json::to_vec(change)?);
    }
    if let Some(status) = default_lifecycle_status {
        digest.update(b"default-lifecycle-status\0");
        digest.update(status.as_bytes());
    }
    let op_id = format!("memory-write:{:x}", digest.finalize());
    Ok(OperationManifest {
        op_id: op_id.clone(),
        parent_op: snapshot.op_id.clone(),
        revision,
        ancestors,
        writes: chunks
            .iter()
            .map(|chunk| ManifestWrite {
                logical_id: chunk.id.clone(),
                version_id: version_id(&chunk.id, &op_id),
            })
            .collect(),
        state_writes: changes
            .iter()
            .map(|change| StateManifestWrite {
                collection: change.collection.clone(),
                key: change.key.clone(),
                version_id: state_version_id(&change.collection, &change.key, &op_id),
            })
            .collect(),
        default_lifecycle_status: default_lifecycle_status.map(str::to_owned),
    })
}

fn version_id(logical_id: &str, operation_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(logical_id.as_bytes());
    digest.update([0]);
    digest.update(operation_id.as_bytes());
    format!("chunk-version:{:x}", digest.finalize())
}

fn state_version_id(collection: &str, key: &str, operation_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(collection.as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    digest.update([0]);
    digest.update(operation_id.as_bytes());
    format!("state-version:{:x}", digest.finalize())
}

async fn put_immutable(
    documents: &dyn tinystoragedrivers_api::DocumentStore,
    collection: &str,
    id: &str,
    body: Value,
) -> tinystoragedrivers_types::Result<()> {
    match documents
        .put_with(
            collection,
            id,
            body.clone(),
            Precondition::Absent,
            WriteOptions::new().schema(RECORD_SCHEMA),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
            let existing = documents.get(collection, id).await?.ok_or_else(|| {
                tinystoragedrivers_types::StorageError::conflict(
                    "immutable record disappeared after a conflict",
                )
            })?;
            if existing.doc == body
                && existing.meta.schema == RECORD_SCHEMA
                && existing.meta.expires_ms.is_none()
            {
                Ok(())
            } else {
                Err(tinystoragedrivers_types::StorageError::conflict(
                    "immutable record id already contains different data",
                ))
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "versioned_tests.rs"]
mod tests;
