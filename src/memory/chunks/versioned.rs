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
    CHUNK_CONTENT_POINTERS, CHUNK_VERSIONS, CHUNKS, COMMIT_HEADS, OPERATIONS, STATE_VERSIONS,
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

/// Write a chunk batch as immutable versions and commit them in one head CAS.
///
/// # Errors
/// Returns an error if a staged value conflicts, the captured head changed,
/// or the host document store fails.
pub(crate) fn upsert_chunks(config: &MemoryConfig, chunks: &[Chunk]) -> Result<usize> {
    commit_operation(config, chunks, &[])
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
        if is_current_commit_retry(config, &snapshot, &final_chunks, &final_changes)? {
            return Ok(chunks.len());
        }
        match upsert_chunks_at(config, &snapshot, chunks, &final_chunks, &final_changes) {
            Ok(count) => return Ok(count),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("chunk version commit did not succeed within the retry budget")
    }))
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
        match upsert_chunks_at(config, &snapshot, &[], &[], changes) {
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
) -> Result<usize> {
    let (revision, ancestors) = next_revision(config, snapshot)?;
    let operation = make_manifest(snapshot, revision, ancestors, final_chunks, changes)?;
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
) -> Result<usize> {
    let (revision, ancestors) = next_revision_async(config, snapshot).await?;
    let operation = make_manifest(snapshot, revision, ancestors, final_chunks, changes)?;
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
) -> Result<bool> {
    let Some(snapshot) = head.op_id.clone() else {
        return Ok(false);
    };
    let documents = config.storage()?.documents().clone();
    let expected = chunks.to_vec();
    let expected_changes = changes.to_vec();
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
            if operation.writes.len() != expected.len()
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
    let snapshot = capture_head(config)?;
    let documents = config.storage()?.documents().clone();
    let state_collections = state_collections
        .iter()
        .map(|collection| (*collection).to_owned())
        .collect::<Vec<_>>();
    config
        .storage_blocking()?
        .run(async move {
            let mut ids = HashSet::new();
            for collection in [CHUNK_VERSIONS, CHUNKS] {
                let mut query = Query::all().limit(PAGE_SIZE);
                loop {
                    let page = documents.query(collection, &query).await?;
                    for record in page.items {
                        if collection == CHUNKS {
                            ids.insert(record.id);
                        } else {
                            if record.meta.schema != RECORD_SCHEMA
                                || record.meta.expires_ms.is_some()
                            {
                                return Err(tinystoragedrivers_types::StorageError::backend(
                                    "chunk version has unexpected schema or expiry metadata",
                                ));
                            }
                            let version: ChunkVersion = serde_json::from_value(record.doc)
                                .map_err(tinystoragedrivers_types::StorageError::from)?;
                            if version_id(&version.logical_id, &version.operation_id) == record.id {
                                ids.insert(version.logical_id);
                            }
                        }
                    }
                    match page.next {
                        Some(cursor) => query = query.clone().after(cursor),
                        None => break,
                    }
                }
            }
            let mut chunks = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(chunk) = read_chunk_at(&*documents, &snapshot, &id).await? {
                    chunks.push(chunk);
                }
            }
            let mut states = HashMap::new();
            for collection in state_collections {
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
                states.insert(collection, values);
            }
            Ok::<_, tinystoragedrivers_types::StorageError>((chunks, states))
        })?
        .map_err(anyhow::Error::from)
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

pub(crate) fn write_state(config: &MemoryConfig, change: StateChange) -> Result<()> {
    write_state_changes(config, &[change])
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
                if read_state_at(
                    &*documents,
                    &read_snapshot,
                    &condition.collection,
                    &condition.key,
                )
                .await?
                    != condition.value
                {
                    return Ok::<_, tinystoragedrivers_types::StorageError>(false);
                }
            }
            Ok(true)
        })??;
        if !matches {
            return Ok(false);
        }
        match upsert_chunks_at(config, &snapshot, chunks, chunks, changes) {
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
                    if read_state_at(
                        &*documents,
                        &read_snapshot,
                        &condition.collection,
                        &condition.key,
                    )
                    .await?
                        != condition.value
                    {
                        return Ok::<_, tinystoragedrivers_types::StorageError>(false);
                    }
                }
                Ok(true)
            })
            .await??;
        if !matches {
            return Ok(false);
        }
        match upsert_chunks_at_async(config, &snapshot, chunks, chunks, changes).await {
            Ok(_) => return Ok(true),
            Err(error) if is_storage_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last_conflict.unwrap_or_else(|| {
        anyhow::anyhow!("conditional state commit did not succeed within the retry budget")
    }))
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
    let documents = config.storage()?.documents().clone();
    let collection = collection.to_owned();
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
) -> Result<OperationManifest> {
    let mut digest = Sha256::new();
    digest.update(snapshot.op_id.as_deref().unwrap_or("root").as_bytes());
    for chunk in chunks {
        digest.update(serde_json::to_vec(chunk)?);
    }
    for change in changes {
        digest.update(serde_json::to_vec(change)?);
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
