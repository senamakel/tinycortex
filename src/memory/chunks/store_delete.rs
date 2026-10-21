//! Atomic chunk deletion and its dependent owner-state cleanup.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::future::Future;

use super::collections as c;
use super::raw_refs::RawRef;
use super::types::{Chunk, SourceKind};
use super::versioned::{self, HeadSnapshot, StateChange};
use crate::memory::config::MemoryConfig;

/// Delete chunks belonging to one source and atomically remove their sidecars.
pub fn delete_chunks_by_source(
    config: &MemoryConfig,
    kind: SourceKind,
    source_id: &str,
) -> Result<usize> {
    run_delete_sync(delete_chunks_by_source_async(config, kind, source_id))
}

/// Async counterpart to [`delete_chunks_by_source`].
pub async fn delete_chunks_by_source_async(
    config: &MemoryConfig,
    kind: SourceKind,
    source_id: &str,
) -> Result<usize> {
    delete_filtered_async(config, DeleteFilter::Source(kind, source_id, false)).await
}

/// Delete chunks whose source id has the exact literal prefix.
pub fn delete_chunks_by_source_prefix(
    config: &MemoryConfig,
    kind: SourceKind,
    prefix: &str,
) -> Result<usize> {
    run_delete_sync(delete_chunks_by_source_prefix_async(config, kind, prefix))
}

/// Async counterpart to [`delete_chunks_by_source_prefix`].
pub async fn delete_chunks_by_source_prefix_async(
    config: &MemoryConfig,
    kind: SourceKind,
    prefix: &str,
) -> Result<usize> {
    delete_filtered_async(config, DeleteFilter::Source(kind, prefix, true)).await
}

/// Delete chunks owned by `owner`, preserving sources that still have another
/// owner's chunks.
pub fn delete_chunks_by_owner(
    config: &MemoryConfig,
    kind: SourceKind,
    owner: &str,
) -> Result<usize> {
    run_delete_sync(delete_chunks_by_owner_async(config, kind, owner))
}

/// Async counterpart to [`delete_chunks_by_owner`].
pub async fn delete_chunks_by_owner_async(
    config: &MemoryConfig,
    kind: SourceKind,
    owner: &str,
) -> Result<usize> {
    delete_filtered_async(config, DeleteFilter::Owner(kind, owner)).await
}

/// Delete one chunk and its dependent indexes. Unknown ids are a no-op.
pub fn delete_chunk_by_id(config: &MemoryConfig, chunk_id: &str) -> Result<usize> {
    run_delete_sync(delete_chunk_by_id_async(config, chunk_id))
}

/// Async counterpart to [`delete_chunk_by_id`].
pub async fn delete_chunk_by_id_async(config: &MemoryConfig, chunk_id: &str) -> Result<usize> {
    delete_filtered_async(config, DeleteFilter::Id(chunk_id)).await
}

fn run_delete_sync<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        anyhow::bail!("synchronous chunk deletion cannot run inside a Tokio runtime; use its async counterpart");
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build chunk deletion runtime")?
        .block_on(future)
}

#[derive(Clone, Copy)]
enum DeleteFilter<'a> {
    Source(SourceKind, &'a str, bool),
    Owner(SourceKind, &'a str),
    Id(&'a str),
}

fn matches(filter: DeleteFilter<'_>, chunk: &Chunk) -> bool {
    match filter {
        DeleteFilter::Source(kind, value, prefix) => {
            chunk.metadata.source_kind == kind
                && if prefix {
                    chunk.metadata.source_id.starts_with(value)
                } else {
                    chunk.metadata.source_id == value
                }
        }
        DeleteFilter::Owner(kind, owner) => {
            chunk.metadata.source_kind == kind && chunk.metadata.owner == owner
        }
        DeleteFilter::Id(id) => chunk.id == id,
    }
}

async fn delete_filtered_async(config: &MemoryConfig, filter: DeleteFilter<'_>) -> Result<usize> {
    use c::{
        CHUNK_CONTENT_POINTERS, CHUNK_EMBEDDINGS, CHUNK_LIFECYCLE, CHUNK_REEMBED_SKIPS,
        INGESTED_SOURCES, RAW_REFS, SCORES,
    };
    for _ in 0..8 {
        let (snapshot, chunks, states) = versioned::list_chunk_snapshot_with_head_async(
            config,
            &[
                CHUNK_CONTENT_POINTERS,
                CHUNK_EMBEDDINGS,
                CHUNK_REEMBED_SKIPS,
                INGESTED_SOURCES,
                RAW_REFS,
                SCORES,
            ],
        )
        .await?;
        let selected = chunks
            .iter()
            .filter(|chunk| matches(filter, chunk))
            .cloned()
            .collect::<Vec<_>>();
        if selected.is_empty() {
            if matches!(filter, DeleteFilter::Source(_, _, _)) {
                let mut expected = Vec::new();
                let mut changes = Vec::new();
                stage_ingest_gates(&mut expected, &mut changes, filter, &[], &[], &states);
                if !changes.is_empty()
                    && !versioned::commit_if_state_matches_at_snapshot_async(
                        config,
                        &snapshot,
                        &expected,
                        &[],
                        &changes,
                    )
                    .await?
                {
                    continue;
                }
            }
            return Ok(0);
        }
        let selected_ids = selected
            .iter()
            .map(|chunk| chunk.id.as_str())
            .collect::<HashSet<_>>();
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        let mut content_paths = Vec::new();
        for chunk in &selected {
            stage_delete(
                &mut expected,
                &mut changes,
                CHUNK_LIFECYCLE,
                &chunk.id,
                states
                    .get(CHUNK_LIFECYCLE)
                    .and_then(|rows| rows.iter().find(|(key, _)| key == &chunk.id))
                    .map(|(_, value)| value.clone()),
                Some(serde_json::json!({"status":"deleted"})),
            );
        }
        for collection in [CHUNK_CONTENT_POINTERS, RAW_REFS, SCORES] {
            for (key, value) in state_rows(&states, collection) {
                let chunk_id = value
                    .get("chunk_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(key);
                if selected_ids.contains(chunk_id) {
                    if collection == CHUNK_CONTENT_POINTERS {
                        if let Some(path) = value
                            .get("content_path")
                            .and_then(serde_json::Value::as_str)
                        {
                            content_paths.push(path.to_owned());
                        }
                    }
                    stage_delete(
                        &mut expected,
                        &mut changes,
                        collection,
                        key,
                        Some(value.clone()),
                        None,
                    );
                }
            }
        }
        for collection in [CHUNK_EMBEDDINGS, CHUNK_REEMBED_SKIPS] {
            for (key, value) in state_rows(&states, collection) {
                if value
                    .get("chunk_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| selected_ids.contains(id))
                {
                    stage_delete(
                        &mut expected,
                        &mut changes,
                        collection,
                        key,
                        Some(value.clone()),
                        None,
                    );
                }
            }
        }
        let candidate_raw = raw_paths_for(&selected, &states);
        let survivors = chunks
            .iter()
            .filter(|chunk| !selected_ids.contains(chunk.id.as_str()))
            .collect::<Vec<_>>();
        let mut surviving_raw = HashSet::new();
        let mut corrupt_survivor = false;
        for (key, value) in state_rows(&states, RAW_REFS) {
            if survivors.iter().any(|chunk| chunk.id.as_str() == key) {
                match serde_json::from_value::<Vec<RawRef>>(value.clone()) {
                    Ok(refs) => surviving_raw.extend(refs.into_iter().map(|raw_ref| raw_ref.path)),
                    Err(_) => corrupt_survivor = true,
                }
            }
        }
        if !corrupt_survivor {
            let gate_rows = state_rows(&states, INGESTED_SOURCES);
            for path in candidate_raw.difference(&surviving_raw) {
                content_paths.push(path.clone());
                for (key, value) in gate_rows {
                    if value.get("source_kind").and_then(serde_json::Value::as_str)
                        == Some(super::store_sources::RAW_FILE_GATE_KIND)
                        && value.get("source_id").and_then(serde_json::Value::as_str) == Some(path)
                    {
                        stage_delete(
                            &mut expected,
                            &mut changes,
                            INGESTED_SOURCES,
                            key,
                            Some(value.clone()),
                            None,
                        );
                    }
                }
            }
        }
        stage_ingest_gates(
            &mut expected,
            &mut changes,
            filter,
            &selected,
            &survivors,
            &states,
        );
        for chunk in &selected {
            let staged =
                crate::memory::store::entity_index::transaction::stage_delete_node_entities_async(
                    config, &chunk.id,
                )
                .await?;
            expected.extend(staged.expected);
            changes.extend(staged.changes);
        }
        stage_empty_source_trees_async(
            config,
            &snapshot,
            &selected,
            &survivors,
            &mut expected,
            &mut changes,
            &mut content_paths,
        )
        .await?;
        if versioned::commit_if_state_matches_at_snapshot_async(
            config,
            &snapshot,
            &expected,
            &[],
            &changes,
        )
        .await?
        {
            remove_chunk_content_files(config, &content_paths);
            return Ok(selected.len());
        }
    }
    anyhow::bail!("chunk selection changed too frequently to delete safely")
}

fn state_rows<'a>(
    states: &'a HashMap<String, Vec<(String, serde_json::Value)>>,
    collection: &str,
) -> &'a [(String, serde_json::Value)] {
    states.get(collection).map(Vec::as_slice).unwrap_or(&[])
}

fn stage_delete(
    expected: &mut Vec<StateChange>,
    changes: &mut Vec<StateChange>,
    collection: &str,
    key: &str,
    prior: Option<serde_json::Value>,
    value: Option<serde_json::Value>,
) {
    expected.push(StateChange {
        collection: collection.to_owned(),
        key: key.to_owned(),
        value: prior,
    });
    changes.push(StateChange {
        collection: collection.to_owned(),
        key: key.to_owned(),
        value,
    });
}

fn raw_paths_for(
    chunks: &[Chunk],
    states: &HashMap<String, Vec<(String, serde_json::Value)>>,
) -> HashSet<String> {
    let ids = chunks
        .iter()
        .map(|chunk| chunk.id.as_str())
        .collect::<HashSet<_>>();
    state_rows(states, c::RAW_REFS)
        .iter()
        .filter(|(key, _)| ids.contains(key.as_str()))
        .filter_map(|(_, value)| serde_json::from_value::<Vec<RawRef>>(value.clone()).ok())
        .flatten()
        .map(|raw_ref| raw_ref.path)
        .collect()
}

fn stage_ingest_gates(
    expected: &mut Vec<StateChange>,
    changes: &mut Vec<StateChange>,
    filter: DeleteFilter<'_>,
    selected: &[Chunk],
    survivors: &[&Chunk],
    states: &HashMap<String, Vec<(String, serde_json::Value)>>,
) {
    let selected_sources = selected
        .iter()
        .map(|chunk| {
            (
                chunk.metadata.source_kind,
                chunk.metadata.source_id.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    let surviving_sources = survivors
        .iter()
        .map(|chunk| {
            (
                chunk.metadata.source_kind,
                chunk.metadata.source_id.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    for (key, value) in state_rows(states, c::INGESTED_SOURCES) {
        let Some(kind) = value.get("source_kind").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(id) = value.get("source_id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let explicit = match filter {
            DeleteFilter::Source(wanted, source, prefix) => {
                kind == wanted.as_str()
                    && if prefix {
                        id.starts_with(source)
                    } else {
                        id == source
                    }
            }
            DeleteFilter::Owner(_, _) | DeleteFilter::Id(_) => false,
        };
        let orphaned = selected_sources
            .iter()
            .any(|(source_kind, source_id)| source_kind.as_str() == kind && *source_id == id)
            && !surviving_sources
                .iter()
                .any(|(source_kind, source_id)| source_kind.as_str() == kind && *source_id == id);
        if explicit || orphaned {
            stage_delete(
                expected,
                changes,
                c::INGESTED_SOURCES,
                key,
                Some(value.clone()),
                None,
            );
        }
    }
}

async fn stage_empty_source_trees_async(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    selected: &[Chunk],
    survivors: &[&Chunk],
    expected: &mut Vec<StateChange>,
    changes: &mut Vec<StateChange>,
    content_paths: &mut Vec<String>,
) -> Result<()> {
    let tree_rows = versioned::list_state_at_snapshot_async(config, snapshot, c::TREES).await?;
    let scopes = selected
        .iter()
        .map(|chunk| {
            chunk
                .metadata
                .path_scope
                .as_deref()
                .unwrap_or(&chunk.metadata.source_id)
        })
        .collect::<HashSet<_>>();
    let survivor_scopes = survivors
        .iter()
        .map(|chunk| {
            chunk
                .metadata
                .path_scope
                .as_deref()
                .unwrap_or(&chunk.metadata.source_id)
        })
        .collect::<HashSet<_>>();
    for (key, value) in tree_rows {
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("source") {
            continue;
        }
        let Some(scope) = value.get("scope").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !scopes.contains(scope) || survivor_scopes.contains(scope) {
            continue;
        }
        let Some(tree_id) = value.get("id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let staged =
            crate::memory::tree::store::stage_tree_cascade_deletion_async(config, tree_id).await?;
        if let Some(staged) = staged {
            if staged.snapshot != *snapshot {
                anyhow::bail!("owner head changed while staging source tree deletion");
            }
            expected.extend(staged.expected);
            changes.extend(staged.changes);
            content_paths.extend(staged.result.content_paths);
            expected.push(StateChange {
                collection: c::TREES.to_owned(),
                key: key.clone(),
                value: Some(value),
            });
        }
    }
    Ok(())
}

/// Delete an empty source gate/tree after a caller removes source data.
pub fn delete_orphaned_source_tree(
    config: &MemoryConfig,
    source_kind: SourceKind,
    source_id: &str,
) -> Result<bool> {
    run_delete_sync(delete_orphaned_source_tree_async(
        config,
        source_kind,
        source_id,
    ))
}

/// Async counterpart to [`delete_orphaned_source_tree`].
pub async fn delete_orphaned_source_tree_async(
    config: &MemoryConfig,
    source_kind: SourceKind,
    source_id: &str,
) -> Result<bool> {
    let (snapshot, chunks, states) =
        versioned::list_chunk_snapshot_with_head_async(config, &[c::INGESTED_SOURCES, c::TREES])
            .await?;
    if chunks.iter().any(|chunk| {
        chunk.metadata.source_kind == source_kind && chunk.metadata.source_id == source_id
    }) {
        return Ok(false);
    }
    let mut expected = Vec::new();
    let mut changes = Vec::new();
    for (key, value) in state_rows(&states, c::INGESTED_SOURCES) {
        if value.get("source_kind").and_then(serde_json::Value::as_str)
            == Some(source_kind.as_str())
            && value
                .get("source_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| id == source_id || id.starts_with(&format!("{source_id}@")))
        {
            stage_delete(
                &mut expected,
                &mut changes,
                c::INGESTED_SOURCES,
                key,
                Some(value.clone()),
                None,
            );
        }
    }
    let trees = state_rows(&states, c::TREES)
        .iter()
        .filter(|(_, value)| {
            value.get("kind").and_then(serde_json::Value::as_str) == Some("source")
                && value.get("scope").and_then(serde_json::Value::as_str) == Some(source_id)
        })
        .collect::<Vec<_>>();
    for (key, value) in trees {
        let tree_id = value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("source tree missing id")?;
        if let Some(staged) =
            crate::memory::tree::store::stage_tree_cascade_deletion_async(config, tree_id).await?
        {
            if staged.snapshot != snapshot {
                return Ok(false);
            }
            expected.extend(staged.expected);
            changes.extend(staged.changes);
            expected.push(StateChange {
                collection: c::TREES.to_owned(),
                key: key.clone(),
                value: Some(value.clone()),
            });
        }
    }
    versioned::commit_if_state_matches_at_snapshot_async(
        config,
        &snapshot,
        &expected,
        &[],
        &changes,
    )
    .await
}

/// Remove every chunk and its coupled tree state. The count is the number of
/// logical rows removed from the same table families as the previous purge;
/// retained immutable history is not included.
pub fn purge_all(config: &MemoryConfig) -> Result<usize> {
    run_delete_sync(purge_all_async(config))
}

/// Async counterpart to [`purge_all`].
pub async fn purge_all_async(config: &MemoryConfig) -> Result<usize> {
    use c::{
        BUFFERS, CHUNK_CONTENT_POINTERS, CHUNK_EMBEDDINGS, CHUNK_LIFECYCLE, CHUNK_REEMBED_SKIPS,
        ENTITY_EDGES, ENTITY_HOTNESS, ENTITY_INDEX, INGESTED_SOURCES, INGEST_OUTBOX, RAW_REFS,
        SCORES, SUMMARIES, SUMMARY_CONTENT_POINTERS, SUMMARY_EMBEDDINGS, TREES,
    };
    const OWNED_COLLECTIONS: &[&str] = &[
        BUFFERS,
        CHUNK_CONTENT_POINTERS,
        CHUNK_EMBEDDINGS,
        CHUNK_REEMBED_SKIPS,
        ENTITY_EDGES,
        ENTITY_HOTNESS,
        ENTITY_INDEX,
        INGESTED_SOURCES,
        INGEST_OUTBOX,
        RAW_REFS,
        SCORES,
        SUMMARIES,
        SUMMARY_CONTENT_POINTERS,
        SUMMARY_EMBEDDINGS,
        TREES,
    ];
    for _ in 0..8 {
        let (snapshot, chunks, states) =
            versioned::list_chunk_snapshot_with_head_async(config, OWNED_COLLECTIONS).await?;
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        let mut content_paths = Vec::new();
        let mut logical_rows = chunks.len();
        for chunk in &chunks {
            stage_delete(
                &mut expected,
                &mut changes,
                CHUNK_LIFECYCLE,
                &chunk.id,
                state_rows(&states, CHUNK_LIFECYCLE)
                    .iter()
                    .find(|(key, _)| key == &chunk.id)
                    .map(|(_, value)| value.clone()),
                Some(serde_json::json!({"status":"deleted"})),
            );
        }
        for collection in OWNED_COLLECTIONS {
            for (key, value) in state_rows(&states, collection) {
                if *collection == CHUNK_CONTENT_POINTERS || *collection == SUMMARY_CONTENT_POINTERS
                {
                    if let Some(path) = value
                        .get("content_path")
                        .and_then(serde_json::Value::as_str)
                    {
                        content_paths.push(path.to_owned());
                    }
                }
                if *collection == RAW_REFS {
                    if let Ok(refs) = serde_json::from_value::<Vec<RawRef>>(value.clone()) {
                        content_paths.extend(refs.into_iter().map(|raw_ref| raw_ref.path));
                    }
                }
                if purge_row_counts_toward_legacy_total(collection, key) {
                    logical_rows += 1;
                }
                stage_delete(
                    &mut expected,
                    &mut changes,
                    collection,
                    key,
                    Some(value.clone()),
                    None,
                );
            }
        }
        let (job_expected, job_changes) =
            crate::memory::queue::store::stage_purge_jobs_at_snapshot(config, &snapshot).await?;
        logical_rows += job_expected
            .iter()
            .filter(|change| {
                change
                    .key
                    .starts_with(crate::memory::queue::store::JOB_PREFIX)
            })
            .count();
        expected.extend(job_expected);
        changes.extend(job_changes);
        if versioned::commit_if_state_matches_at_snapshot_async(
            config,
            &snapshot,
            &expected,
            &[],
            &changes,
        )
        .await?
        {
            remove_chunk_content_files(config, &content_paths);
            return Ok(logical_rows);
        }
    }
    anyhow::bail!("memory owner changed too frequently to purge safely")
}

fn purge_row_counts_toward_legacy_total(collection: &str, key: &str) -> bool {
    match collection {
        c::TREES => key.starts_with("tree:"),
        c::ENTITY_INDEX => key.starts_with("occ:"),
        c::CHUNK_CONTENT_POINTERS | c::RAW_REFS | c::INGEST_OUTBOX => false,
        c::SUMMARY_CONTENT_POINTERS => false,
        _ => true,
    }
}

/// Remove staged content files only when no committed chunk or summary points
/// at them. Corrupt pointer state fails closed and keeps every candidate.
pub(crate) fn remove_unreferenced_content_files(
    config: &MemoryConfig,
    candidates: &[String],
) -> Result<()> {
    run_delete_sync(remove_unreferenced_content_files_async(config, candidates))
}

/// Async counterpart for ingest pipelines running on Tokio.
pub(crate) async fn remove_unreferenced_content_files_async(
    config: &MemoryConfig,
    candidates: &[String],
) -> Result<()> {
    if candidates.is_empty() {
        return Ok(());
    }
    let (snapshot, chunks, states) = versioned::list_chunk_snapshot_with_head_async(
        config,
        &[
            c::RAW_REFS,
            c::CHUNK_CONTENT_POINTERS,
            c::SUMMARY_CONTENT_POINTERS,
        ],
    )
    .await?;
    let wanted = candidates
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut referenced = HashSet::new();
    let mut corrupt = false;
    for (key, value) in state_rows(&states, c::RAW_REFS) {
        if chunks.iter().any(|chunk| chunk.id == *key) {
            match serde_json::from_value::<Vec<RawRef>>(value.clone()) {
                Ok(refs) => referenced.extend(refs.into_iter().map(|raw_ref| raw_ref.path)),
                Err(_) => corrupt = true,
            }
        }
    }
    for collection in [c::CHUNK_CONTENT_POINTERS, c::SUMMARY_CONTENT_POINTERS] {
        for (_, value) in state_rows(&states, collection) {
            if let Some(path) = value
                .get("content_path")
                .and_then(serde_json::Value::as_str)
            {
                referenced.insert(path.to_owned());
            }
        }
    }
    // The extra head check ensures an append or pointer update during the scan
    // cannot make an unreferenced decision stale.
    if versioned::capture_state_snapshot_async(config).await? != snapshot || corrupt {
        return Ok(());
    }
    let safe = wanted
        .into_iter()
        .filter(|path| !referenced.contains(*path))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    remove_chunk_content_files(config, &safe);
    Ok(())
}

/// Best-effort removal of vault paths after the owner head commits.
pub(super) fn remove_chunk_content_files(config: &MemoryConfig, content_paths: &[String]) {
    use std::path::{Component, Path};
    let root = super::content_root(config);
    let canonical_root = match std::fs::canonicalize(&root) {
        Ok(path) => path,
        Err(_) => return,
    };
    for rel in content_paths {
        let path = Path::new(rel);
        if path.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            continue;
        }
        let candidate = root.join(path);
        let canonical = match std::fs::canonicalize(&candidate) {
            Ok(path) => path,
            Err(_) => continue,
        };
        if !canonical.starts_with(&canonical_root) {
            continue;
        }
        let _ = std::fs::remove_file(candidate);
    }
}
