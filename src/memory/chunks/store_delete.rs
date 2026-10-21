//! Atomic chunk deletion and its dependent owner-state cleanup.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};

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
    delete_filtered(config, DeleteFilter::Source(kind, source_id, false))
}

/// Delete chunks whose source id has the exact literal prefix.
pub fn delete_chunks_by_source_prefix(
    config: &MemoryConfig,
    kind: SourceKind,
    prefix: &str,
) -> Result<usize> {
    delete_filtered(config, DeleteFilter::Source(kind, prefix, true))
}

/// Delete chunks owned by `owner`, preserving sources that still have another
/// owner's chunks.
pub fn delete_chunks_by_owner(
    config: &MemoryConfig,
    kind: SourceKind,
    owner: &str,
) -> Result<usize> {
    delete_filtered(config, DeleteFilter::Owner(kind, owner))
}

/// Delete one chunk and its dependent indexes. Unknown ids are a no-op.
pub fn delete_chunk_by_id(config: &MemoryConfig, chunk_id: &str) -> Result<usize> {
    delete_filtered(config, DeleteFilter::Id(chunk_id))
}

#[derive(Clone, Copy)]
enum DeleteFilter<'a> {
    Source(SourceKind, &'a str, bool),
    Owner(SourceKind, &'a str),
    Id(&'a str),
    All,
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
        DeleteFilter::All => true,
    }
}

fn delete_filtered(config: &MemoryConfig, filter: DeleteFilter<'_>) -> Result<usize> {
    use c::{
        CHUNK_CONTENT_POINTERS, CHUNK_EMBEDDINGS, CHUNK_LIFECYCLE, CHUNK_REEMBED_SKIPS,
        INGESTED_SOURCES, RAW_REFS, SCORES,
    };
    for _ in 0..8 {
        let (snapshot, chunks, states) = versioned::list_chunk_snapshot_with_head(
            config,
            &[
                CHUNK_CONTENT_POINTERS,
                CHUNK_EMBEDDINGS,
                CHUNK_REEMBED_SKIPS,
                INGESTED_SOURCES,
                RAW_REFS,
                SCORES,
            ],
        )?;
        let selected = chunks
            .iter()
            .filter(|chunk| matches(filter, chunk))
            .cloned()
            .collect::<Vec<_>>();
        if selected.is_empty() {
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
                crate::memory::store::entity_index::transaction::stage_delete_node_entities(
                    config, &chunk.id,
                )?;
            expected.extend(staged.expected);
            changes.extend(staged.changes);
        }
        stage_empty_source_trees(
            config,
            &snapshot,
            &selected,
            &survivors,
            &mut expected,
            &mut changes,
            &mut content_paths,
        )?;
        if versioned::commit_if_state_matches_at_snapshot(config, &snapshot, &expected, &changes)? {
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
            DeleteFilter::All => true,
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

fn stage_empty_source_trees(
    config: &MemoryConfig,
    snapshot: &HeadSnapshot,
    selected: &[Chunk],
    survivors: &[&Chunk],
    expected: &mut Vec<StateChange>,
    changes: &mut Vec<StateChange>,
    content_paths: &mut Vec<String>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let tree_rows = runtime.block_on(versioned::list_state_at_snapshot_async(
        config,
        snapshot,
        c::TREES,
    ))?;
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build tree deletion staging runtime")?;
        let staged = runtime.block_on(
            crate::memory::tree::store::stage_tree_cascade_deletion_async(config, tree_id),
        )?;
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
    let (snapshot, chunks, states) =
        versioned::list_chunk_snapshot_with_head(config, &[c::INGESTED_SOURCES, c::TREES])?;
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        if let Some(staged) = runtime.block_on(
            crate::memory::tree::store::stage_tree_cascade_deletion_async(config, tree_id),
        )? {
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
    versioned::commit_if_state_matches_at_snapshot(config, &snapshot, &expected, &changes)
}

/// Remove every chunk and its coupled tree state. Returns the number of
/// records removed from this owner, matching the old cross-table count only
/// approximately because immutable versions are retained for snapshot safety.
pub fn purge_all(config: &MemoryConfig) -> Result<usize> {
    delete_filtered(config, DeleteFilter::All)
}

/// Remove staged content files only when no committed chunk or summary points
/// at them. Corrupt pointer state fails closed and keeps every candidate.
pub(crate) fn remove_unreferenced_content_files(
    config: &MemoryConfig,
    candidates: &[String],
) -> Result<()> {
    if candidates.is_empty() {
        return Ok(());
    }
    let (snapshot, chunks, states) = versioned::list_chunk_snapshot_with_head(
        config,
        &[
            c::RAW_REFS,
            c::CHUNK_CONTENT_POINTERS,
            c::SUMMARY_CONTENT_POINTERS,
        ],
    )?;
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
    if versioned::capture_head(config)? != snapshot || corrupt {
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
