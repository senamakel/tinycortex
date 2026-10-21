//! Typed destination API for importing records read by a host-owned legacy
//! source adapter. TinyCortex accepts domain values and performs one owner-head
//! commit; it does not open or traverse the source database.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::collections as c;
use super::raw_refs::RawRef;
use super::types::{Chunk, SourceKind};
use super::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::queue::{store, NewJob};
use crate::memory::score::store::ScoreRow;
use crate::memory::store::entity_index::transaction::{
    stage_node_occurrences_async, EntityOccurrence,
};
use crate::memory::tree::store::{SummaryNode, Tree};

/// A legacy content pointer already copied and verified by the host importer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyContentPointer {
    /// Safe, forward-slash relative path beneath the host content root.
    pub path: String,
    /// SHA-256 of the complete content file.
    pub sha256: String,
}

/// A per-model embedding decoded from the source store.
#[derive(Clone, Debug, PartialEq)]
pub struct LegacyEmbedding {
    /// Stable model signature used by the source store.
    pub model_signature: String,
    /// Decoded vector values, preserving their original order.
    pub vector: Vec<f32>,
    /// Historical creation time. This remains domain data, not host metadata.
    pub created_at_ms: i64,
}

/// One chunk and its typed adjuncts from an older TinyCortex writer.
#[derive(Clone, Debug)]
pub struct LegacyChunkRecord {
    /// Complete legacy chunk, including its historical creation timestamp.
    pub chunk: Chunk,
    /// Optional full-content pointer after the host has copied its file.
    pub content: Option<LegacyContentPointer>,
    /// Raw-archive references in their historical order.
    pub raw_refs: Vec<RawRef>,
    /// Historical admission rationale, when present.
    pub score: Option<ScoreRow>,
    /// Historical lifecycle state; `None` leaves the destination default.
    pub lifecycle_status: Option<String>,
    /// Embeddings for each stored model signature.
    pub embeddings: Vec<LegacyEmbedding>,
    /// Entity occurrences indexed on this chunk by the old writer.
    pub entities: Vec<LegacyEntityOccurrence>,
}

/// Historical entity occurrence as persisted by the legacy writer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyEntityOccurrence {
    /// Canonical entity identifier.
    pub entity_id: String,
    /// Chunk or summary node that contains this entity.
    pub node_id: String,
    /// Legacy node kind, usually `leaf` or `summary`.
    pub node_kind: String,
    /// Entity kind discriminator.
    pub entity_kind: String,
    /// Original surface form.
    pub surface: String,
    /// Historical occurrence score.
    pub score: f32,
    /// Historical node timestamp in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Owning tree, if known.
    pub tree_id: Option<String>,
    /// Whether the occurrence was marked as the user's identity.
    pub is_user: bool,
}

/// Historical co-occurrence edge and its original accumulated weight.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyEntityEdge {
    /// First canonical entity in the stored, sorted pair.
    pub entity_a: String,
    /// Second canonical entity in the stored, sorted pair.
    pub entity_b: String,
    /// Stored edge weight.
    pub weight: i64,
    /// Historical update time in epoch milliseconds.
    pub updated_at_ms: i64,
}

/// A source-level ingest gate from the legacy store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacySourceGate {
    /// Source discriminator.
    pub source_kind: SourceKind,
    /// Logical source identifier.
    pub source_id: String,
    /// Historical gate creation time in epoch milliseconds.
    pub ingested_at_ms: i64,
}

/// Full-content pointer for a legacy summary body copied by the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacySummaryContent {
    /// Summary identifier whose complete markdown is stored at this path.
    pub summary_id: String,
    /// Safe, forward-slash relative path beneath the host content root.
    pub path: String,
    /// SHA-256 of the complete summary body.
    pub sha256: String,
}

/// Per-model summary embedding decoded from the source store.
#[derive(Clone, Debug, PartialEq)]
pub struct LegacySummaryEmbedding {
    /// Summary row identifier.
    pub summary_id: String,
    /// Stable model signature used by the source store.
    pub model_signature: String,
    /// Decoded vector values in their original order.
    pub vector: Vec<f32>,
    /// Historical creation time in epoch milliseconds.
    pub created_at_ms: i64,
}

/// One import batch read by the host's legacy adapter.
#[derive(Clone, Debug, Default)]
pub struct LegacyImportBatch {
    /// Chunk records and their directly owned adjuncts.
    pub chunks: Vec<LegacyChunkRecord>,
    /// Tree records, including their historical ids and timestamps.
    pub trees: Vec<Tree>,
    /// Sealed summaries and their original content.
    pub summaries: Vec<SummaryNode>,
    /// Full-content pointers for summaries whose body was externalized.
    pub summary_content: Vec<LegacySummaryContent>,
    /// Per-model summary embedding rows.
    pub summary_embeddings: Vec<LegacySummaryEmbedding>,
    /// Entity occurrences not already attached to a chunk record.
    pub entities: Vec<LegacyEntityOccurrence>,
    /// Existing aggregate edge weights; import does not increment them.
    pub edges: Vec<LegacyEntityEdge>,
    /// Source-ingest gates.
    pub source_gates: Vec<LegacySourceGate>,
    /// Durable extract jobs with stable IDs supplied by the source adapter.
    pub jobs: Vec<NewJob>,
}

/// Counts imported logical records. Existing identical records count as
/// already imported, making a retry safe after the host loses the response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LegacyImportCounts {
    /// Chunk rows present after import.
    pub chunks: usize,
    /// Score rows present after import.
    pub scores: usize,
    /// Entity occurrence rows present after import.
    pub entities: usize,
    /// Tree and summary rows present after import.
    pub trees: usize,
    /// Ready job hints published or repaired after commit.
    pub jobs: usize,
}

#[derive(Deserialize)]
struct ExistingContentPointer {
    chunk_id: String,
    content_path: String,
    content_sha256: String,
}

/// Import a typed legacy batch under one captured owner head.
///
/// Source reads and filesystem copying belong to the host. Every destination
/// row, pointer, entity manifest, and ready queue job is committed together;
/// queue hints are published only after that CAS succeeds.
pub async fn import_legacy_batch(
    config: &MemoryConfig,
    batch: &LegacyImportBatch,
) -> Result<LegacyImportCounts> {
    validate_batch(batch)?;
    for _ in 0..8 {
        let (snapshot, current_chunks, states) = versioned::list_chunk_snapshot_with_head_async(
            config,
            &[
                c::CHUNK_CONTENT_POINTERS,
                c::RAW_REFS,
                c::SCORES,
                c::CHUNK_LIFECYCLE,
                c::CHUNK_EMBEDDINGS,
                c::INGESTED_SOURCES,
                c::TREES,
                c::SUMMARIES,
                c::SUMMARY_CONTENT_POINTERS,
                c::SUMMARY_EMBEDDINGS,
                c::ENTITY_INDEX,
                c::ENTITY_EDGES,
            ],
        )
        .await?;
        let current_chunks: HashMap<_, _> = current_chunks
            .into_iter()
            .map(|chunk| (chunk.id.clone(), chunk))
            .collect();
        let state_map = states
            .into_iter()
            .map(|(collection, rows)| (collection, rows.into_iter().collect::<HashMap<_, _>>()))
            .collect::<HashMap<_, _>>();
        let mut chunks_to_write = Vec::new();
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        let mut desired = BTreeMap::<(String, String), Option<serde_json::Value>>::new();

        for record in &batch.chunks {
            match current_chunks.get(&record.chunk.id) {
                Some(current) if current == &record.chunk => {}
                Some(_) => anyhow::bail!(
                    "legacy chunk ID {} already exists with different content",
                    record.chunk.id
                ),
                None => chunks_to_write.push(record.chunk.clone()),
            }
            if let Some(content) = &record.content {
                add_desired(
                    &mut desired,
                    c::CHUNK_CONTENT_POINTERS,
                    &record.chunk.id,
                    Some(serde_json::json!({
                        "chunk_id": record.chunk.id,
                        "content_path": content.path,
                        "content_sha256": content.sha256,
                    })),
                )?;
            }
            if !record.raw_refs.is_empty() {
                add_desired(
                    &mut desired,
                    c::RAW_REFS,
                    &record.chunk.id,
                    Some(serde_json::to_value(&record.raw_refs)?),
                )?;
            }
            if let Some(score) = &record.score {
                add_desired(
                    &mut desired,
                    c::SCORES,
                    &record.chunk.id,
                    Some(serde_json::to_value(score)?),
                )?;
            }
            if let Some(status) = &record.lifecycle_status {
                add_desired(
                    &mut desired,
                    c::CHUNK_LIFECYCLE,
                    &record.chunk.id,
                    Some(serde_json::json!({"status": status})),
                )?;
            } else if !current_chunks.contains_key(&record.chunk.id) {
                add_desired(
                    &mut desired,
                    c::CHUNK_LIFECYCLE,
                    &record.chunk.id,
                    Some(serde_json::json!({
                        "status": super::store::CHUNK_STATUS_ADMITTED
                    })),
                )?;
            }
            for embedding in &record.embeddings {
                let key = super::embeddings::chunk_embedding_key(
                    &record.chunk.id,
                    &embedding.model_signature,
                );
                add_desired(
                    &mut desired,
                    c::CHUNK_EMBEDDINGS,
                    &key,
                    Some(serde_json::json!({
                        "chunk_id": record.chunk.id,
                        "model_signature": embedding.model_signature,
                        "vector": super::embeddings::embedding_to_blob(&embedding.vector),
                        "dim": embedding.vector.len(),
                        "created_at_ms": embedding.created_at_ms,
                    })),
                )?;
            }
        }

        for gate in &batch.source_gates {
            let key = format!("{}\0{}", gate.source_kind.as_str(), gate.source_id);
            add_desired(
                &mut desired,
                c::INGESTED_SOURCES,
                &key,
                Some(serde_json::json!({
                    "source_kind": gate.source_kind.as_str(),
                    "source_id": gate.source_id,
                    "ingested_at_ms": gate.ingested_at_ms,
                })),
            )?;
        }
        for tree in &batch.trees {
            add_desired(
                &mut desired,
                c::TREES,
                &format!("tree:{}", tree.id),
                Some(serde_json::to_value(tree)?),
            )?;
            add_desired(
                &mut desired,
                c::TREES,
                &format!("scope:{}:{}", tree.kind.as_str(), tree.scope),
                Some(serde_json::Value::String(tree.id.clone())),
            )?;
        }
        for summary in &batch.summaries {
            add_desired(
                &mut desired,
                c::SUMMARIES,
                &format!("summary:{}", summary.id),
                Some(serde_json::to_value(summary)?),
            )?;
        }
        for content in &batch.summary_content {
            add_desired(
                &mut desired,
                c::SUMMARY_CONTENT_POINTERS,
                &format!("summary:{}", content.summary_id),
                Some(serde_json::json!({
                    "summary_id": content.summary_id,
                    "content_path": content.path,
                    "content_sha256": content.sha256,
                })),
            )?;
        }
        for embedding in &batch.summary_embeddings {
            let key = super::embeddings::summary_embedding_key(
                &embedding.summary_id,
                &embedding.model_signature,
            );
            add_desired(
                &mut desired,
                c::SUMMARY_EMBEDDINGS,
                &key,
                Some(serde_json::json!({
                    "summary_id": embedding.summary_id,
                    "model_signature": embedding.model_signature,
                    "vector": super::embeddings::embedding_to_blob(&embedding.vector),
                    "dim": embedding.vector.len(),
                    "created_at_ms": embedding.created_at_ms,
                })),
            )?;
        }
        for edge in &batch.edges {
            let (a, b) = if edge.entity_a < edge.entity_b {
                (&edge.entity_a, &edge.entity_b)
            } else {
                (&edge.entity_b, &edge.entity_a)
            };
            anyhow::ensure!(a != b && edge.weight >= 0, "invalid imported entity edge");
            add_desired(
                &mut desired,
                c::ENTITY_EDGES,
                &edge_key(a, b),
                Some(serde_json::json!({
                    "entity_a": a,
                    "entity_b": b,
                    "weight": edge.weight,
                    "updated_ms": edge.updated_at_ms,
                })),
            )?;
        }

        let mut staged_entities = BTreeMap::<String, Vec<EntityOccurrence>>::new();
        for occurrence in batch
            .chunks
            .iter()
            .flat_map(|record| record.entities.iter())
            .chain(batch.entities.iter())
        {
            staged_entities
                .entry(occurrence.node_id.clone())
                .or_default()
                .push(EntityOccurrence {
                    entity_id: occurrence.entity_id.clone(),
                    node_id: occurrence.node_id.clone(),
                    node_kind: occurrence.node_kind.clone(),
                    entity_kind: occurrence.entity_kind.clone(),
                    surface: occurrence.surface.clone(),
                    score: occurrence.score,
                    timestamp_ms: occurrence.timestamp_ms,
                    tree_id: occurrence.tree_id.clone(),
                    is_user: occurrence.is_user,
                });
        }
        for (node_id, occurrences) in staged_entities {
            let staged = stage_node_occurrences_async(config, &node_id, occurrences).await?;
            for change in &staged.changes {
                let current = state_map
                    .get(&change.collection)
                    .and_then(|rows| rows.get(&change.key));
                if current.is_some() && current != change.value.as_ref() {
                    anyhow::bail!(
                        "legacy import conflicts with existing {} row {}",
                        change.collection,
                        change.key
                    );
                }
            }
            merge_changes(&mut expected, &mut changes, staged.expected, staged.changes)?;
        }

        changes.retain(|change| {
            state_map
                .get(&change.collection)
                .and_then(|rows| rows.get(&change.key))
                != change.value.as_ref()
        });

        for ((collection, key), value) in desired {
            let current = state_map
                .get(&collection)
                .and_then(|rows| rows.get(&key))
                .cloned();
            expected.push(StateChange {
                collection: collection.clone(),
                key: key.clone(),
                value: current.clone(),
            });
            if current != value {
                if current.is_some() {
                    anyhow::bail!("legacy import conflicts with existing {collection} row {key}");
                }
                changes.push(StateChange {
                    collection,
                    key,
                    value,
                });
            }
        }

        // Existing content pointers are decoded too, so malformed rows cannot
        // be accepted as successful idempotent retries.
        for record in &batch.chunks {
            if let Some(value) = state_map
                .get(c::CHUNK_CONTENT_POINTERS)
                .and_then(|rows| rows.get(&record.chunk.id))
            {
                let pointer: ExistingContentPointer = serde_json::from_value(value.clone())
                    .context("decode existing imported chunk content pointer")?;
                anyhow::ensure!(pointer.chunk_id == record.chunk.id);
            }
        }

        let staged_jobs =
            store::stage_import_jobs_at_snapshot(config, &snapshot, &batch.jobs).await?;
        expected.extend(staged_jobs.expected);
        changes.extend(staged_jobs.changes);
        let counts = LegacyImportCounts {
            chunks: batch.chunks.len(),
            scores: batch
                .chunks
                .iter()
                .filter(|record| record.score.is_some())
                .count(),
            entities: batch.entities.len()
                + batch
                    .chunks
                    .iter()
                    .map(|record| record.entities.len())
                    .sum::<usize>(),
            trees: batch.trees.len() + batch.summaries.len(),
            jobs: staged_jobs.publish.len(),
        };

        let committed = if chunks_to_write.is_empty() && changes.is_empty() {
            versioned::capture_state_snapshot_async(config).await? == snapshot
        } else {
            versioned::commit_if_state_matches_at_snapshot_async(
                config,
                &snapshot,
                &expected,
                &chunks_to_write,
                &changes,
            )
            .await?
        };
        if !committed {
            continue;
        }
        store::publish_import_job_hints_async(config, &staged_jobs.publish).await?;
        return Ok(counts);
    }
    anyhow::bail!("legacy import owner head changed too frequently")
}

fn validate_batch(batch: &LegacyImportBatch) -> Result<()> {
    let mut chunk_ids = HashSet::new();
    for record in &batch.chunks {
        anyhow::ensure!(
            !record.chunk.id.is_empty() && chunk_ids.insert(record.chunk.id.as_str()),
            "legacy import contains an empty or duplicate chunk ID"
        );
        if let Some(score) = &record.score {
            anyhow::ensure!(
                score.chunk_id == record.chunk.id,
                "legacy score/chunk ID mismatch"
            );
        }
        for occurrence in &record.entities {
            anyhow::ensure!(
                occurrence.node_id == record.chunk.id,
                "chunk {} has an entity occurrence for another node",
                record.chunk.id
            );
        }
        if let Some(content) = &record.content {
            anyhow::ensure!(
                safe_relative_path(&content.path) && is_sha256(&content.sha256),
                "invalid legacy content pointer for chunk {}",
                record.chunk.id
            );
        }
        if let Some(embedding) = &record.embedding {
            anyhow::ensure!(
                !embedding.model_signature.trim().is_empty()
                    && !embedding.vector.is_empty()
                    && embedding.vector.iter().all(|value| value.is_finite()),
                "invalid legacy embedding for chunk {}",
                record.chunk.id
            );
        }
        for reference in &record.raw_refs {
            anyhow::ensure!(
                safe_relative_path(&reference.path),
                "invalid legacy raw-ref path"
            );
            if let Some(end) = reference.end {
                anyhow::ensure!(end >= reference.start, "legacy raw-ref range is reversed");
            }
        }
    }
    let tree_ids: HashSet<_> = batch.trees.iter().map(|tree| tree.id.as_str()).collect();
    anyhow::ensure!(
        tree_ids.len() == batch.trees.len(),
        "duplicate legacy tree ID"
    );
    let summary_ids: HashSet<_> = batch
        .summaries
        .iter()
        .map(|node| node.id.as_str())
        .collect();
    anyhow::ensure!(
        summary_ids.len() == batch.summaries.len(),
        "duplicate legacy summary ID"
    );
    for summary in &batch.summaries {
        anyhow::ensure!(
            tree_ids.contains(summary.tree_id.as_str()),
            "legacy summary {} has no imported owning tree",
            summary.id
        );
    }
    for content in &batch.summary_content {
        anyhow::ensure!(
            summary_ids.contains(content.summary_id.as_str())
                && safe_relative_path(&content.path)
                && is_sha256(&content.sha256),
            "invalid legacy summary content pointer for {}",
            content.summary_id
        );
    }
    for embedding in &batch.summary_embeddings {
        anyhow::ensure!(
            summary_ids.contains(embedding.summary_id.as_str())
                && !embedding.model_signature.trim().is_empty()
                && !embedding.vector.is_empty()
                && embedding.vector.iter().all(|value| value.is_finite()),
            "invalid legacy summary embedding for {}",
            embedding.summary_id
        );
    }
    let valid_nodes: HashSet<_> = chunk_ids
        .iter()
        .copied()
        .chain(summary_ids.iter().copied())
        .collect();
    for occurrence in batch
        .chunks
        .iter()
        .flat_map(|record| record.entities.iter())
        .chain(batch.entities.iter())
    {
        anyhow::ensure!(
            valid_nodes.contains(occurrence.node_id.as_str()),
            "legacy entity occurrence references a node outside the import batch"
        );
        anyhow::ensure!(
            !occurrence.entity_id.is_empty()
                && !occurrence.node_kind.is_empty()
                && !occurrence.entity_kind.is_empty()
                && occurrence.score.is_finite(),
            "invalid legacy entity occurrence"
        );
        if let Some(tree_id) = &occurrence.tree_id {
            anyhow::ensure!(
                tree_ids.contains(tree_id.as_str()),
                "entity references unknown tree"
            );
        }
    }
    Ok(())
}

fn add_desired(
    desired: &mut BTreeMap<(String, String), Option<serde_json::Value>>,
    collection: &str,
    key: &str,
    value: Option<serde_json::Value>,
) -> Result<()> {
    let identity = (collection.to_owned(), key.to_owned());
    if let Some(previous) = desired.insert(identity, value.clone()) {
        anyhow::ensure!(
            previous == value,
            "conflicting duplicate imported owner row"
        );
    }
    Ok(())
}

fn merge_changes(
    expected: &mut Vec<StateChange>,
    changes: &mut Vec<StateChange>,
    staged_expected: Vec<StateChange>,
    staged_changes: Vec<StateChange>,
) -> Result<()> {
    for condition in staged_expected {
        if let Some(previous) = expected
            .iter()
            .find(|item| item.collection == condition.collection && item.key == condition.key)
        {
            anyhow::ensure!(
                previous.value == condition.value,
                "conflicting import preconditions"
            );
        } else {
            expected.push(condition);
        }
    }
    for change in staged_changes {
        if let Some(previous) = changes
            .iter()
            .find(|item| item.collection == change.collection && item.key == change.key)
        {
            anyhow::ensure!(
                previous.value == change.value,
                "conflicting import mutations"
            );
        } else {
            changes.push(change);
        }
    }
    Ok(())
}

fn safe_relative_path(path: &str) -> bool {
    let parsed = std::path::Path::new(path);
    !path.is_empty()
        && !parsed.is_absolute()
        && !path.contains('\\')
        && !path.contains(':')
        && parsed
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn edge_key(a: &str, b: &str) -> String {
    let encode = |value: &str| {
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    format!("edge:{}:{}", encode(a), encode(b))
}

#[cfg(test)]
#[path = "legacy_import_tests.rs"]
mod tests;
