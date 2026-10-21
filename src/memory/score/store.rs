//! Persistence for score rationale and entity occurrence state.
//!
//! Score rows, node-scoped occurrence sets, and co-occurrence edges are stored
//! in TinyCortex's host-owned versioned collections. `stage_*` helpers return
//! owner-head preconditions and mutations so ingest and tree sealing can
//! commit them atomically with their chunk, tree, and queue state.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::memory::chunks::collections::{CHUNK_LIFECYCLE, ENTITY_INDEX, SCORES};
use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::queue::{JobMutation, JobStateCollection};
use crate::memory::score::extract::EntityKind;
use crate::memory::score::resolver::CanonicalEntity;
use crate::memory::score::signals::ScoreSignals;
use crate::memory::store::entity_index::transaction::{
    stage_delete_tree_occurrences_async, stage_node_entities, stage_node_entities_async,
    stage_node_occurrences, stage_upsert_node_entities, EntityOccurrence, StagedEntityChanges,
};

pub use super::store_query::{
    count_entity_index, count_scores, list_entity_ids_for_node, list_entity_ids_for_node_async,
    lookup_entity, lookup_entity_async, lookup_entity_in_window, lookup_entity_in_window_async,
};

/// Serialized per-chunk score rationale.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoreRow {
    /// Chunk this rationale belongs to.
    pub chunk_id: String,
    /// Aggregate admission score recomputed from `signals` at admission time.
    pub total: f32,
    /// Per-signal breakdown that fed `total`.
    pub signals: ScoreSignals,
    /// Whether the chunk was dropped rather than kept.
    pub dropped: bool,
    /// Human-readable rationale for the decision, when recorded.
    pub reason: Option<String>,
    /// Time the score was computed, in epoch milliseconds.
    pub computed_at_ms: i64,
    /// Diagnostic only; not stored by the current schema.
    #[serde(default)]
    pub llm_importance_reason: Option<String>,
}

/// Result row from [`lookup_entity`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityHit {
    /// Canonical entity id (`"<kind>:<value>"`).
    pub entity_id: String,
    /// Tree node (chunk or summary) containing the entity.
    pub node_id: String,
    /// `"leaf"` or `"summary"`.
    pub node_kind: String,
    /// Parsed entity kind.
    pub entity_kind: EntityKind,
    /// Observed surface form, or the canonical id for summary rows.
    pub surface: String,
    /// Occurrence score carried from the entity or node.
    pub score: f32,
    /// Node timestamp, in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Owning tree, when known.
    pub tree_id: Option<String>,
    /// Whether the host identity resolver marked this as the user's entity.
    #[serde(default)]
    pub is_user: bool,
}

/// Conditional score-row update for inclusion in a larger owner-head commit.
pub(crate) fn stage_score_row(
    config: &MemoryConfig,
    row: &ScoreRow,
) -> Result<StagedEntityChanges> {
    let current = versioned::read_state(config, SCORES, &row.chunk_id)?;
    Ok(StagedEntityChanges {
        expected: vec![StateChange {
            collection: SCORES.to_owned(),
            key: row.chunk_id.clone(),
            value: current,
        }],
        changes: vec![StateChange {
            collection: SCORES.to_owned(),
            key: row.chunk_id.clone(),
            value: Some(serde_json::to_value(row)?),
        }],
    })
}

pub(crate) async fn stage_score_row_async(
    config: &MemoryConfig,
    row: &ScoreRow,
) -> Result<StagedEntityChanges> {
    let current = versioned::read_state_async(config, SCORES, &row.chunk_id).await?;
    Ok(StagedEntityChanges {
        expected: vec![StateChange {
            collection: SCORES.to_owned(),
            key: row.chunk_id.clone(),
            value: current,
        }],
        changes: vec![StateChange {
            collection: SCORES.to_owned(),
            key: row.chunk_id.clone(),
            value: Some(serde_json::to_value(row)?),
        }],
    })
}

/// Stage a score result and its replacement entity occurrence set.
pub(crate) fn stage_score_and_entities(
    config: &MemoryConfig,
    row: &ScoreRow,
    entities: &[CanonicalEntity],
    kept: bool,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<StagedEntityChanges> {
    let mut staged = stage_score_row(config, row)?;
    let occurrences = if kept {
        entities
            .iter()
            .map(|entity| EntityOccurrence {
                entity_id: entity.canonical_id.clone(),
                node_id: row.chunk_id.clone(),
                node_kind: "leaf".to_owned(),
                entity_kind: entity.kind.as_str().to_owned(),
                surface: entity.surface.clone(),
                score: entity.score,
                timestamp_ms,
                tree_id: tree_id.map(str::to_owned),
                is_user: false,
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let entity_changes = stage_node_entities(config, &row.chunk_id, occurrences, timestamp_ms)?;
    staged.expected.extend(entity_changes.expected);
    staged.changes.extend(entity_changes.changes);
    Ok(staged)
}

pub(crate) async fn stage_score_and_entities_async(
    config: &MemoryConfig,
    row: &ScoreRow,
    entities: &[CanonicalEntity],
    kept: bool,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<StagedEntityChanges> {
    let mut staged = stage_score_row_async(config, row).await?;
    let occurrences = if kept {
        entities
            .iter()
            .map(|entity| EntityOccurrence {
                entity_id: entity.canonical_id.clone(),
                node_id: row.chunk_id.clone(),
                node_kind: "leaf".to_owned(),
                entity_kind: entity.kind.as_str().to_owned(),
                surface: entity.surface.clone(),
                score: entity.score,
                timestamp_ms,
                tree_id: tree_id.map(str::to_owned),
                is_user: false,
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let entity_changes =
        stage_node_entities_async(config, &row.chunk_id, occurrences, timestamp_ms).await?;
    staged.expected.extend(entity_changes.expected);
    staged.changes.extend(entity_changes.changes);
    Ok(staged)
}

/// Stage a score result and its replacement entity occurrences for one queue
/// settlement. The returned mutations are not persisted; the queue worker
/// commits them with the job receipt and any follow-up intents under one owner
/// head. A dropped result still returns tombstones for prior occurrences.
pub async fn plan_score_persistence_for_queue(
    config: &MemoryConfig,
    result: &super::ScoreResult,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<Vec<JobMutation>> {
    let chunks = versioned::get_chunks_batch_async(
        config,
        std::slice::from_ref(&result.chunk_id),
    )
    .await?;
    anyhow::ensure!(
        chunks.contains_key(&result.chunk_id),
        "cannot plan score persistence for missing chunk {}",
        result.chunk_id
    );
    let lifecycle = versioned::read_state_async(config, CHUNK_LIFECYCLE, &result.chunk_id).await?;
    anyhow::ensure!(
        lifecycle
            .as_ref()
            .and_then(|value| value.get("status"))
            .and_then(serde_json::Value::as_str)
            != Some("deleted"),
        "cannot plan score persistence for deleted chunk {}",
        result.chunk_id
    );
    let row = super::score_row(result);
    let staged = stage_score_and_entities_async(
        config,
        &row,
        &result.canonical_entities,
        result.kept,
        timestamp_ms,
        tree_id,
    )
    .await?;
    anyhow::ensure!(
        staged.expected.len() == staged.changes.len(),
        "score planner produced mismatched preconditions and changes"
    );
    let mut mutations = staged
        .expected
        .into_iter()
        .zip(staged.changes)
        .map(|(expected, change)| {
            anyhow::ensure!(
                expected.collection == change.collection && expected.key == change.key,
                "score planner produced mismatched mutation identity"
            );
            let collection = match change.collection.as_str() {
                SCORES => JobStateCollection::ChunkScores,
                ENTITY_INDEX => JobStateCollection::EntityIndex,
                other => anyhow::bail!("unsupported score mutation collection {other}"),
            };
            Ok(JobMutation {
                collection,
                key: change.key,
                expected: expected.value,
                value: change.value,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Persisting the unchanged lifecycle value is a same-head existence guard:
    // a concurrent deletion cannot let a stale extraction publish score or
    // entity state after the chunk has been tombstoned.
    mutations.push(JobMutation {
        collection: JobStateCollection::ChunkLifecycle,
        key: result.chunk_id.clone(),
        expected: lifecycle.clone(),
        value: lifecycle,
    });
    Ok(mutations)
}

/// Stage summary entities for an owner commit. Summary surfaces are stored as
/// their canonical ids, matching the historical summary index representation.
pub(crate) fn stage_summary_entity_ids(
    config: &MemoryConfig,
    entity_ids: &[String],
    node_id: &str,
    score: f32,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<StagedEntityChanges> {
    let occurrences = entity_ids
        .iter()
        .map(|canonical_id| EntityOccurrence {
            entity_id: canonical_id.clone(),
            node_id: node_id.to_owned(),
            node_kind: "summary".to_owned(),
            entity_kind: canonical_id
                .split_once(':')
                .map_or(canonical_id.as_str(), |(kind, _)| kind)
                .to_owned(),
            surface: canonical_id.clone(),
            score,
            timestamp_ms,
            tree_id: tree_id.map(str::to_owned),
            is_user: false,
        })
        .collect::<Vec<_>>();
    stage_node_entities(config, node_id, occurrences, timestamp_ms)
}

pub(crate) async fn stage_summary_entity_ids_async(
    config: &MemoryConfig,
    entity_ids: &[String],
    node_id: &str,
    score: f32,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<StagedEntityChanges> {
    let occurrences = entity_ids
        .iter()
        .map(|canonical_id| EntityOccurrence {
            entity_id: canonical_id.clone(),
            node_id: node_id.to_owned(),
            node_kind: "summary".to_owned(),
            entity_kind: canonical_id
                .split_once(':')
                .map_or(canonical_id.as_str(), |(kind, _)| kind)
                .to_owned(),
            surface: canonical_id.clone(),
            score,
            timestamp_ms,
            tree_id: tree_id.map(str::to_owned),
            is_user: false,
        })
        .collect::<Vec<_>>();
    stage_node_entities_async(config, node_id, occurrences, timestamp_ms).await
}

/// Stage deletion of entity occurrences owned by a tree for an atomic cascade.
/// Historical aggregate edge weights remain untouched, matching tree deletion
/// behavior before the owner-store migration.
pub(crate) async fn stage_delete_tree_entities_async(
    config: &MemoryConfig,
    tree_id: &str,
) -> Result<StagedEntityChanges> {
    stage_delete_tree_occurrences_async(config, tree_id).await
}

/// Upsert one score row. The conditional update is retried if another writer
/// commits the same score key first.
pub fn upsert_score(config: &MemoryConfig, row: &ScoreRow) -> Result<()> {
    for _ in 0..8 {
        let staged = stage_score_row(config, row)?;
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(());
        }
    }
    anyhow::bail!(
        "score row update conflicted repeatedly for chunk {}",
        row.chunk_id
    )
}

/// Persist the score row and its entity index as one versioned owner commit.
pub fn persist_score(
    config: &MemoryConfig,
    result: &super::ScoreResult,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<()> {
    let row = super::score_row(result);
    for _ in 0..8 {
        let staged = stage_score_and_entities(
            config,
            &row,
            &result.canonical_entities,
            result.kept,
            timestamp_ms,
            tree_id,
        )?;
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(());
        }
    }
    anyhow::bail!(
        "score persistence conflicted repeatedly for chunk {}",
        row.chunk_id
    )
}

/// Fetch one score row.
pub fn get_score(config: &MemoryConfig, chunk_id: &str) -> Result<Option<ScoreRow>> {
    versioned::read_state(config, SCORES, chunk_id)?
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

/// Async score lookup for callers running on current-thread runtimes.
pub async fn get_score_async(config: &MemoryConfig, chunk_id: &str) -> Result<Option<ScoreRow>> {
    versioned::read_state_async(config, SCORES, chunk_id)
        .await?
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

/// Narrow batched read of score totals. Missing ids are absent from the map.
pub fn get_scores_batch(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, f32>> {
    let rows = versioned::read_states(config, SCORES, chunk_ids)?;
    rows.into_iter()
        .map(|(id, value)| {
            let row: ScoreRow = serde_json::from_value(value)?;
            Ok((id, row.total))
        })
        .collect()
}

/// Async batched score read for callers running on current-thread runtimes.
pub async fn get_scores_batch_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashMap<String, f32>> {
    let rows = versioned::read_states_async(config, SCORES, chunk_ids).await?;
    rows.into_iter()
        .map(|(id, value)| {
            let row: ScoreRow = serde_json::from_value(value)?;
            Ok((id, row.total))
        })
        .collect()
}

/// Return ids whose latest persisted score marked them dropped.
pub fn get_dropped_chunk_ids_batch(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashSet<String>> {
    let rows = versioned::read_states(config, SCORES, chunk_ids)?;
    rows.into_iter()
        .map(|(id, value)| {
            let row: ScoreRow = serde_json::from_value(value)?;
            Ok((id, row.dropped))
        })
        .filter_map(|row| match row {
            Ok((id, true)) => Some(Ok(id)),
            Ok((_, false)) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub async fn get_dropped_chunk_ids_batch_async(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<HashSet<String>> {
    let rows = versioned::read_states_async(config, SCORES, chunk_ids).await?;
    rows.into_iter()
        .map(|(id, value)| {
            let row: ScoreRow = serde_json::from_value(value)?;
            Ok((id, row.dropped))
        })
        .filter_map(|row| match row {
            Ok((id, true)) => Some(Ok(id)),
            Ok((_, false)) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

/// Persist one entity occurrence and its co-occurrence edges.
pub fn index_entity(
    config: &MemoryConfig,
    entity: &CanonicalEntity,
    node_id: &str,
    node_kind: &str,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<()> {
    index_entities(
        config,
        std::slice::from_ref(entity),
        node_id,
        node_kind,
        timestamp_ms,
        tree_id,
    )
    .map(|_| ())
}

/// Replace a node's entity occurrences and increment edges in the same owner
/// commit. Re-indexing an entity pair updates its occurrence but edge weight
/// preserves the historical per-index-call accumulation.
pub fn index_entities(
    config: &MemoryConfig,
    entities: &[CanonicalEntity],
    node_id: &str,
    node_kind: &str,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<usize> {
    let occurrences = entities
        .iter()
        .map(|entity| {
            occurrence_from_entity(entity, node_id, node_kind, timestamp_ms, tree_id, false)
        })
        .collect::<Vec<_>>();
    for _ in 0..8 {
        let staged =
            stage_upsert_node_entities(config, node_id, occurrences.clone(), timestamp_ms)?;
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(entities.len());
        }
    }
    anyhow::bail!("entity indexing conflicted repeatedly for node {node_id}")
}

fn occurrence_from_entity(
    entity: &CanonicalEntity,
    node_id: &str,
    node_kind: &str,
    timestamp_ms: i64,
    tree_id: Option<&str>,
    is_user: bool,
) -> EntityOccurrence {
    EntityOccurrence {
        entity_id: entity.canonical_id.clone(),
        node_id: node_id.to_owned(),
        node_kind: node_kind.to_owned(),
        entity_kind: entity.kind.as_str().to_owned(),
        surface: entity.surface.clone(),
        score: entity.score,
        timestamp_ms,
        tree_id: tree_id.map(str::to_owned),
        is_user,
    }
}

/// Remove every occurrence for a node, retaining the empty manifest so later
/// writers remain fenced against stale index snapshots.
pub fn clear_entity_index_for_node(config: &MemoryConfig, node_id: &str) -> Result<usize> {
    for _ in 0..8 {
        let staged = stage_node_occurrences(config, node_id, std::iter::empty())?;
        let removed = staged
            .changes
            .iter()
            .filter(|change| {
                change.collection == crate::memory::chunks::collections::ENTITY_INDEX
                    && change.value.is_none()
            })
            .count();
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(removed);
        }
    }
    anyhow::bail!("entity-index clear conflicted repeatedly for node {node_id}")
}

/// Replace summary node occurrences and increment their co-occurrence edges.
pub fn index_summary_entity_ids(
    config: &MemoryConfig,
    entity_ids: &[String],
    node_id: &str,
    score: f32,
    timestamp_ms: i64,
    tree_id: Option<&str>,
) -> Result<usize> {
    for _ in 0..8 {
        let staged =
            stage_summary_entity_ids(config, entity_ids, node_id, score, timestamp_ms, tree_id)?;
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(entity_ids.len());
        }
    }
    anyhow::bail!("summary entity indexing conflicted repeatedly for node {node_id}")
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
