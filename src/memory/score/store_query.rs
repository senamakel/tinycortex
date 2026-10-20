//! Read-only score and entity-index diagnostics over owner state.

use anyhow::{Context, Result};

use crate::memory::chunks::collections::{ENTITY_INDEX, SCORES};
use crate::memory::chunks::versioned;
use crate::memory::config::MemoryConfig;
use crate::memory::score::extract::EntityKind;
use crate::memory::score::store::EntityHit;
use crate::memory::store::entity_index::transaction::EntityOccurrence;

pub fn lookup_entity(
    config: &MemoryConfig,
    entity_id: &str,
    limit: Option<usize>,
) -> Result<Vec<EntityHit>> {
    lookup_entity_in_window(config, entity_id, None, None, limit)
}

pub async fn lookup_entity_async(
    config: &MemoryConfig,
    entity_id: &str,
    limit: Option<usize>,
) -> Result<Vec<EntityHit>> {
    lookup_entity_in_window_async(config, entity_id, None, None, limit).await
}

pub fn lookup_entity_in_window(
    config: &MemoryConfig,
    entity_id: &str,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    limit: Option<usize>,
) -> Result<Vec<EntityHit>> {
    let mut rows = read_occurrences(config)?
        .into_iter()
        .filter(|row| {
            row.entity_id == entity_id
                && since_ms.is_none_or(|since| row.timestamp_ms >= since)
                && until_ms.is_none_or(|until| row.timestamp_ms <= until)
        })
        .map(to_hit)
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .timestamp_ms
            .cmp(&left.timestamp_ms)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    rows.truncate(limit.unwrap_or(100).min(i64::MAX as usize));
    Ok(rows)
}

pub async fn lookup_entity_in_window_async(
    config: &MemoryConfig,
    entity_id: &str,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    limit: Option<usize>,
) -> Result<Vec<EntityHit>> {
    let mut rows = read_occurrences_async(config)
        .await?
        .into_iter()
        .filter(|row| {
            row.entity_id == entity_id
                && since_ms.is_none_or(|since| row.timestamp_ms >= since)
                && until_ms.is_none_or(|until| row.timestamp_ms <= until)
        })
        .map(to_hit)
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .timestamp_ms
            .cmp(&left.timestamp_ms)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    rows.truncate(limit.unwrap_or(100).min(i64::MAX as usize));
    Ok(rows)
}

pub fn list_entity_ids_for_node(config: &MemoryConfig, node_id: &str) -> Result<Vec<String>> {
    let mut rows: Vec<_> = read_occurrences(config)?
        .into_iter()
        .filter(|row| row.node_id == node_id)
        .collect();
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.timestamp_ms.cmp(&left.timestamp_ms))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    rows.dedup_by(|left, right| left.entity_id == right.entity_id);
    Ok(rows.into_iter().map(|row| row.entity_id).collect())
}

pub async fn list_entity_ids_for_node_async(
    config: &MemoryConfig,
    node_id: &str,
) -> Result<Vec<String>> {
    let mut rows: Vec<_> = read_occurrences_async(config)
        .await?
        .into_iter()
        .filter(|row| row.node_id == node_id)
        .collect();
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.timestamp_ms.cmp(&left.timestamp_ms))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    rows.dedup_by(|left, right| left.entity_id == right.entity_id);
    Ok(rows.into_iter().map(|row| row.entity_id).collect())
}

pub fn count_entity_index(config: &MemoryConfig) -> Result<u64> {
    Ok(read_occurrences(config)?.len() as u64)
}

pub fn count_scores(config: &MemoryConfig) -> Result<u64> {
    Ok(versioned::list_state(config, SCORES)?.len() as u64)
}

fn read_occurrences(config: &MemoryConfig) -> Result<Vec<EntityOccurrence>> {
    versioned::list_state(config, ENTITY_INDEX)?
        .into_iter()
        .filter(|(key, _)| key.starts_with("occ:"))
        .map(|(_, value)| serde_json::from_value(value).context("decode entity-index occurrence"))
        .collect()
}

async fn read_occurrences_async(config: &MemoryConfig) -> Result<Vec<EntityOccurrence>> {
    versioned::list_state_async(config, ENTITY_INDEX)
        .await?
        .into_iter()
        .filter(|(key, _)| key.starts_with("occ:"))
        .map(|(_, value)| serde_json::from_value(value).context("decode entity-index occurrence"))
        .collect()
}

fn to_hit(row: EntityOccurrence) -> Result<EntityHit> {
    let entity_kind = EntityKind::parse(&row.entity_kind)
        .map_err(anyhow::Error::msg)
        .context("decode entity-index kind")?;
    Ok(EntityHit {
        entity_id: row.entity_id,
        node_id: row.node_id,
        node_kind: row.node_kind,
        entity_kind,
        surface: row.surface,
        score: row.score,
        timestamp_ms: row.timestamp_ms,
        tree_id: row.tree_id,
        is_user: row.is_user,
    })
}

#[cfg(test)]
#[path = "store_query_tests.rs"]
mod tests;
