//! Persisted undirected entity co-occurrence edges.

use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::memory::chunks::collections::ENTITY_EDGES;
use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::store::entity_index::transaction::StagedEntityChanges;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct EntityEdge {
    entity_a: String,
    entity_b: String,
    weight: i64,
    updated_ms: i64,
}

fn order_pair<'a>(a: &'a str, b: &'a str) -> Option<(&'a str, &'a str)> {
    match a.cmp(b) {
        std::cmp::Ordering::Less => Some((a, b)),
        std::cmp::Ordering::Greater => Some((b, a)),
        std::cmp::Ordering::Equal => None,
    }
}

/// Return unique canonical undirected pairs for one entity set.
pub fn pairs_from_entities(entity_ids: &[String]) -> Vec<(String, String)> {
    let unique: Vec<_> = entity_ids
        .iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut pairs = Vec::new();
    for left in 0..unique.len() {
        for right in left + 1..unique.len() {
            if let Some((a, b)) = order_pair(unique[left], unique[right]) {
                pairs.push((a.to_string(), b.to_string()));
            }
        }
    }
    pairs
}

fn edge_key(a: &str, b: &str) -> String {
    let encode = |value: &str| {
        let mut out = String::with_capacity(value.len() * 2);
        for byte in value.as_bytes() {
            use std::fmt::Write as _;
            write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
        }
        out
    };
    format!("edge:{}:{}", encode(a), encode(b))
}

/// Stage edge-weight increments; callers include the returned comparisons and
/// writes with their node/entity changes in one owner-head commit.
pub(crate) fn stage_edge_pairs(
    config: &MemoryConfig,
    pairs: &[(String, String)],
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    stage_edge_pairs_from(
        versioned::list_state(config, ENTITY_EDGES)?,
        pairs,
        timestamp_ms,
    )
}

pub(crate) async fn stage_edge_pairs_async(
    config: &MemoryConfig,
    pairs: &[(String, String)],
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    stage_edge_pairs_from(
        versioned::list_state_async(config, ENTITY_EDGES).await?,
        pairs,
        timestamp_ms,
    )
}

fn stage_edge_pairs_from(
    entries: Vec<(String, serde_json::Value)>,
    pairs: &[(String, String)],
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    let current: HashMap<_, _> = entries.into_iter().collect();
    let mut staged = StagedEntityChanges::default();
    let canonical: BTreeSet<_> = pairs
        .iter()
        .filter_map(|(a, b)| order_pair(a, b).map(|(a, b)| (a.to_owned(), b.to_owned())))
        .collect();
    for (a, b) in canonical {
        let key = edge_key(&a, &b);
        let value = current.get(&key).cloned();
        let weight = if let Some(value) = &value {
            let existing: EntityEdge = serde_json::from_value(value.clone())
                .context("decode entity co-occurrence edge")?;
            anyhow::ensure!(
                existing.entity_a == a && existing.entity_b == b,
                "entity edge key does not match its record"
            );
            existing.weight.saturating_add(1)
        } else {
            1
        };
        staged.expected.push(StateChange {
            collection: ENTITY_EDGES.to_owned(),
            key: key.clone(),
            value,
        });
        staged.changes.push(StateChange {
            collection: ENTITY_EDGES.to_owned(),
            key,
            value: Some(serde_json::to_value(EntityEdge {
                entity_a: a,
                entity_b: b,
                weight,
                updated_ms: timestamp_ms,
            })?),
        });
    }
    Ok(staged)
}

/// Increment co-occurrence edge weights. Conflicting concurrent updates are
/// retried from a fresh state snapshot so no increment is lost.
pub fn upsert_edges(
    config: &MemoryConfig,
    pairs: &[(String, String)],
    timestamp_ms: i64,
) -> Result<usize> {
    let unique: BTreeSet<_> = pairs
        .iter()
        .filter_map(|(a, b)| order_pair(a, b).map(|(a, b)| (a.to_owned(), b.to_owned())))
        .collect();
    if unique.is_empty() {
        return Ok(0);
    }
    for _ in 0..8 {
        let staged = stage_edge_pairs(
            config,
            &unique.iter().cloned().collect::<Vec<_>>(),
            timestamp_ms,
        )?;
        if versioned::commit_if_state_matches(config, &staged.expected, &[], &staged.changes)? {
            return Ok(unique.len());
        }
    }
    anyhow::bail!("entity edge update conflicted repeatedly")
}

/// Read adjacent entities ordered by accumulated co-occurrence weight.
pub fn edge_neighbors(config: &MemoryConfig, entity_id: &str) -> Result<Vec<(String, i64)>> {
    let mut neighbors = Vec::new();
    for (_, value) in versioned::list_state(config, ENTITY_EDGES)? {
        let edge: EntityEdge =
            serde_json::from_value(value).context("decode entity co-occurrence edge")?;
        if edge.entity_a == entity_id {
            neighbors.push((edge.entity_b, edge.weight));
        } else if edge.entity_b == entity_id {
            neighbors.push((edge.entity_a, edge.weight));
        }
    }
    neighbors.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    Ok(neighbors)
}

/// Async counterpart to [`edge_neighbors`] for current-thread runtimes.
pub async fn edge_neighbors_async(
    config: &MemoryConfig,
    entity_id: &str,
) -> Result<Vec<(String, i64)>> {
    let mut neighbors = Vec::new();
    for (_, value) in versioned::list_state_async(config, ENTITY_EDGES).await? {
        let edge: EntityEdge =
            serde_json::from_value(value).context("decode entity co-occurrence edge")?;
        if edge.entity_a == entity_id {
            neighbors.push((edge.entity_b, edge.weight));
        } else if edge.entity_b == entity_id {
            neighbors.push((edge.entity_a, edge.weight));
        }
    }
    neighbors.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    Ok(neighbors)
}

/// Remove all edges incident to any of the supplied entities.
pub fn clear_edges_for_entities(config: &MemoryConfig, entity_ids: &[String]) -> Result<usize> {
    let remove: BTreeSet<_> = entity_ids.iter().collect();
    if remove.is_empty() {
        return Ok(0);
    }
    for _ in 0..8 {
        let current = versioned::list_state(config, ENTITY_EDGES)?;
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        for (key, value) in current {
            let edge: EntityEdge = serde_json::from_value(value.clone())
                .context("decode entity co-occurrence edge")?;
            if remove.contains(&edge.entity_a) || remove.contains(&edge.entity_b) {
                expected.push(StateChange {
                    collection: ENTITY_EDGES.to_owned(),
                    key: key.clone(),
                    value: Some(value),
                });
                changes.push(StateChange {
                    collection: ENTITY_EDGES.to_owned(),
                    key,
                    value: None,
                });
            }
        }
        if changes.is_empty() {
            return Ok(0);
        }
        let count = changes.len();
        if versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
            return Ok(count);
        }
    }
    anyhow::bail!("entity edge deletion conflicted repeatedly")
}

pub fn count_edges(config: &MemoryConfig) -> Result<u64> {
    Ok(versioned::list_state(config, ENTITY_EDGES)?.len() as u64)
}

#[cfg(test)]
#[path = "edge_store_tests.rs"]
mod tests;
