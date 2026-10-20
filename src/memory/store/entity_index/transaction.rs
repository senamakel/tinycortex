//! Compare-and-swap entity-index state builders.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::memory::chunks::collections::ENTITY_INDEX;
use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;

/// Preconditions and mutations staged for one logical update.
#[derive(Clone, Debug, Default)]
pub(crate) struct StagedEntityChanges {
    pub(crate) expected: Vec<StateChange>,
    pub(crate) changes: Vec<StateChange>,
}

/// Serialized entity occurrence stored in the owner collection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EntityOccurrence {
    pub(crate) entity_id: String,
    pub(crate) node_id: String,
    pub(crate) node_kind: String,
    pub(crate) entity_kind: String,
    pub(crate) surface: String,
    pub(crate) score: f32,
    pub(crate) timestamp_ms: i64,
    pub(crate) tree_id: Option<String>,
    pub(crate) is_user: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct NodeIndexManifest {
    node_id: String,
    occurrence_keys: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TreeIndexManifest {
    tree_id: String,
    node_ids: Vec<String>,
}

/// Collision-safe key for a node's occurrence-set manifest.
pub(crate) fn node_manifest_key(node_id: &str) -> String {
    format!("node:{}", encode_key_part(node_id))
}

fn tree_manifest_key(tree_id: &str) -> String {
    format!("tree:{}", encode_key_part(tree_id))
}

/// Collision-safe key for an `(entity_id, node_id)` occurrence.
pub(crate) fn occurrence_key(entity_id: &str, node_id: &str) -> String {
    format!(
        "occ:{}:{}",
        encode_key_part(entity_id),
        encode_key_part(node_id)
    )
}

fn encode_key_part(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
#[path = "transaction_tests.rs"]
mod tests;

/// Stage replacement of every indexed occurrence for one node.
///
/// The whole collection is materialized under one versioned snapshot so the
/// existing rows and the manifest describe the same commit head. Every writer
/// also compares the manifest value; concurrent first writers or replacements
/// of the same node cannot overwrite each other's occurrence set.
pub(crate) fn stage_node_occurrences(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
) -> Result<StagedEntityChanges> {
    let entries = versioned::list_state(config, ENTITY_INDEX)?;
    stage_node_occurrences_from(entries, node_id, occurrences, true)
}

/// Async counterpart for current-thread-safe owner workflows.
pub(crate) async fn stage_node_occurrences_async(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
) -> Result<StagedEntityChanges> {
    let entries = versioned::list_state_async(config, ENTITY_INDEX).await?;
    stage_node_occurrences_from(entries, node_id, occurrences, true)
}

/// Stage additive index updates, preserving occurrences not mentioned by the
/// caller. Used by the standalone entity-index facade; score rescores use the
/// replacement operation above so stale entities are removed.
pub(crate) fn stage_upsert_node_occurrences(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
) -> Result<StagedEntityChanges> {
    let entries = versioned::list_state(config, ENTITY_INDEX)?;
    stage_node_occurrences_from(entries, node_id, occurrences, false)
}

pub(crate) async fn stage_upsert_node_occurrences_async(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
) -> Result<StagedEntityChanges> {
    let entries = versioned::list_state_async(config, ENTITY_INDEX).await?;
    stage_node_occurrences_from(entries, node_id, occurrences, false)
}

/// Stage deletion of every node occurrence owned by one tree. Aggregate
/// co-occurrence edge history is intentionally retained: the legacy delete
/// path removed occurrence rows but did not remove/decrement aggregate edges,
/// whose increments cannot be attributed to a particular node.
pub(crate) async fn stage_delete_tree_occurrences_async(
    config: &MemoryConfig,
    tree_id: &str,
) -> Result<StagedEntityChanges> {
    let entries = versioned::list_state_async(config, ENTITY_INDEX).await?;
    let values: HashMap<_, _> = entries.iter().cloned().collect();
    let tree_key = tree_manifest_key(tree_id);
    let tree_value = values.get(&tree_key).cloned();
    let mut node_ids = BTreeMap::<String, ()>::new();
    if let Some(value) = &tree_value {
        let manifest: TreeIndexManifest =
            serde_json::from_value(value.clone()).context("decode entity-index tree manifest")?;
        anyhow::ensure!(
            manifest.tree_id == tree_id,
            "entity-index tree manifest key mismatch"
        );
        node_ids.extend(manifest.node_ids.into_iter().map(|node_id| (node_id, ())));
    } else {
        for (key, value) in entries.iter().filter(|(key, _)| key.starts_with("occ:")) {
            let row: EntityOccurrence = serde_json::from_value(value.clone())
                .context("decode entity occurrence for tree deletion")?;
            anyhow::ensure!(
                occurrence_key(&row.entity_id, &row.node_id) == *key,
                "entity-index occurrence key does not match its record"
            );
            if row.tree_id.as_deref() == Some(tree_id) {
                node_ids.insert(row.node_id, ());
            }
        }
    }

    let mut staged = StagedEntityChanges::default();
    for node_id in node_ids.into_keys() {
        let mut node = stage_node_occurrences_from(entries.clone(), &node_id, [], true)?;
        let manifest_key = node_manifest_key(&node_id);
        for change in &mut node.changes {
            if change.key == manifest_key {
                change.value = None;
            }
        }
        staged.expected.extend(
            node.expected
                .into_iter()
                .filter(|change| change.key != tree_key),
        );
        staged.changes.extend(
            node.changes
                .into_iter()
                .filter(|change| change.key != tree_key),
        );
    }
    // The tree-wide sentinel protects against a node being added after this
    // snapshot but before the cascade commit.
    staged.expected.push(StateChange {
        collection: ENTITY_INDEX.to_owned(),
        key: tree_key.clone(),
        value: tree_value,
    });
    staged.changes.push(StateChange {
        collection: ENTITY_INDEX.to_owned(),
        key: tree_key,
        value: Some(serde_json::to_value(TreeIndexManifest {
            tree_id: tree_id.to_owned(),
            node_ids: Vec::new(),
        })?),
    });
    Ok(staged)
}

fn stage_node_occurrences_from(
    entries: Vec<(String, serde_json::Value)>,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
    replace: bool,
) -> Result<StagedEntityChanges> {
    let values: HashMap<_, _> = entries.into_iter().collect();
    let manifest_key = node_manifest_key(node_id);
    let old_manifest = values.get(&manifest_key).cloned();

    let old_keys = if let Some(value) = &old_manifest {
        let manifest: NodeIndexManifest =
            serde_json::from_value(value.clone()).context("decode entity-index node manifest")?;
        anyhow::ensure!(
            manifest.node_id == node_id,
            "entity-index manifest key mismatch"
        );
        for key in &manifest.occurrence_keys {
            let value = values
                .get(key)
                .with_context(|| format!("entity-index manifest references missing row {key}"))?;
            let row: EntityOccurrence = serde_json::from_value(value.clone())
                .context("decode entity-index occurrence referenced by manifest")?;
            anyhow::ensure!(
                row.node_id == node_id,
                "entity-index manifest crosses node boundary"
            );
            anyhow::ensure!(
                occurrence_key(&row.entity_id, &row.node_id) == *key,
                "entity-index occurrence key does not match its record"
            );
        }
        manifest.occurrence_keys
    } else {
        let mut keys = Vec::new();
        for (key, value) in values.iter().filter(|(key, _)| key.starts_with("occ:")) {
            let row: EntityOccurrence = serde_json::from_value(value.clone())
                .context("decode unmanifested entity-index occurrence")?;
            anyhow::ensure!(
                occurrence_key(&row.entity_id, &row.node_id) == *key,
                "entity-index occurrence key does not match its record"
            );
            if row.node_id == node_id {
                keys.push(key.clone());
            }
        }
        keys
    };

    let old_tree_ids = old_keys
        .iter()
        .filter_map(|key| values.get(key))
        .map(|value| {
            serde_json::from_value::<EntityOccurrence>(value.clone())
                .context("decode old occurrence tree ownership")
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter_map(|row| row.tree_id)
        .collect::<std::collections::BTreeSet<_>>();

    // The old SQLite composite key made the last duplicate entity/node pair
    // win. Keep that result and preserve a previously true host identity bit.
    let mut next_rows = BTreeMap::<String, EntityOccurrence>::new();
    if !replace {
        for key in &old_keys {
            let value = values
                .get(key)
                .with_context(|| format!("entity-index manifest references missing row {key}"))?;
            let row: EntityOccurrence = serde_json::from_value(value.clone())
                .context("decode existing entity-index occurrence")?;
            next_rows.insert(key.clone(), row);
        }
    }
    for mut row in occurrences {
        anyhow::ensure!(
            row.node_id == node_id,
            "entity-index occurrence node mismatch"
        );
        let key = occurrence_key(&row.entity_id, node_id);
        if let Some(existing) = values.get(&key) {
            let old: EntityOccurrence = serde_json::from_value(existing.clone())
                .context("decode existing entity-index occurrence")?;
            row.is_user |= old.is_user;
        }
        next_rows.insert(key, row);
    }
    let new_tree_ids = next_rows
        .values()
        .filter_map(|row| row.tree_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let next_keys: Vec<_> = next_rows.keys().cloned().collect();

    let mut expected = vec![StateChange {
        collection: ENTITY_INDEX.to_owned(),
        key: manifest_key.clone(),
        value: old_manifest,
    }];
    let mut changes = Vec::new();
    for key in &old_keys {
        expected.push(StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key: key.clone(),
            value: values.get(key).cloned(),
        });
        if !next_rows.contains_key(key) {
            changes.push(StateChange {
                collection: ENTITY_INDEX.to_owned(),
                key: key.clone(),
                value: None,
            });
        }
    }
    for (key, row) in next_rows {
        if !old_keys.contains(&key) {
            expected.push(StateChange {
                collection: ENTITY_INDEX.to_owned(),
                key: key.clone(),
                value: values.get(&key).cloned(),
            });
        }
        changes.push(StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key,
            value: Some(serde_json::to_value(row)?),
        });
    }
    let manifest = NodeIndexManifest {
        node_id: node_id.to_owned(),
        occurrence_keys: next_keys,
    };
    changes.push(StateChange {
        collection: ENTITY_INDEX.to_owned(),
        key: manifest_key,
        value: Some(serde_json::to_value(manifest)?),
    });

    // Writers also conditionally update one tree-level sentinel. Tree-wide
    // deletion captures this key so a concurrent first node cannot be missed.
    for tree_id in old_tree_ids.union(&new_tree_ids) {
        let key = tree_manifest_key(tree_id);
        let current = values.get(&key).cloned();
        let mut manifest = if let Some(value) = &current {
            let manifest: TreeIndexManifest = serde_json::from_value(value.clone())
                .context("decode entity-index tree manifest")?;
            anyhow::ensure!(
                manifest.tree_id == *tree_id,
                "entity-index tree manifest key mismatch"
            );
            manifest
        } else {
            let mut node_ids = std::collections::BTreeSet::new();
            for (occ_key, value) in values.iter().filter(|(key, _)| key.starts_with("occ:")) {
                let occurrence: EntityOccurrence = serde_json::from_value(value.clone())
                    .context("decode occurrence while preparing tree manifest")?;
                anyhow::ensure!(
                    occurrence_key(&occurrence.entity_id, &occurrence.node_id) == *occ_key,
                    "entity-index occurrence key does not match its record"
                );
                if occurrence.tree_id.as_deref() == Some(tree_id) {
                    node_ids.insert(occurrence.node_id);
                }
            }
            TreeIndexManifest {
                tree_id: tree_id.clone(),
                node_ids: node_ids.into_iter().collect(),
            }
        };
        let mut node_ids: std::collections::BTreeSet<_> =
            manifest.node_ids.iter().cloned().collect();
        if old_tree_ids.contains(tree_id) {
            node_ids.remove(node_id);
        }
        if new_tree_ids.contains(tree_id) {
            node_ids.insert(node_id.to_owned());
        }
        expected.push(StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key: key.clone(),
            value: current,
        });
        manifest.node_ids = node_ids.into_iter().collect();
        changes.push(StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key,
            value: Some(serde_json::to_value(manifest)?),
        });
    }
    Ok(StagedEntityChanges { expected, changes })
}

/// Stage a node occurrence replacement and edge increments in one transaction.
pub(crate) fn stage_node_entities(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    let occurrences: Vec<_> = occurrences.into_iter().collect();
    let mut staged = stage_node_occurrences(config, node_id, occurrences.clone())?;
    let ids: Vec<_> = occurrences
        .iter()
        .map(|occurrence| occurrence.entity_id.clone())
        .collect();
    let edges = crate::memory::graph::edge_store::stage_edge_pairs(
        config,
        &crate::memory::graph::pairs_from_entities(&ids),
        timestamp_ms,
    )?;
    staged.expected.extend(edges.expected);
    staged.changes.extend(edges.changes);
    Ok(staged)
}

pub(crate) async fn stage_node_entities_async(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    let occurrences: Vec<_> = occurrences.into_iter().collect();
    let mut staged = stage_node_occurrences_async(config, node_id, occurrences.clone()).await?;
    let ids: Vec<_> = occurrences
        .iter()
        .map(|occurrence| occurrence.entity_id.clone())
        .collect();
    let edges = crate::memory::graph::edge_store::stage_edge_pairs_async(
        config,
        &crate::memory::graph::pairs_from_entities(&ids),
        timestamp_ms,
    )
    .await?;
    staged.expected.extend(edges.expected);
    staged.changes.extend(edges.changes);
    Ok(staged)
}

pub(crate) async fn stage_upsert_node_entities_async(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    let occurrences: Vec<_> = occurrences.into_iter().collect();
    let mut staged =
        stage_upsert_node_occurrences_async(config, node_id, occurrences.clone()).await?;
    let ids: Vec<_> = occurrences
        .iter()
        .map(|occurrence| occurrence.entity_id.clone())
        .collect();
    let edges = crate::memory::graph::edge_store::stage_edge_pairs_async(
        config,
        &crate::memory::graph::pairs_from_entities(&ids),
        timestamp_ms,
    )
    .await?;
    staged.expected.extend(edges.expected);
    staged.changes.extend(edges.changes);
    Ok(staged)
}

pub(crate) fn stage_upsert_node_entities(
    config: &MemoryConfig,
    node_id: &str,
    occurrences: impl IntoIterator<Item = EntityOccurrence>,
    timestamp_ms: i64,
) -> Result<StagedEntityChanges> {
    let occurrences: Vec<_> = occurrences.into_iter().collect();
    let mut staged = stage_upsert_node_occurrences(config, node_id, occurrences.clone())?;
    let ids: Vec<_> = occurrences
        .iter()
        .map(|occurrence| occurrence.entity_id.clone())
        .collect();
    let edges = crate::memory::graph::edge_store::stage_edge_pairs(
        config,
        &crate::memory::graph::pairs_from_entities(&ids),
        timestamp_ms,
    )?;
    staged.expected.extend(edges.expected);
    staged.changes.extend(edges.changes);
    Ok(staged)
}
