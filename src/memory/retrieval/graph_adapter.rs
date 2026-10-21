//! Adapter exposing the versioned entity occurrence index to graph queries.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::memory::chunks::collections::{ENTITY_INDEX, SCORES, SUMMARIES};
use crate::memory::chunks::versioned;
use crate::memory::config::MemoryConfig;
use crate::memory::graph::{EntityOccurrenceIndex, GraphEdge};
use crate::memory::score::store::{list_entity_ids_for_node, lookup_entity, ScoreRow};
use crate::memory::store::entity_index::transaction::EntityOccurrence;
use crate::memory::tree::SummaryNode;

/// Versioned [`EntityOccurrenceIndex`] over the host-owned occurrence index.
pub struct ConfigEntityIndex<'a> {
    config: &'a MemoryConfig,
}

impl<'a> ConfigEntityIndex<'a> {
    /// Bind the adapter to a prepared storage configuration.
    pub fn new(config: &'a MemoryConfig) -> Self {
        Self { config }
    }
}

impl EntityOccurrenceIndex for ConfigEntityIndex<'_> {
    fn co_occurring_entities(
        &self,
        subject_entity: &str,
        limit: usize,
    ) -> Result<Option<Vec<GraphEdge>>> {
        let mut dropped = HashSet::new();
        for (key, value) in versioned::list_state(self.config, SCORES)? {
            let row: ScoreRow = serde_json::from_value(value)?;
            if row.dropped {
                dropped.insert(key);
            }
        }
        let mut deleted = HashSet::new();
        for (key, value) in versioned::list_state(self.config, SUMMARIES)? {
            let row: SummaryNode = serde_json::from_value(value)?;
            if row.deleted {
                deleted.insert(key);
            }
        }
        let rows = versioned::list_state(self.config, ENTITY_INDEX)?
            .into_iter()
            .filter(|(key, _)| key.starts_with("occ:"))
            .map(|(_, value)| serde_json::from_value::<EntityOccurrence>(value))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|row| {
                if row.node_kind == "summary" {
                    !deleted.contains(&row.node_id)
                } else {
                    !dropped.contains(&row.node_id)
                }
            })
            .collect::<Vec<_>>();
        let nodes: HashSet<_> = rows
            .iter()
            .filter(|row| row.entity_id == subject_entity)
            .map(|row| row.node_id.clone())
            .collect();
        let mut shared = HashMap::<String, HashSet<String>>::new();
        for row in rows {
            if row.entity_id != subject_entity && nodes.contains(&row.node_id) {
                shared.entry(row.entity_id).or_default().insert(row.node_id);
            }
        }
        let mut edges: Vec<_> = shared
            .into_iter()
            .map(|(object, nodes)| GraphEdge {
                subject: subject_entity.to_owned(),
                object,
                weight: nodes.len().min(u32::MAX as usize) as u32,
            })
            .collect();
        edges.sort_by(|left, right| {
            right
                .weight
                .cmp(&left.weight)
                .then_with(|| left.object.cmp(&right.object))
        });
        edges.truncate(limit);
        Ok(Some(edges))
    }

    fn nodes_for_entity(&self, entity_id: &str) -> Result<Vec<String>> {
        let hits = lookup_entity(
            self.config,
            entity_id,
            Some(self.config.retrieval.limits.occurrence_lookup_limit),
        )?;
        let mut nodes = Vec::with_capacity(hits.len());
        let mut seen = HashSet::new();
        for hit in hits {
            if seen.insert(hit.node_id.clone()) {
                nodes.push(hit.node_id);
            }
        }
        Ok(nodes)
    }

    fn entities_on_node(&self, node_id: &str) -> Result<Vec<String>> {
        list_entity_ids_for_node(self.config, node_id)
    }
}
