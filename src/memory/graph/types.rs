//! Graph type definitions: the derived edge shape and the occurrence-index
//! contract the graph queries read through.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A derived co-occurrence edge between two entities.
///
/// Not a triple in the classical sense — there is no explicit predicate. The
/// `weight` field is the count of distinct nodes the pair has both appeared
/// on, which serves as a cheap proxy for relationship strength.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    /// The entity the query was anchored on.
    pub subject: String,
    /// A co-occurring entity sharing at least one node with `subject`.
    pub object: String,
    /// Number of distinct nodes on which `subject` and `object` both appear.
    pub weight: u32,
}

/// Read contract for deriving graph co-occurrence from entity occurrences.
///
/// Production adapters read TinyCortex's owner-scoped versioned occurrence
/// collection. The trait remains injectable so graph ordering and grouping
/// behavior can be tested independently of a host storage backend.
///
/// The two operations provide the index reads needed by a co-occurrence query:
///
/// - [`nodes_for_entity`] is the `WHERE a.entity_id = ?` side.
/// - [`entities_on_node`] is the `JOIN … ON a.node_id = b.node_id` side.
///
/// Both must return **distinct** ids so that the derived weight equals
/// `COUNT(DISTINCT node_id)`, matching the original query semantics.
///
/// [`nodes_for_entity`]: EntityOccurrenceIndex::nodes_for_entity
/// [`entities_on_node`]: EntityOccurrenceIndex::entities_on_node
pub trait EntityOccurrenceIndex {
    /// Optional backend-native co-occurrence query.
    ///
    /// Persistent adapters should implement this as one set-based query;
    /// lightweight test indexes may return `Ok(None)` to use the portable
    /// `nodes_for_entity` + `entities_on_node` derivation.
    fn co_occurring_entities(
        &self,
        _subject_entity: &str,
        _limit: usize,
    ) -> Result<Option<Vec<GraphEdge>>> {
        Ok(None)
    }

    /// Distinct node ids on which `entity_id` has been indexed.
    ///
    /// An entity with no occurrences yields an empty vector (not an error).
    fn nodes_for_entity(&self, entity_id: &str) -> Result<Vec<String>>;

    /// Distinct canonical entity ids indexed against `node_id`.
    ///
    /// A node with no entities yields an empty vector (not an error).
    fn entities_on_node(&self, node_id: &str) -> Result<Vec<String>>;
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
