//! Entity relationships derived from the host-owned versioned occurrence index.
//!
//! Co-occurrence queries count distinct nodes containing both entities. The
//! occurrence index and persisted edge weights use TinyCortex's owner-scoped
//! collections; hosts bind the required storage ports before these APIs run.
//! LLM-extracted `(subject, predicate, object)` triples remain a separate
//! domain record (`crate::memory::types::GraphRelationRecord`).
//!
//! [`query`] exposes a storage-agnostic read trait for derived graph queries;
//! `edge_store` owns persistent edge updates and traversal diagnostics.

mod bfs;
pub(crate) mod edge_store;
pub mod query;
pub mod types;

pub use bfs::{pair_distances, pair_distances_async, PairDistance};
pub use edge_store::{
    clear_edges_for_entities, count_edges, edge_neighbors, edge_neighbors_async,
    pairs_from_entities, upsert_edges,
};

pub use query::{co_occurring_entities, group_by_weight, neighbors};
pub use types::{EntityOccurrenceIndex, GraphEdge};
