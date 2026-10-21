//! Entity occurrence index over the owner collection's node-keyed manifests.
//!
//! Maps a canonical entity id to every tree node (chunk or summary) it appears
//! in, so retrieval can resolve entity-scoped queries in O(lookup). This is the
//! occurrence index over tree nodes, NOT the markdown contact registry.
//!
//! Ported from OpenHuman's `memory_tree::score::store` (entity CRUD) plus the
//! `EntityKind` / `CanonicalEntity` / `EntityHit` types from the score module.
//!
//! ## Concurrency / atomicity contract
//!
//! Use [`EntityIndex::for_memory_config`] with a prepared host storage bundle.
//! Writes use TinyCortex's owner-head compare-and-swap state commit; the index
//! does not create an engine, connection, or fallback store.

pub mod store;
pub(crate) mod transaction;
pub mod types;

pub use store::{EntityIndex, NoSelfIdentity, SelfIdentity};
pub use types::{CanonicalEntity, EntityHit, EntityKind};
