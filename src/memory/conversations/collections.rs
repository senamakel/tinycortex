//! Pure storage declarations owned by the conversation store.
//!
//! The host calls [`collections`] while preparing its scoped document store.
//! This module performs no I/O.

use tinystoragedrivers_types::{CollectionSpec, IndexSpec};

/// The document collections required for rebuildable conversation indexes.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    vec![CollectionSpec::new(THREAD_INDEX)
        .index(IndexSpec::new("by_updated_at", ["updated_at"]))
        .index(IndexSpec::new("by_parent", ["parent_thread_id"]))]
}

/// Collection holding rebuildable per-thread metadata projections.
pub const THREAD_INDEX: &str = "memory_conversation_thread_index";

#[cfg(test)]
#[path = "collections_tests.rs"]
mod tests;
