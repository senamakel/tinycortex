//! Pure collection declarations owned by the people store.
//!
//! Hosts include these declarations when preparing the scoped document store;
//! this module performs no I/O.

use tinystoragedrivers_types::{CollectionSpec, IndexSpec};

/// Logical document collections used for people, canonical handle aliases,
/// and interaction history.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(super::store::PEOPLE)
            .index(IndexSpec::new("by_display_name", ["display_name"])),
        CollectionSpec::new(super::store::PEOPLE_HANDLES)
            .index(IndexSpec::new("by_handle", ["kind", "value"]).unique())
            .index(IndexSpec::new("by_person", ["person_id"])),
        CollectionSpec::new(super::store::PEOPLE_INTERACTIONS)
            .index(IndexSpec::new("by_person_time", ["person_id", "timestamp"])),
    ]
}

#[cfg(test)]
#[path = "collections_tests.rs"]
mod tests;
