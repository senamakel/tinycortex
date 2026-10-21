use super::*;
use crate::memory::chunks::test_support::host_config;
use crate::memory::store::entity_index::types::{CanonicalEntity, EntityKind};

fn config() -> (tempfile::TempDir, crate::memory::config::MemoryConfig) {
    let temp = tempfile::tempdir().unwrap();
    let config = host_config(temp.path());
    (temp, config)
}

fn sample_entity(id: &str) -> CanonicalEntity {
    CanonicalEntity {
        canonical_id: format!("email:{id}"),
        kind: EntityKind::Email,
        surface: format!("{id}@example.com"),
        span_start: 0,
        span_end: (id.len() + 12) as u32,
        score: 1.0,
    }
}

#[derive(Debug)]
struct EmailSelf;

impl SelfIdentity for EmailSelf {
    fn is_self(&self, kind: EntityKind, surface: &str) -> bool {
        kind == EntityKind::Email && surface == "alice@example.com"
    }
}

#[test]
fn prepared_storage_is_shared_with_score_queries() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config(&config).unwrap();
    index
        .index_entity(&sample_entity("alice"), "chunk-1", "leaf", 1000, None)
        .unwrap();
    let hits = crate::memory::score::store::lookup_entity(&config, "email:alice", None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, "chunk-1");
}

#[test]
fn identity_marks_rows_and_is_preserved_by_later_identity_free_updates() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config_with_identity(&config, Arc::new(EmailSelf)).unwrap();
    let alice = sample_entity("alice");
    index
        .index_entity(&alice, "chunk-1", "leaf", 1000, None)
        .unwrap();
    let hits = index.lookup_entity("email:alice", None).unwrap();
    assert!(hits[0].is_user);

    let plain = EntityIndex::for_memory_config(&config).unwrap();
    plain
        .index_entity(&alice, "chunk-1", "leaf", 2000, None)
        .unwrap();
    assert!(plain.lookup_entity("email:alice", None).unwrap()[0].is_user);
}

#[test]
fn entity_kind_wire_strings_round_trip_and_reject_unknowns() {
    for (kind, wire, mechanical) in [
        (EntityKind::Email, "email", true),
        (EntityKind::Url, "url", true),
        (EntityKind::Handle, "handle", true),
        (EntityKind::Hashtag, "hashtag", true),
        (EntityKind::Person, "person", false),
        (EntityKind::Organization, "organization", false),
        (EntityKind::Location, "location", false),
        (EntityKind::Event, "event", false),
        (EntityKind::Product, "product", false),
        (EntityKind::Datetime, "datetime", false),
        (EntityKind::Technology, "technology", false),
        (EntityKind::Artifact, "artifact", false),
        (EntityKind::Quantity, "quantity", false),
        (EntityKind::Misc, "misc", false),
        (EntityKind::Topic, "topic", false),
    ] {
        assert_eq!(kind.as_str(), wire);
        assert_eq!(EntityKind::parse(wire).unwrap(), kind);
        assert_eq!(kind.is_mechanical(), mechanical);
    }
    assert!(EntityKind::parse("unknown").is_err());
}

#[test]
fn index_entity_is_additive_and_lookup_is_newest_first() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config(&config).unwrap();
    let alice = sample_entity("alice");
    let bob = sample_entity("bob");
    index
        .index_entity(&alice, "node", "leaf", 1000, None)
        .unwrap();
    index
        .index_entity(&bob, "node", "leaf", 1500, None)
        .unwrap();
    index
        .index_entity(&alice, "later", "leaf", 2000, None)
        .unwrap();

    assert_eq!(index.count_entity_index().unwrap(), 3);
    let hits = index.lookup_entity("email:alice", None).unwrap();
    assert_eq!(
        hits.iter()
            .map(|hit| hit.node_id.as_str())
            .collect::<Vec<_>>(),
        ["later", "node"]
    );
}

#[test]
fn index_batch_edges_and_repeated_occurrence_are_idempotent() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config(&config).unwrap();
    let entities = vec![sample_entity("a"), sample_entity("b"), sample_entity("c")];
    assert_eq!(
        index
            .index_entities(&entities, "node", "leaf", 1000, None)
            .unwrap(),
        3
    );
    assert_eq!(index.count_entity_index().unwrap(), 3);
    assert_eq!(crate::memory::graph::count_edges(&config).unwrap(), 3);
    index
        .index_entity(&entities[0], "node", "leaf", 1100, None)
        .unwrap();
    assert_eq!(index.count_entity_index().unwrap(), 3);
}

#[test]
fn clear_removes_only_the_target_nodes_occurrences_and_edges() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config(&config).unwrap();
    let alice = sample_entity("alice");
    let bob = sample_entity("bob");
    index
        .index_entities(&[alice.clone(), bob.clone()], "one", "leaf", 1000, None)
        .unwrap();
    index
        .index_entities(&[alice.clone(), bob], "two", "leaf", 2000, None)
        .unwrap();

    assert_eq!(index.clear_entity_index_for_node("one").unwrap(), 2);
    assert_eq!(index.lookup_entity("email:alice", None).unwrap().len(), 1);
    assert_eq!(
        index.list_entity_ids_for_node("one").unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(crate::memory::graph::count_edges(&config).unwrap(), 1);
}

#[test]
fn summary_rows_keep_parseable_kind_and_identity() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config_with_identity(&config, Arc::new(EmailSelf)).unwrap();
    index
        .index_summary_entity_ids(
            &["email:alice@example.com".into(), "hashtag:launch-q2".into()],
            "summary",
            0.84,
            2000,
            Some("tree"),
        )
        .unwrap();
    let email = index
        .lookup_entity("email:alice@example.com", None)
        .unwrap();
    assert_eq!(email[0].entity_kind, EntityKind::Email);
    assert!(email[0].is_user);
    let tag = index.lookup_entity("hashtag:launch-q2", None).unwrap();
    assert_eq!(tag[0].entity_kind, EntityKind::Hashtag);
}

#[test]
fn entity_ids_for_node_sort_by_score() {
    let (_temp, config) = config();
    let index = EntityIndex::for_memory_config(&config).unwrap();
    let mut high = sample_entity("high");
    high.score = 0.9;
    let mut low = sample_entity("low");
    low.score = 0.1;
    index
        .index_entities(&[low, high], "node", "leaf", 1000, None)
        .unwrap();
    assert_eq!(
        index.list_entity_ids_for_node("node").unwrap(),
        vec!["email:high", "email:low"]
    );
}
