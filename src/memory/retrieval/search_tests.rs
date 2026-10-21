//! Search behavior over the real testkit-backed owner store.

use super::*;
use crate::memory::chunks::test_support::host_config;
use crate::memory::score::extract::EntityKind;
use crate::memory::score::resolver::CanonicalEntity;

fn test_config() -> (tempfile::TempDir, MemoryConfig) {
    let temp = tempfile::tempdir().unwrap();
    let config = host_config(temp.path());
    (temp, config)
}

fn seed(
    config: &MemoryConfig,
    canonical_id: &str,
    kind: EntityKind,
    surface: &str,
    node: &str,
    ts: i64,
) {
    crate::memory::score::store::index_entity(
        config,
        &CanonicalEntity {
            canonical_id: canonical_id.to_owned(),
            kind,
            surface: surface.to_owned(),
            span_start: 0,
            span_end: surface.len() as u32,
            score: 1.0,
        },
        node,
        "leaf",
        ts,
        None,
    )
    .unwrap();
}

fn seed_entities(config: &MemoryConfig, node: &str) {
    seed(
        config,
        "email:alice@example.com",
        EntityKind::Email,
        "alice@example.com",
        node,
        1_700_000_000_000,
    );
    seed(
        config,
        "email:bob@example.com",
        EntityKind::Email,
        "bob@example.com",
        node,
        1_700_000_000_000,
    );
    seed(
        config,
        "hashtag:launch-q2",
        EntityKind::Hashtag,
        "#launch-q2",
        node,
        1_700_000_000_000,
    );
}

#[test]
fn empty_and_blank_queries_return_no_matches() {
    let (_tmp, config) = test_config();
    assert!(search_entities(&config, "alice", None, 10)
        .unwrap()
        .is_empty());
    seed_entities(&config, "leaf-1");
    assert!(search_entities(&config, "   ", None, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn searches_case_insensitive_substrings_in_ids_and_surfaces() {
    let (_tmp, config) = test_config();
    seed_entities(&config, "leaf-1");
    assert!(search_entities(&config, "ALICE", None, 10)
        .unwrap()
        .iter()
        .any(|hit| hit.canonical_id == "email:alice@example.com"));
    assert!(search_entities(&config, "launch", None, 10)
        .unwrap()
        .iter()
        .any(|hit| hit.canonical_id == "hashtag:launch-q2"));
}

#[test]
fn wildcard_characters_are_literal_substrings() {
    let (_tmp, config) = test_config();
    seed(
        &config,
        "topic:100%_ready",
        EntityKind::Topic,
        "100%_ready",
        "literal",
        1,
    );
    seed(
        &config,
        "topic:100x-ready",
        EntityKind::Topic,
        "100x-ready",
        "decoy",
        2,
    );
    let matches = search_entities(&config, "100%_", None, 10).unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].canonical_id, "topic:100%_ready");
}

#[test]
fn kind_filter_and_aggregate_order_are_deterministic() {
    let (_tmp, config) = test_config();
    seed(
        &config,
        "email:alice@example.com",
        EntityKind::Email,
        "Alice",
        "a",
        1,
    );
    seed(
        &config,
        "email:alice@example.com",
        EntityKind::Email,
        "Alice A",
        "b",
        3,
    );
    seed(
        &config,
        "hashtag:alice-launch",
        EntityKind::Hashtag,
        "#alice-launch",
        "c",
        2,
    );
    let filtered = search_entities(&config, "alice", Some(&[EntityKind::Hashtag]), 10).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].kind, EntityKind::Hashtag);
    let matches = search_entities(&config, "alice", None, 10).unwrap();
    assert_eq!(matches[0].canonical_id, "email:alice@example.com");
    assert_eq!(matches[0].mention_count, 2);
    assert_eq!(matches[0].last_seen_ms, 3);
}

#[test]
fn zero_limit_uses_default_and_large_limit_is_clamped() {
    let (_tmp, config) = test_config();
    assert_eq!(
        normalise_limit(&config, 0),
        config.retrieval.limits.search_default_limit
    );
    assert_eq!(
        normalise_limit(&config, 10_000),
        config.retrieval.limits.max_limit
    );
}
