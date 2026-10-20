//! Tests for versioned entity-hotness persistence.

use super::*;
use crate::memory::chunks::test_support::host_config;
use crate::memory::store::entity_index::transaction::{stage_node_occurrences, EntityOccurrence};
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = host_config(tmp.path());
    (tmp, cfg)
}

#[test]
fn get_missing_is_none() {
    let (_tmp, cfg) = test_config();
    assert!(get(&cfg, "email:alice@example.com").unwrap().is_none());
}

#[test]
fn get_or_fresh_returns_zero_row() {
    let (_tmp, cfg) = test_config();
    let counters = get_or_fresh(&cfg, "email:alice@example.com").unwrap();
    assert_eq!(counters.entity_id, "email:alice@example.com");
    assert_eq!(counters.mention_count_30d, 0);
    assert_eq!(counters.distinct_sources, 0);
    assert!(counters.last_hotness.is_none());
    assert_eq!(count(&cfg).unwrap(), 0);
}

#[test]
fn upsert_round_trip_and_replacement() {
    let (_tmp, cfg) = test_config();
    let mut counters = HotnessCounters {
        entity_id: "email:alice@example.com".into(),
        mention_count_30d: 12,
        distinct_sources: 3,
        last_seen_ms: Some(1_700_000_000_000),
        query_hits_30d: 2,
        graph_centrality: Some(0.25),
        ingests_since_check: 40,
        last_hotness: Some(9.5),
        last_updated_ms: 1_700_000_123_000,
    };
    upsert(&cfg, &counters).unwrap();
    assert_eq!(get(&cfg, &counters.entity_id).unwrap().unwrap(), counters);

    counters.mention_count_30d = 7;
    counters.query_hits_30d = 3;
    counters.last_hotness = Some(4.5);
    counters.last_updated_ms = 1_700_000_124_000;
    upsert(&cfg, &counters).unwrap();

    assert_eq!(count(&cfg).unwrap(), 1);
    assert_eq!(get_or_fresh(&cfg, &counters.entity_id).unwrap(), counters);
}

#[test]
fn distinct_sources_counts_unique_non_null_tree_ids_from_entity_owner_state() {
    let (_tmp, cfg) = test_config();
    for (node_id, tree_id) in [
        ("alice", Some("tree-a")),
        ("bob", Some("tree-a")),
        ("carol", Some("tree-b")),
        ("dave", None),
    ] {
        let occurrence = EntityOccurrence {
            entity_id: "topic:rust".into(),
            node_id: node_id.into(),
            node_kind: "leaf".into(),
            entity_kind: "topic".into(),
            surface: "rust".into(),
            score: 1.0,
            timestamp_ms: 1,
            tree_id: tree_id.map(str::to_owned),
            is_user: false,
        };
        let staged = stage_node_occurrences(&cfg, node_id, [occurrence]).unwrap();
        assert!(
            versioned::commit_if_state_matches(&cfg, &staged.expected, &[], &staged.changes)
                .unwrap()
        );
    }

    assert_eq!(distinct_sources_for(&cfg, "topic:rust").unwrap(), 2);
    assert_eq!(distinct_sources_for(&cfg, "topic:missing").unwrap(), 0);
}
