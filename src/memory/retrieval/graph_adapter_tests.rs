//! Tests that graph adapters read the same host-owned occurrence index.

use crate::memory::chunks::test_support::host_config;
use crate::memory::graph::co_occurring_entities;
use crate::memory::graph::EntityOccurrenceIndex;
use crate::memory::retrieval::graph_adapter::ConfigEntityIndex;
use crate::memory::score::extract::EntityKind;
use crate::memory::score::resolver::CanonicalEntity;

fn entity(id: &str, kind: EntityKind) -> CanonicalEntity {
    CanonicalEntity {
        canonical_id: id.to_owned(),
        kind,
        surface: id.to_owned(),
        span_start: 0,
        span_end: id.len() as u32,
        score: 1.0,
    }
}

#[test]
fn graph_adapter_reads_versioned_occurrences_and_counts_distinct_nodes() {
    let temp = tempfile::tempdir().unwrap();
    let config = host_config(temp.path());
    crate::memory::score::store::index_entities(
        &config,
        &[
            entity("topic:phoenix", EntityKind::Topic),
            entity("person:alice", EntityKind::Person),
        ],
        "node-a",
        "summary",
        10,
        None,
    )
    .unwrap();
    crate::memory::score::store::index_entities(
        &config,
        &[
            entity("topic:phoenix", EntityKind::Topic),
            entity("person:alice", EntityKind::Person),
        ],
        "node-b",
        "leaf",
        11,
        None,
    )
    .unwrap();

    let index = ConfigEntityIndex::new(&config);
    assert_eq!(
        index.nodes_for_entity("topic:phoenix").unwrap(),
        vec!["node-b", "node-a"]
    );
    assert_eq!(
        index.entities_on_node("node-a").unwrap(),
        vec!["person:alice", "topic:phoenix"]
    );
    let edges = co_occurring_entities(&index, "topic:phoenix", Some(10)).unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].object, "person:alice");
    assert_eq!(edges[0].weight, 2);
    assert!(index.nodes_for_entity("topic:missing").unwrap().is_empty());
}
