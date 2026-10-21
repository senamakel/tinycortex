use super::*;
use crate::memory::chunks::test_support::host_config;
use crate::memory::score::extract::EntityKind;
use crate::memory::score::resolver::CanonicalEntity;

fn fixture() -> (tempfile::TempDir, MemoryConfig) {
    let temp = tempfile::tempdir().unwrap();
    let config = host_config(temp.path());
    (temp, config)
}

fn entity(id: &str, score: f32) -> CanonicalEntity {
    CanonicalEntity {
        canonical_id: format!("topic:{id}"),
        kind: EntityKind::Topic,
        surface: id.to_owned(),
        span_start: 0,
        span_end: id.len() as u32,
        score,
    }
}

#[test]
fn lookup_entity_in_window_filters_orders_and_limits() {
    let (_temp, config) = fixture();
    for (node, timestamp) in [("old", 10), ("mid", 20), ("new", 30)] {
        crate::memory::score::store::index_entity(
            &config,
            &entity("x", 0.5),
            node,
            "leaf",
            timestamp,
            None,
        )
        .unwrap();
    }
    crate::memory::score::store::index_entity(
        &config,
        &entity("y", 0.9),
        "other",
        "leaf",
        25,
        None,
    )
    .unwrap();

    let hits = lookup_entity_in_window(&config, "topic:x", Some(15), Some(35), Some(1)).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node_id, "new");
}

#[test]
fn list_entity_ids_for_node_is_distinct_and_deterministic() {
    let (_temp, config) = fixture();
    crate::memory::score::store::index_entities(
        &config,
        &[entity("a", 0.4), entity("b", 0.9)],
        "node",
        "leaf",
        10,
        None,
    )
    .unwrap();
    assert_eq!(
        list_entity_ids_for_node(&config, "node").unwrap(),
        vec!["topic:b", "topic:a"]
    );
}
