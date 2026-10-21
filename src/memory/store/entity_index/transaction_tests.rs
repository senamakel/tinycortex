use super::*;

#[test]
fn key_encoding_is_unambiguous_for_delimiters_and_unicode() {
    assert_ne!(occurrence_key("a:b", "c"), occurrence_key("a", "b:c"));
    assert_ne!(
        occurrence_key("a/雪", "node"),
        occurrence_key("a", "雪/node")
    );
}

#[test]
fn malformed_unmanifested_occurrence_refuses_replacement_without_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let malformed_key = "occ:legacy:node";
    let malformed_value = serde_json::json!({ "node_id": "node", "entity_id": 3 });
    assert!(versioned::commit_if_state_matches(
        &config,
        &[StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key: malformed_key.to_owned(),
            value: None,
        }],
        &[],
        &[StateChange {
            collection: ENTITY_INDEX.to_owned(),
            key: malformed_key.to_owned(),
            value: Some(malformed_value.clone()),
        }]
    )
    .unwrap());

    let result = stage_node_occurrences(&config, "node", std::iter::empty());
    assert!(result.is_err());
    assert_eq!(
        versioned::read_state(&config, ENTITY_INDEX, malformed_key).unwrap(),
        Some(malformed_value)
    );
    assert!(
        versioned::read_state(&config, ENTITY_INDEX, &node_manifest_key("node"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn tree_occurrence_deletion_is_staged_and_preserves_shared_aggregate_edges() {
    let temp = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let entities = [
        crate::memory::store::entity_index::types::CanonicalEntity {
            canonical_id: "topic:alpha".into(),
            kind: crate::memory::store::entity_index::types::EntityKind::Topic,
            surface: "alpha".into(),
            span_start: 0,
            span_end: 5,
            score: 1.0,
        },
        crate::memory::store::entity_index::types::CanonicalEntity {
            canonical_id: "topic:beta".into(),
            kind: crate::memory::store::entity_index::types::EntityKind::Topic,
            surface: "beta".into(),
            span_start: 6,
            span_end: 10,
            score: 1.0,
        },
    ];
    let index =
        crate::memory::store::entity_index::EntityIndex::for_memory_config(&config).unwrap();
    index
        .index_entities(&entities, "node-one", "leaf", 1, Some("tree-one"))
        .unwrap();
    index
        .index_entities(&entities, "node-two", "leaf", 2, Some("tree-two"))
        .unwrap();
    let before = crate::memory::graph::edge_neighbors(&config, "topic:alpha").unwrap();
    assert_eq!(before, vec![("topic:beta".into(), 2)]);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let staged = runtime
        .block_on(stage_delete_tree_occurrences_async(&config, "tree-one"))
        .unwrap();
    drop(runtime);
    // A node added after planning updates the tree sentinel, so the stale
    // cascade cannot omit it or partially delete the planned nodes.
    index
        .index_entities(&entities, "node-late", "leaf", 3, Some("tree-one"))
        .unwrap();
    assert_eq!(
        index.list_entity_ids_for_node("node-one").unwrap(),
        vec!["topic:alpha", "topic:beta"]
    );
    assert!(!staged.changes.is_empty());
    assert!(
        !versioned::commit_if_state_matches(&config, &staged.expected, &[], &staged.changes)
            .unwrap()
    );
    assert_eq!(
        index.list_entity_ids_for_node("node-one").unwrap(),
        vec!["topic:alpha", "topic:beta"]
    );

    let before_delete = crate::memory::graph::edge_neighbors(&config, "topic:alpha").unwrap();
    assert_eq!(before_delete, vec![("topic:beta".into(), 3)]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let staged = runtime
        .block_on(stage_delete_tree_occurrences_async(&config, "tree-one"))
        .unwrap();
    drop(runtime);
    assert!(
        versioned::commit_if_state_matches(&config, &staged.expected, &[], &staged.changes)
            .unwrap()
    );

    assert!(index
        .list_entity_ids_for_node("node-one")
        .unwrap()
        .is_empty());
    assert_eq!(
        index.list_entity_ids_for_node("node-two").unwrap(),
        vec!["topic:alpha", "topic:beta"]
    );
    assert!(index
        .list_entity_ids_for_node("node-late")
        .unwrap()
        .is_empty());
    // Tree deletion historically removes occurrences, not aggregate graph
    // history; shared edge weights therefore remain unchanged.
    assert_eq!(
        crate::memory::graph::edge_neighbors(&config, "topic:alpha").unwrap(),
        before_delete
    );
}
