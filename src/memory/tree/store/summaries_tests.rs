use super::*;

use chrono::{TimeZone, Utc};
use tempfile::TempDir;

use crate::memory::chunks::test_support::host_config;
use crate::memory::tree::store::TreeKind;

fn summary(id: &str) -> SummaryNode {
    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    SummaryNode {
        id: id.to_owned(),
        tree_id: "tree-summary".to_owned(),
        tree_kind: TreeKind::Source,
        level: 1,
        parent_id: None,
        child_ids: vec!["chunk-a".to_owned(), "chunk-b".to_owned()],
        content: "A committed summary body".to_owned(),
        token_count: 6,
        entities: vec!["entity:alpha".to_owned()],
        topics: vec!["#test".to_owned()],
        time_range_start: timestamp,
        time_range_end: timestamp,
        score: 0.75,
        sealed_at: timestamp,
        deleted: false,
        embedding: Some(vec![0.1, 0.2, 0.3]),
        doc_id: None,
        version_ms: None,
    }
}

#[test]
fn staged_summary_and_sidecars_share_one_owner_commit() {
    let tmp = TempDir::new().unwrap();
    let config = host_config(tmp.path());
    let node = summary("summary-staged");
    let staged = StagedSummary {
        summary_id: node.id.clone(),
        content_path: "source/tree/summary-staged.md".to_owned(),
        content_sha256: "a".repeat(64),
    };
    let plan = stage_summary_changes(&node, Some(&staged), "local/model@3").unwrap();
    assert_eq!(plan.expected.len(), 3);
    assert_eq!(plan.changes.len(), 3);
    assert!(crate::memory::chunks::versioned::commit_if_state_matches(
        &config,
        &plan.expected,
        &[],
        &plan.changes,
    )
    .unwrap());

    let mut stored_node = node.clone();
    stored_node.embedding = None;
    assert_eq!(get_summary(&config, &node.id).unwrap(), Some(stored_node));
    assert_eq!(
        get_summary_embedding_for_signature(&config, &node.id, "local/model@3").unwrap(),
        node.embedding
    );
    let pointer = crate::memory::chunks::versioned::read_state(
        &config,
        crate::memory::chunks::collections::SUMMARY_CONTENT_POINTERS,
        &content_pointer_key(&node.id),
    )
    .unwrap()
    .unwrap();
    assert_eq!(pointer["content_path"], staged.content_path);

    assert!(!crate::memory::chunks::versioned::commit_if_state_matches(
        &config,
        &plan.expected,
        &[],
        &plan.changes,
    )
    .unwrap());
}
