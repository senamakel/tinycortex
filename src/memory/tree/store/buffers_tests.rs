use super::*;

use chrono::{TimeZone, Utc};
use tempfile::TempDir;

use crate::memory::chunks::test_support::host_config;

fn active_tree(id: &str, scope: &str) -> super::super::types::Tree {
    super::super::types::Tree {
        id: id.to_owned(),
        kind: super::super::types::TreeKind::Source,
        scope: scope.to_owned(),
        root_id: None,
        max_level: 0,
        status: super::super::types::TreeStatus::Active,
        created_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        last_sealed_at: None,
        ask: None,
    }
}

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let config = host_config(tmp.path());
    (tmp, config)
}

#[test]
fn append_buffer_item_is_idempotent_and_preserves_oldest_timestamp() {
    let (_tmp, config) = test_config();
    let tree = active_tree("tree-append", "source:append");
    crate::memory::tree::store::insert_tree(&config, &tree).unwrap();
    let later = Utc.timestamp_millis_opt(1_700_000_000_200).unwrap();
    let earlier = Utc.timestamp_millis_opt(1_700_000_000_100).unwrap();

    append_buffer_item(&config, &tree.id, 0, "chunk-a", 17, later).unwrap();
    append_buffer_item(&config, &tree.id, 0, "chunk-a", 17, earlier).unwrap();
    append_buffer_item(&config, &tree.id, 0, "chunk-b", 23, earlier).unwrap();

    let buffer = get_buffer(&config, &tree.id, 0).unwrap();
    assert_eq!(buffer.item_ids, ["chunk-a", "chunk-b"]);
    assert_eq!(buffer.token_sum, 40);
    assert_eq!(buffer.oldest_at, Some(earlier));
}

#[test]
fn append_buffer_item_rechecks_archived_tree_and_never_creates_orphan_buffer() {
    let (_tmp, config) = test_config();
    let tree = active_tree("tree-archived", "source:archived");
    crate::memory::tree::store::insert_tree(&config, &tree).unwrap();
    crate::memory::tree::store::archive_tree(&config, &tree.id).unwrap();

    let error =
        append_buffer_item(&config, &tree.id, 0, "chunk-late", 1, tree.created_at).unwrap_err();

    assert!(error.to_string().contains("archived"));
    assert!(get_buffer(&config, &tree.id, 0)
        .unwrap()
        .item_ids
        .is_empty());
}

#[test]
fn tree_scope_and_batch_reads_resolve_from_the_owner_snapshot() {
    let (_tmp, config) = test_config();
    let source = active_tree("tree-snapshot", "source:one");
    let other = active_tree("tree-other", "source:two");
    crate::memory::tree::store::insert_tree(&config, &source).unwrap();
    crate::memory::tree::store::insert_tree(&config, &other).unwrap();

    assert_eq!(
        crate::memory::tree::store::get_tree_by_scope(
            &config,
            super::super::types::TreeKind::Source,
            "source:one",
        )
        .unwrap(),
        Some(source.clone())
    );
    let trees = crate::memory::tree::store::get_trees_batch(
        &config,
        &[source.id.clone(), other.id.clone(), "missing".to_owned()],
    )
    .unwrap();
    assert_eq!(trees.len(), 2);
    assert_eq!(trees.get(&source.id), Some(&source));
    assert_eq!(trees.get(&other.id), Some(&other));
}

#[test]
fn stale_buffer_scan_returns_only_expired_nonempty_l0_buffers_in_time_order() {
    let (_tmp, config) = test_config();
    let first = active_tree("tree-stale-a", "source:stale-a");
    let second = active_tree("tree-stale-b", "source:stale-b");
    crate::memory::tree::store::insert_tree(&config, &first).unwrap();
    crate::memory::tree::store::insert_tree(&config, &second).unwrap();
    let old = Utc.timestamp_millis_opt(1_699_999_999_000).unwrap();
    let newer = Utc.timestamp_millis_opt(1_700_000_001_000).unwrap();
    append_buffer_item(&config, &first.id, 0, "old", 1, old).unwrap();
    append_buffer_item(&config, &first.id, 1, "upper", 1, old).unwrap();
    append_buffer_item(&config, &second.id, 0, "new", 1, newer).unwrap();

    let stale = list_stale_buffers(&config, first.created_at).unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].tree_id, first.id);
    assert_eq!(stale[0].level, 0);
}
