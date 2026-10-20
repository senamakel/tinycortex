use super::*;
use crate::memory::chunks::versioned::commit_if_state_matches_at_snapshot_async;
use crate::memory::chunks::{collections, test_support::host_config};
use crate::memory::tree::registry::get_or_create_tree_async;
use crate::memory::tree::store::{append_buffer_item_async, get_buffer_async};
use tempfile::TempDir;

#[tokio::test(flavor = "current_thread")]
async fn tree_delete_replans_when_append_advances_the_enumerated_head() {
    let tmp = TempDir::new().unwrap();
    let config = host_config(tmp.path());
    let tree = get_or_create_tree_async(&config, TreeKind::Source, "delete-race")
        .await
        .unwrap();
    append_buffer_item_async(
        &config,
        &tree.id,
        0,
        "chunk-before-delete",
        4,
        chrono::Utc::now(),
    )
    .await
    .unwrap();

    let stale_plan = stage_tree_cascade_deletion_async(&config, &tree.id)
        .await
        .unwrap()
        .unwrap();
    append_buffer_item_async(
        &config,
        &tree.id,
        0,
        "chunk-raced-delete",
        7,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    assert!(!commit_if_state_matches_at_snapshot_async(
        &config,
        &stale_plan.snapshot,
        &stale_plan.expected,
        &[],
        &stale_plan.changes,
    )
    .await
    .unwrap());

    delete_tree_cascade_async(&config, &tree.id).await.unwrap();
    assert!(get_tree_async(&config, &tree.id).await.unwrap().is_none());
    assert!(get_buffer_async(&config, &tree.id, 0)
        .await
        .unwrap()
        .is_empty());
    assert!(
        crate::memory::chunks::versioned::list_state_async(&config, collections::BUFFERS)
            .await
            .unwrap()
            .iter()
            .all(
                |(_, value)| value.get("tree_id").and_then(serde_json::Value::as_str)
                    != Some(tree.id.as_str())
            )
    );
}
