use super::*;
use crate::memory::tree::{
    registry::get_or_create_tree_async, store::get_buffer_async, ConcatSummariser,
};

#[tokio::test]
async fn direct_summary_lands_at_l1_without_creating_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let tree = get_or_create_tree_async(
        &config,
        crate::memory::tree::store::TreeKind::Source,
        "github:org/repo",
    )
    .await
    .unwrap();
    let now = Utc::now();
    let outcome = ingest_summary(
        &config,
        &tree,
        SummaryIngestInput {
            content: "summary".into(),
            token_count: 2,
            entities: Vec::new(),
            topics: Vec::new(),
            time_range_start: now,
            time_range_end: now,
            score: 0.5,
            child_labels: vec!["100_issue-1".into()],
            child_basenames: Vec::new(),
        },
        &ConcatSummariser,
    )
    .await
    .unwrap();
    let count_config = config.clone();
    let chunk_count =
        std::thread::spawn(move || crate::memory::chunks::count_chunks(&count_config))
            .join()
            .unwrap()
            .unwrap();
    assert_eq!(chunk_count, 0);
    let summary = store::get_summary_async(&config, &outcome.summary_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.level, 1);
    assert_eq!(summary.child_ids, vec!["100_issue-1"]);
    assert_eq!(
        get_buffer_async(&config, &tree.id, 1)
            .await
            .unwrap()
            .item_ids
            .len(),
        1
    );
}
