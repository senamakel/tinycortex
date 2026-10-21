//! Tests for time-based buffer flush. Adapted from OpenHuman's flush tests:
//! the chat-provider summariser is replaced by [`ConcatSummariser`], and chunks
//! are read inline (no on-disk content staging).

use super::*;
use chrono::Duration;
use tempfile::TempDir;

use crate::memory::chunks::upsert_chunks;
use crate::memory::chunks::{chunk_id, Chunk, Metadata, SourceKind, SourceRef};
use crate::memory::tree::bucket_seal::{append_leaf, LeafRef};
use crate::memory::tree::registry::get_or_create_tree_async;
use crate::memory::tree::store::{Buffer, TreeKind};
use crate::memory::tree::summarise::ConcatSummariser;

struct RejectTreeObserver {
    tree_id: String,
}

impl crate::memory::tree::bucket_seal::SealObserver for RejectTreeObserver {
    fn summary_committed(
        &self,
        tree: &crate::memory::tree::store::Tree,
        _node: &crate::memory::tree::store::SummaryNode,
        _content_path: Option<&str>,
        _reason: &str,
    ) -> anyhow::Result<()> {
        if tree.id == self.tree_id {
            anyhow::bail!("injected observer failure")
        }
        Ok(())
    }
}

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn count_summaries_outside_runtime(config: &MemoryConfig, tree_id: &str) -> u64 {
    let config = config.clone();
    let tree_id = tree_id.to_owned();
    std::thread::spawn(move || store::count_summaries(&config, &tree_id))
        .join()
        .unwrap()
        .unwrap()
}

fn buffer_outside_runtime(config: &MemoryConfig, tree_id: &str, level: u32) -> Buffer {
    let config = config.clone();
    let tree_id = tree_id.to_owned();
    std::thread::spawn(move || store::get_buffer(&config, &tree_id, level))
        .join()
        .unwrap()
        .unwrap()
}

async fn seed_buffer(config: &MemoryConfig, buffer: &Buffer) {
    let key = format!("buffer:{}:{}", buffer.tree_id, buffer.level);
    let expected = [crate::memory::chunks::versioned::StateChange {
        collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
        key: key.clone(),
        value: None,
    }];
    let changes = [crate::memory::chunks::versioned::StateChange {
        collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
        key,
        value: Some(serde_json::to_value(buffer).unwrap()),
    }];
    assert!(
        crate::memory::chunks::versioned::commit_if_state_matches_async(
            config,
            &expected,
            &[],
            &changes
        )
        .await
        .unwrap()
    );
}

fn seed_chunk(cfg: &MemoryConfig, src: &str, seq: u32, content: &str, ts: DateTime<Utc>) -> Chunk {
    let c = Chunk {
        id: chunk_id(SourceKind::Chat, src, seq, content),
        content: content.to_string(),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: src.into(),
            owner: "alice".into(),
            timestamp: ts,
            time_range: (ts, ts),
            tags: vec![],
            source_ref: Some(SourceRef::new("slack://x")),
            path_scope: None,
        },
        token_count: 100,
        seq_in_source: seq,
        created_at: ts,
        partial_message: false,
    };
    let config = cfg.clone();
    let chunk_for_write = c.clone();
    std::thread::spawn(move || {
        upsert_chunks(&config, std::slice::from_ref(&chunk_for_write)).unwrap();
    })
    .join()
    .unwrap();
    c
}

#[tokio::test]
async fn flush_seals_old_buffer_even_under_budget() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let old_ts = Utc::now() - Duration::days(10);
    let c = seed_chunk(&cfg, "slack:#eng", 0, "old content to seal", old_ts);

    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: 100,
        timestamp: old_ts,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 0);

    let seals = flush_stale_buffers(&cfg, Duration::days(7), &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(seals, 1);
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 1);
    assert!(buffer_outside_runtime(&cfg, &tree.id, 0).is_empty());
}

#[tokio::test]
async fn flush_does_not_force_seal_under_fanout_upper_buffer() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();

    let old_ts = Utc::now() - Duration::days(10);
    seed_buffer(
        &cfg,
        &Buffer {
            tree_id: tree.id.clone(),
            level: 1,
            item_ids: vec!["fake-l1-child".into()],
            token_sum: 100,
            oldest_at: Some(old_ts),
        },
    )
    .await;

    let seals = flush_stale_buffers(&cfg, Duration::days(7), &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(seals, 0, "L1 stale buffer must not be force-sealed");
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 0);
    assert_eq!(
        buffer_outside_runtime(&cfg, &tree.id, 1).item_ids,
        vec!["fake-l1-child".to_string()]
    );
}

#[tokio::test]
async fn flush_noop_when_buffer_is_recent() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let now = Utc::now();
    let c = seed_chunk(&cfg, "slack:#eng", 0, "fresh", now);
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: 50,
        timestamp: now,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();

    let seals = flush_stale_buffers(&cfg, Duration::days(7), &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(seals, 0);
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 0);
}

#[tokio::test]
async fn flush_seals_multiple_distinct_trees_via_batched_lookup() {
    let (_tmp, cfg) = test_config();
    let tree_a = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let tree_b = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#design")
        .await
        .unwrap();
    assert_ne!(tree_a.id, tree_b.id);
    let s = ConcatSummariser::new();
    let old_ts = Utc::now() - Duration::days(10);

    for (src, content, tree) in [
        ("slack:#eng", "engineering content", &tree_a),
        ("slack:#design", "design content", &tree_b),
    ] {
        let c = seed_chunk(&cfg, src, 0, content, old_ts);
        let leaf = LeafRef {
            chunk_id: c.id.clone(),
            token_count: 100,
            timestamp: old_ts,
            content: c.content.clone(),
            entities: vec![],
            topics: vec![],
            score: 0.5,
        };
        append_leaf(&cfg, tree, &leaf, &s, &LabelStrategy::Empty)
            .await
            .unwrap();
    }

    let seals = flush_stale_buffers(&cfg, Duration::days(7), &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(seals, 2);
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree_a.id), 1);
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree_b.id), 1);
}

#[tokio::test]
async fn flush_continues_after_one_tree_fails() {
    let (_tmp, cfg) = test_config();
    let tree_a = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#poison")
        .await
        .unwrap();
    let tree_b = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#healthy")
        .await
        .unwrap();
    let summariser = ConcatSummariser::new();
    let old_ts = Utc::now() - Duration::days(10);

    for (src, tree) in [("slack:#poison", &tree_a), ("slack:#healthy", &tree_b)] {
        let chunk = seed_chunk(&cfg, src, 0, src, old_ts);
        append_leaf(
            &cfg,
            tree,
            &LeafRef {
                chunk_id: chunk.id,
                token_count: 100,
                timestamp: old_ts,
                content: chunk.content,
                entities: vec![],
                topics: vec![],
                score: 0.5,
            },
            &summariser,
            &LabelStrategy::Empty,
        )
        .await
        .unwrap();
    }

    let observer = RejectTreeObserver {
        tree_id: tree_a.id.clone(),
    };
    let services = SealServices {
        summariser: &summariser,
        embedder: None,
        observer: &observer,
    };
    let err = flush_stale_buffers_with_services(
        &cfg,
        Duration::days(7),
        &services,
        &LabelStrategy::Empty,
    )
    .await
    .expect_err("one tree should report the injected failure");

    assert!(format!("{err:#}").contains("injected observer failure"));
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree_b.id), 1);
}

#[tokio::test]
async fn flush_default_noops_when_no_stale_buffers_exist() {
    let (_tmp, cfg) = test_config();
    let s = ConcatSummariser::new();

    let seals = flush_stale_buffers_default(&cfg, &s, &LabelStrategy::Empty)
        .await
        .unwrap();

    assert_eq!(seals, 0);
}

#[tokio::test]
async fn force_flush_tree_reports_missing_tree() {
    let (_tmp, cfg) = test_config();
    let s = ConcatSummariser::new();

    let err = force_flush_tree(&cfg, "missing-tree", None, &s, &LabelStrategy::Empty)
        .await
        .expect_err("missing tree should be reported");

    assert!(
        format!("{err:#}").contains("no tree with id missing-tree"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn force_flush_tree_with_none_seals_under_budget_l0() {
    // Regression (TR-3/TR-12): `force_flush_tree(.., None, ..)` — the exact call
    // shape `TreeFactory::seal_now` uses — must still force-seal an under-budget
    // L0 buffer. Before the fix the force flag was derived from `now.is_some()`,
    // so passing `None` silently made the documented force-seal a no-op.
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let now = Utc::now();
    let c = seed_chunk(&cfg, "slack:#eng", 0, "disconnect flush content", now);
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: 50,
        timestamp: now,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 0);

    let sealed = force_flush_tree(&cfg, &tree.id, None, &s, &LabelStrategy::Empty)
        .await
        .unwrap();

    assert_eq!(sealed.len(), 1, "force_flush with None must seal");
    assert!(buffer_outside_runtime(&cfg, &tree.id, 0).is_empty());
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 1);
}

#[tokio::test]
async fn force_flush_tree_seals_current_l0_buffer() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let now = Utc::now();
    let c = seed_chunk(&cfg, "slack:#eng", 0, "manual flush content", now);
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: 50,
        timestamp: now,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();

    let sealed = force_flush_tree(&cfg, &tree.id, Some(now), &s, &LabelStrategy::Empty)
        .await
        .unwrap();

    assert_eq!(sealed.len(), 1);
    assert!(buffer_outside_runtime(&cfg, &tree.id, 0).is_empty());
    assert_eq!(count_summaries_outside_runtime(&cfg, &tree.id), 1);
}
