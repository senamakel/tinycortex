//! Label-strategy, topic-kind, blank-fallback, and hydration-order tests for
//! the bucket-seal engine. Split out of `bucket_seal_tests.rs` to keep each
//! file under the 500-line cap; shares the same adaptations.

use super::*;
use std::sync::Arc;
use tempfile::TempDir;

use crate::memory::chunks::{chunk_id, Chunk, Metadata, SourceKind, SourceRef};
use crate::memory::config::MemoryConfig;
use crate::memory::store::entity_index::{CanonicalEntity, EntityIndex, EntityKind};
use crate::memory::tree::registry::get_or_create_tree_async;
use crate::memory::tree::store::{self, Tree, TreeKind, TreeStatus, INPUT_TOKEN_BUDGET};
use crate::memory::tree::summarise::{
    ConcatSummariser, Summariser, SummaryContext, SummaryInput, SummaryOutput,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

/// A summariser that always returns whitespace — exercises the blank-output
/// fallback path in `seal_one_level`.
struct BlankSummariser;
#[async_trait]
impl Summariser for BlankSummariser {
    async fn summarise(
        &self,
        _inputs: &[SummaryInput],
        _ctx: &SummaryContext<'_>,
    ) -> anyhow::Result<SummaryOutput> {
        Ok(SummaryOutput {
            content: "   \n\t  ".to_string(),
            token_count: 0,
            ..Default::default()
        })
    }
}

async fn seed_chunk(
    cfg: &MemoryConfig,
    seq: u32,
    content: &str,
    tokens: u32,
    tags: Vec<String>,
) -> Chunk {
    let ts = Utc
        .timestamp_millis_opt(1_700_000_000_000 + seq as i64)
        .unwrap();
    let c = Chunk {
        id: chunk_id(SourceKind::Chat, "slack:#eng", seq, content),
        content: content.to_string(),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: "slack:#eng".into(),
            owner: "alice".into(),
            timestamp: ts,
            time_range: (ts, ts),
            tags,
            source_ref: Some(SourceRef::new("slack://x")),
            path_scope: None,
        },
        token_count: tokens,
        seq_in_source: seq,
        created_at: ts,
        partial_message: false,
    };
    crate::memory::chunks::upsert_chunks_async(cfg, std::slice::from_ref(&c))
        .await
        .unwrap();
    c
}

async fn index_leaf_entities(cfg: &MemoryConfig, chunk_id: &str, entities: &[String]) {
    let ts = 1_700_000_000_000;
    let canonical = entities
        .iter()
        .map(|entity_id| {
            let (kind, surface) = entity_id
                .split_once(':')
                .map(|(k, v)| (EntityKind::parse(k).unwrap_or(EntityKind::Misc), v))
                .unwrap_or((EntityKind::Misc, entity_id.as_str()));
            CanonicalEntity {
                canonical_id: entity_id.clone(),
                kind,
                surface: surface.to_string(),
                span_start: 0,
                span_end: surface.len() as u32,
                score: 1.0,
            }
        })
        .collect::<Vec<_>>();
    EntityIndex::for_memory_config(cfg)
        .unwrap()
        .index_entities_async(&canonical, chunk_id, "leaf", ts, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn seal_with_extract_strategy_populates_entities_and_topics() {
    use crate::memory::score::extract::CompositeExtractor;

    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();

    // ConcatSummariser echoes input content, so the summary text carries the
    // email + hashtag the regex extractor then finds.
    let content = "alice@example.com is leading the #launch sprint this week.";
    let c = seed_chunk(&cfg, 0, content, INPUT_TOKEN_BUDGET + 1, vec![]).await;
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: c.token_count,
        timestamp: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };

    let strategy = LabelStrategy::ExtractFromContent(Arc::new(CompositeExtractor::regex_only()));
    let sealed = append_leaf(&cfg, &tree, &leaf, &s, &strategy)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);

    let summary = store::get_summary_async(&cfg, &sealed[0])
        .await
        .unwrap()
        .unwrap();
    assert!(
        summary
            .entities
            .iter()
            .any(|e| e == "email:alice@example.com"),
        "got entities={:?}",
        summary.entities
    );
    assert!(
        summary.topics.iter().any(|t| t == "launch"),
        "got topics={:?}",
        summary.topics
    );
}

#[tokio::test]
async fn seal_with_union_strategy_inherits_labels_from_children() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let per_leaf = INPUT_TOKEN_BUDGET / 2;

    let c1 = seed_chunk(
        &cfg,
        0,
        "first leaf body",
        per_leaf,
        vec!["phoenix".into(), "launch".into()],
    )
    .await;
    index_leaf_entities(
        &cfg,
        &c1.id,
        &["email:alice@example.com".into(), "topic:phoenix".into()],
    )
    .await;
    let c2 = seed_chunk(
        &cfg,
        1,
        "second leaf body",
        per_leaf,
        vec!["launch".into(), "qa".into()],
    )
    .await;
    index_leaf_entities(
        &cfg,
        &c2.id,
        &["email:alice@example.com".into(), "person:bob".into()],
    )
    .await;

    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let mk = |c: &Chunk| LeafRef {
        chunk_id: c.id.clone(),
        token_count: per_leaf,
        timestamp: ts,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };

    assert!(
        append_leaf(&cfg, &tree, &mk(&c1), &s, &LabelStrategy::UnionFromChildren)
            .await
            .unwrap()
            .is_empty()
    );
    let sealed = append_leaf(&cfg, &tree, &mk(&c2), &s, &LabelStrategy::UnionFromChildren)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);

    let summary = store::get_summary_async(&cfg, &sealed[0])
        .await
        .unwrap()
        .unwrap();
    let entities: std::collections::BTreeSet<&str> =
        summary.entities.iter().map(String::as_str).collect();
    let topics: std::collections::BTreeSet<&str> =
        summary.topics.iter().map(String::as_str).collect();
    assert!(entities.contains("email:alice@example.com"));
    assert!(entities.contains("topic:phoenix"));
    assert!(entities.contains("person:bob"));
    assert_eq!(entities.len(), 3, "got {entities:?}");
    assert!(topics.contains("phoenix"));
    assert!(topics.contains("launch"));
    assert!(topics.contains("qa"));
    assert_eq!(topics.len(), 3, "got {topics:?}");
}

#[tokio::test]
async fn seal_with_empty_strategy_leaves_labels_empty() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let s = ConcatSummariser::new();
    let c = seed_chunk(
        &cfg,
        0,
        "alice@example.com discussing #launch",
        INPUT_TOKEN_BUDGET + 1,
        vec!["launch".into()],
    )
    .await;
    index_leaf_entities(&cfg, &c.id, &["email:alice@example.com".into()]).await;
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: c.token_count,
        timestamp: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    let sealed = append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);
    let summary = store::get_summary_async(&cfg, &sealed[0])
        .await
        .unwrap()
        .unwrap();
    assert!(summary.entities.is_empty(), "got {:?}", summary.entities);
    assert!(summary.topics.is_empty(), "got {:?}", summary.topics);
}

#[tokio::test]
async fn topic_tree_seal_persists_topic_kind_not_source() {
    let (_tmp, cfg) = test_config();
    let tree = Tree {
        id: "topic-tree-test-id".to_string(),
        kind: TreeKind::Topic,
        scope: "topic:launch".to_string(),
        root_id: None,
        max_level: 0,
        status: TreeStatus::Active,
        created_at: Utc::now(),
        last_sealed_at: None,
        ask: None,
    };
    store::insert_tree_async(&cfg, &tree).await.unwrap();
    let s = ConcatSummariser::new();
    let c = seed_chunk(&cfg, 0, "topic content", INPUT_TOKEN_BUDGET + 1, vec![]).await;
    let leaf = LeafRef {
        chunk_id: c.id.clone(),
        token_count: c.token_count,
        timestamp: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };
    let sealed = append_leaf(&cfg, &tree, &leaf, &s, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);
    let summary = store::get_summary_async(&cfg, &sealed[0])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.tree_kind, TreeKind::Topic);
}

#[tokio::test]
async fn whitespace_summary_falls_back_to_deterministic_content() {
    let (_tmp, cfg) = test_config();
    let tree = get_or_create_tree_async(&cfg, TreeKind::Source, "slack:#eng")
        .await
        .unwrap();
    let blank = BlankSummariser;

    let per_leaf = INPUT_TOKEN_BUDGET * 6 / 10;
    let c1 = seed_chunk(&cfg, 0, "non-empty leaf content 0", per_leaf, vec![]).await;
    let c2 = seed_chunk(&cfg, 1, "non-empty leaf content 1", per_leaf, vec![]).await;
    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let mk = |c: &Chunk| LeafRef {
        chunk_id: c.id.clone(),
        token_count: per_leaf,
        timestamp: ts,
        content: c.content.clone(),
        entities: vec![],
        topics: vec![],
        score: 0.5,
    };

    append_leaf(&cfg, &tree, &mk(&c1), &blank, &LabelStrategy::Empty)
        .await
        .unwrap();
    let sealed = append_leaf(&cfg, &tree, &mk(&c2), &blank, &LabelStrategy::Empty)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);
    let summary = store::get_summary_async(&cfg, &sealed[0])
        .await
        .unwrap()
        .unwrap();
    assert!(
        !summary.content.trim().is_empty(),
        "blank summariser must fall back to deterministic content"
    );
    assert!(summary.content.contains("non-empty leaf content 0"));
    assert!(summary.content.contains("non-empty leaf content 1"));
}

#[tokio::test]
async fn hydrate_summary_inputs_preserves_order_and_skips_missing() {
    use crate::memory::tree::hydrate::hydrate_summary_inputs_async;

    let (_tmp, cfg) = test_config();
    let ts = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let tree = Tree {
        id: "tree-hydrate".into(),
        kind: TreeKind::Source,
        scope: "slack:#eng".into(),
        root_id: None,
        max_level: 0,
        status: TreeStatus::Active,
        created_at: ts,
        last_sealed_at: None,
        ask: None,
    };
    store::insert_tree_async(&cfg, &tree).await.unwrap();

    let mk = |id: &str, content: &str, tokens: u32, score: f32, entity: &str| store::SummaryNode {
        id: id.into(),
        tree_id: tree.id.clone(),
        tree_kind: TreeKind::Source,
        level: 1,
        parent_id: None,
        child_ids: vec!["leaf".into()],
        content: content.into(),
        token_count: tokens,
        entities: vec![entity.into()],
        topics: vec![],
        time_range_start: ts,
        time_range_end: ts,
        score,
        sealed_at: ts,
        deleted: false,
        embedding: None,
        doc_id: None,
        version_ms: None,
    };
    let sum_a = mk("sum-a", "BODY-A", 11, 0.11, "entity:alice");
    let full_body_b = "B".repeat(700);
    let sum_b = mk("sum-b", &full_body_b, 22, 0.22, "entity:bob");
    let staged_a = store::stage_summary_changes(&sum_a, None, "test").unwrap();
    let staged_b = store::stage_summary_changes(&sum_b, None, "test").unwrap();
    let mut expected = staged_a.expected;
    expected.extend(staged_b.expected);
    let mut changes = staged_a.changes;
    changes.extend(staged_b.changes);
    assert!(
        crate::memory::chunks::versioned::commit_if_state_matches_async(
            &cfg,
            &expected,
            &[],
            &changes,
        )
        .await
        .unwrap()
    );

    let ids = vec!["sum-b".into(), "ghost:no-such".into(), "sum-a".into()];
    let out = hydrate_summary_inputs_async(&cfg, &ids).await.unwrap();
    assert_eq!(out.len(), 2, "ghost id must be skipped");
    assert_eq!(out[0].id, "sum-b");
    assert_eq!(out[1].id, "sum-a");
    assert_eq!(out[0].token_count, 22);
    assert_eq!(out[0].content, full_body_b);
    assert_eq!(out[1].content, "BODY-A");
    assert_eq!(out[0].entities, vec!["entity:bob".to_string()]);
}
