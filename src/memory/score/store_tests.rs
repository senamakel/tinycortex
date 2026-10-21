use super::*;
use crate::memory::chunks::test_support::host_config;
use crate::memory::chunks::{upsert_chunks, upsert_chunks_async, Chunk, Metadata, SourceKind};
use crate::memory::config::MemoryConfig;
use chrono::{TimeZone, Utc};
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = host_config(tmp.path());
    (tmp, cfg)
}

fn sample_row(id: &str, dropped: bool) -> ScoreRow {
    ScoreRow {
        chunk_id: id.to_string(),
        total: 0.7,
        signals: ScoreSignals {
            token_count: 1.0,
            unique_words: 0.8,
            metadata_weight: 0.9,
            source_weight: 0.5,
            interaction: 0.6,
            entity_density: 0.3,
            llm_importance: 0.0,
        },
        dropped,
        reason: if dropped {
            Some("below threshold".into())
        } else {
            None
        },
        computed_at_ms: 1_700_000_000_000,
        llm_importance_reason: None,
    }
}

fn sample_entity(id: &str) -> CanonicalEntity {
    CanonicalEntity {
        canonical_id: format!("email:{id}"),
        kind: EntityKind::Email,
        surface: format!("{id}@example.com"),
        span_start: 0,
        span_end: (id.len() + 12) as u32,
        score: 1.0,
    }
}

fn sample_chunk(id: &str) -> Chunk {
    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    Chunk {
        id: id.to_owned(),
        content: "chunk fixture".into(),
        metadata: Metadata::point_in_time(SourceKind::Chat, "room", "owner", timestamp),
        token_count: 2,
        seq_in_source: 0,
        created_at: timestamp,
        partial_message: false,
    }
}

#[test]
fn upsert_then_get_score() {
    let (_tmp, cfg) = test_config();
    let row = sample_row("c1", false);
    upsert_score(&cfg, &row).unwrap();
    let got = get_score(&cfg, "c1").unwrap().expect("row exists");
    assert_eq!(got.chunk_id, row.chunk_id);
    assert!((got.total - row.total).abs() < 1e-6);
    assert_eq!(got.dropped, row.dropped);
    assert_eq!(got.reason, row.reason);
    assert_eq!(got.computed_at_ms, row.computed_at_ms);
    assert!((got.signals.token_count - row.signals.token_count).abs() < 1e-6);
}

#[test]
fn upsert_score_idempotent() {
    let (_tmp, cfg) = test_config();
    let r = sample_row("c1", false);
    upsert_score(&cfg, &r).unwrap();
    upsert_score(&cfg, &r).unwrap();
    assert_eq!(count_scores(&cfg).unwrap(), 1);
}

#[tokio::test]
async fn queue_score_planner_returns_owner_mutations_without_persisting_them() {
    let (_tmp, cfg) = test_config();
    let row = sample_row("planned-chunk", false);
    upsert_chunks_async(&cfg, &[sample_chunk(&row.chunk_id)])
        .await
        .unwrap();
    let result = crate::memory::score::ScoreResult {
        chunk_id: row.chunk_id.clone(),
        total: row.total,
        signals: row.signals,
        kept: true,
        drop_reason: None,
        extracted: Default::default(),
        canonical_entities: vec![sample_entity("alice")],
    };

    let mutations = plan_score_persistence_for_queue(&cfg, &result, 123, Some("source"))
        .await
        .unwrap();

    assert!(mutations.iter().any(|mutation| {
        mutation.collection == crate::memory::queue::JobStateCollection::ChunkScores
            && mutation.key == "planned-chunk"
    }));
    assert!(mutations.iter().any(|mutation| {
        mutation.collection == crate::memory::queue::JobStateCollection::EntityIndex
            && mutation.value.is_some()
    }));
    assert!(get_score_async(&cfg, "planned-chunk")
        .await
        .unwrap()
        .is_none());
    assert!(list_entity_ids_for_node_async(&cfg, "planned-chunk")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn queue_score_planner_refuses_missing_or_deleted_chunks() {
    let (_tmp, cfg) = test_config();
    let row = sample_row("missing-chunk", false);
    let result = crate::memory::score::ScoreResult {
        chunk_id: row.chunk_id.clone(),
        total: row.total,
        signals: row.signals.clone(),
        kept: true,
        drop_reason: None,
        extracted: Default::default(),
        canonical_entities: vec![],
    };
    assert!(
        plan_score_persistence_for_queue(&cfg, &result, 123, Some("source"))
            .await
            .is_err()
    );

    upsert_chunks_async(&cfg, &[sample_chunk(&row.chunk_id)])
        .await
        .unwrap();
    crate::memory::chunks::set_chunk_lifecycle_status(&cfg, &row.chunk_id, "deleted").unwrap();
    assert!(
        plan_score_persistence_for_queue(&cfg, &result, 123, Some("source"))
            .await
            .is_err()
    );
}

#[test]
fn dropped_flag_persists() {
    let (_tmp, cfg) = test_config();
    let r = sample_row("c1", true);
    upsert_score(&cfg, &r).unwrap();
    let got = get_score(&cfg, "c1").unwrap().unwrap();
    assert!(got.dropped);
    assert_eq!(got.reason.as_deref(), Some("below threshold"));
}

#[test]
fn get_missing_score_is_none() {
    let (_tmp, cfg) = test_config();
    assert!(get_score(&cfg, "missing").unwrap().is_none());
}

#[test]
fn index_and_lookup_entity() {
    let (_tmp, cfg) = test_config();
    let e = sample_entity("alice");
    index_entity(&cfg, &e, "chunk-1", "leaf", 1000, Some("source:chat")).unwrap();
    index_entity(&cfg, &e, "chunk-2", "leaf", 2000, Some("source:chat")).unwrap();

    let hits = lookup_entity(&cfg, "email:alice", None).unwrap();
    assert_eq!(hits.len(), 2);
    // newest first
    assert_eq!(hits[0].node_id, "chunk-2");
    assert_eq!(hits[1].node_id, "chunk-1");
}

#[test]
fn index_batch() {
    let (_tmp, cfg) = test_config();
    let entities = vec![sample_entity("a"), sample_entity("b"), sample_entity("c")];
    let n = index_entities(&cfg, &entities, "chunk-1", "leaf", 1000, None).unwrap();
    assert_eq!(n, 3);
    assert_eq!(count_entity_index(&cfg).unwrap(), 3);
    assert_eq!(crate::memory::graph::count_edges(&cfg).unwrap(), 3);
}

#[test]
fn clear_entity_index_drops_stale_rows() {
    let (_tmp, cfg) = test_config();
    let a = sample_entity("a");
    let b = sample_entity("b");
    index_entities(&cfg, &[a.clone(), b], "chunk-1", "leaf", 1000, None).unwrap();
    assert_eq!(count_entity_index(&cfg).unwrap(), 2);

    // Simulate a re-score that only keeps entity "a".
    let cleared = clear_entity_index_for_node(&cfg, "chunk-1").unwrap();
    assert_eq!(cleared, 2);
    index_entities(&cfg, &[a], "chunk-1", "leaf", 1000, None).unwrap();

    let hits = lookup_entity(&cfg, "email:b", None).unwrap();
    assert!(hits.is_empty(), "stale entity should be removed");
    let hits = lookup_entity(&cfg, "email:a", None).unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn index_idempotent_per_entity_node_pair() {
    let (_tmp, cfg) = test_config();
    let e = sample_entity("alice");
    index_entity(&cfg, &e, "chunk-1", "leaf", 1000, None).unwrap();
    index_entity(&cfg, &e, "chunk-1", "leaf", 1000, None).unwrap();
    assert_eq!(count_entity_index(&cfg).unwrap(), 1);
}

#[test]
fn lookup_limit_respected() {
    let (_tmp, cfg) = test_config();
    let e = sample_entity("alice");
    for i in 0..5 {
        index_entity(
            &cfg,
            &e,
            &format!("chunk-{i}"),
            "leaf",
            1000 + i as i64,
            None,
        )
        .unwrap();
    }
    let hits = lookup_entity(&cfg, "email:alice", Some(2)).unwrap();
    assert_eq!(hits.len(), 2);
}

/// Regression: summary indexing must write a parseable
/// `entity_kind` (the "<kind>" prefix before `:`) so `lookup_entity` can still
/// round-trip rows through `EntityKind::parse`. Earlier code stored the full
/// canonical id, which poisoned lookups mixing leaf and summary hits.
#[test]
fn summary_entity_index_kind_is_parseable() {
    let (_tmp, cfg) = test_config();

    // Seed a leaf hit so lookup_entity has something leafy to mix with the
    // summary hit — this reproduces the mixed-row crash.
    let leaf_entity = sample_entity("alice");
    index_entity(&cfg, &leaf_entity, "leaf-1", "leaf", 1000, Some("tree-1")).unwrap();

    let n = index_summary_entity_ids(
        &cfg,
        &["email:alice@example.com".into(), "hashtag:launch-q2".into()],
        "summary-1",
        0.84,
        2000,
        Some("tree-1"),
    )
    .unwrap();
    assert_eq!(n, 2);

    // The column stores "email" and the lookup succeeds with both rows.
    let hits = lookup_entity(&cfg, "email:alice@example.com", None).unwrap();
    assert_eq!(hits.len(), 1, "summary row should be discoverable");
    assert_eq!(hits[0].node_id, "summary-1");
    assert_eq!(hits[0].node_kind, "summary");
    assert_eq!(hits[0].entity_kind, EntityKind::Email);

    // Hashtag row parses as its own kind too.
    let hits = lookup_entity(&cfg, "hashtag:launch-q2", None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].entity_kind, EntityKind::Hashtag);

    // Mixing leaf + summary entity ids in one lookup also parses cleanly.
    let hits = lookup_entity(&cfg, "email:alice", None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].entity_kind, EntityKind::Email);
}

// ---------- get_scores_batch ----------

#[test]
fn get_scores_batch_returns_present_chunk_ids() {
    let (_tmp, cfg) = test_config();
    let r1 = sample_row("c1", false);
    let mut r2 = sample_row("c2", false);
    r2.total = 0.3;
    upsert_score(&cfg, &r1).unwrap();
    upsert_score(&cfg, &r2).unwrap();

    let ids = vec!["c1".to_string(), "c2".to_string()];
    let map = get_scores_batch(&cfg, &ids).unwrap();
    assert_eq!(map.len(), 2);
    assert!((map.get("c1").copied().unwrap() - 0.7).abs() < 1e-6);
    assert!((map.get("c2").copied().unwrap() - 0.3).abs() < 1e-6);
}

#[test]
fn get_scores_batch_empty_input_and_missing_chunk_ids() {
    // Empty input: empty map (no SQL issued).
    let (_tmp, cfg) = test_config();
    let empty = get_scores_batch(&cfg, &[]).unwrap();
    assert!(empty.is_empty());

    // Missing ids: silently absent so callers can fall back to a 0.0 neutral.
    let r = sample_row("c1", false);
    upsert_score(&cfg, &r).unwrap();
    let ids = vec!["c1".to_string(), "ghost:no-such".to_string()];
    let map = get_scores_batch(&cfg, &ids).unwrap();
    assert_eq!(map.len(), 1);
    assert!((map.get("c1").copied().unwrap() - 0.7).abs() < 1e-6);
    assert!(!map.contains_key("ghost:no-such"));
}

#[test]
fn staged_score_and_entities_commit_under_one_owner_head() {
    let (_tmp, cfg) = test_config();
    let row = sample_row("tx-chunk", false);
    let entities = vec![sample_entity("alice"), sample_entity("bob")];
    let staged = stage_score_and_entities(&cfg, &row, &entities, true, 42, Some("tree")).unwrap();

    // Building the transaction is side-effect free: neither its score row nor
    // its entity occurrences/edges are visible before the owner-head CAS.
    assert!(get_score(&cfg, "tx-chunk").unwrap().is_none());
    assert!(list_entity_ids_for_node(&cfg, "tx-chunk")
        .unwrap()
        .is_empty());
    assert_eq!(crate::memory::graph::count_edges(&cfg).unwrap(), 0);

    // A concurrent writer to the same node invalidates the captured manifest
    // and edge expectations. The stale transaction must publish none of its
    // score or entity changes.
    let concurrent = sample_entity("carol");
    index_entities(&cfg, &[concurrent], "tx-chunk", "leaf", 41, Some("tree")).unwrap();
    assert!(!crate::memory::chunks::versioned::commit_if_state_matches(
        &cfg,
        &staged.expected,
        &[],
        &staged.changes
    )
    .unwrap());
    assert!(get_score(&cfg, "tx-chunk").unwrap().is_none());
    assert_eq!(
        list_entity_ids_for_node(&cfg, "tx-chunk").unwrap(),
        vec!["email:carol"]
    );

    // Retake the snapshot and atomically publish the score and replacement
    // entity set. The previously committed occurrence is removed in the same
    // CAS as the new occurrences and score row.
    let staged = stage_score_and_entities(&cfg, &row, &entities, true, 42, Some("tree")).unwrap();
    assert!(crate::memory::chunks::versioned::commit_if_state_matches(
        &cfg,
        &staged.expected,
        &[],
        &staged.changes
    )
    .unwrap());

    assert_eq!(get_score(&cfg, "tx-chunk").unwrap().unwrap().total, 0.7);
    assert_eq!(
        list_entity_ids_for_node(&cfg, "tx-chunk").unwrap(),
        vec!["email:alice", "email:bob"]
    );
    assert_eq!(crate::memory::graph::count_edges(&cfg).unwrap(), 1);
}

#[test]
fn empty_entity_batches_are_noops() {
    let (_tmp, cfg) = test_config();
    assert_eq!(
        index_entities(&cfg, &[], "node", "leaf", 0, None).unwrap(),
        0
    );
    assert_eq!(
        index_summary_entity_ids(&cfg, &[], "node", 0.0, 0, None).unwrap(),
        0
    );
}

#[test]
fn score_batch_reads_many_owner_records() {
    let (_tmp, cfg) = test_config();
    let ids: Vec<String> = (0..300).map(|index| format!("chunk-{index}")).collect();
    for id in &ids {
        upsert_score(&cfg, &sample_row(id, false)).unwrap();
    }

    let scores = get_scores_batch(&cfg, &ids).unwrap();

    assert_eq!(scores.len(), ids.len());
    assert!(ids.iter().all(|id| scores.contains_key(id)));
}

#[test]
fn summary_entity_without_colon_uses_whole_id_as_kind_and_surfaces_parse_error() {
    let (_tmp, cfg) = test_config();
    index_summary_entity_ids(&cfg, &["unknown".into()], "summary", 1.0, 1, None).unwrap();

    assert!(lookup_entity(&cfg, "unknown", None).is_err());
}
