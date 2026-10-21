#![allow(unused_imports)]
//! Unit tests for the chunk store (`super`) — upsert / list / lifecycle /
//! embedding / delete / migration accessors against a tempdir-backed SQLite
//! store.
//!
//! Tests use unique tempdirs so parallel cache checks cannot collide. Tests
//! that must force a reopen reset only their own workspace path.

use super::connection::{
    clear_connection_cache_for, get_or_init_connection, invalidate_connection,
    schema_apply_count_for_path_for_tests, with_connection, CB_THRESHOLD,
};
use super::embeddings::{active_embedding_dims, embedding_to_blob};
use super::migrations::purge_global_topic_trees;
use super::recovery::{is_transient_cold_start, try_cleanup_stale_files};
use super::types::{chunk_id, Chunk, Metadata, SourceKind, SourceRef};
use super::{
    claim_source_ingest_tx, clear_chunk_reembed_skipped, clear_reembed_skipped_for_signature,
    clear_summary_reembed_skipped, content_root, count_chunks, db_path_for, delete_chunks_by_owner,
    delete_chunks_by_source, extraction_coverage, get_chunk, get_chunk_embedding,
    get_chunk_embedding_for_signature, get_chunk_embeddings_for_signature_batch, get_chunks_batch,
    has_uncovered_reembed_work, is_source_ingested, list_chunks, mark_chunk_reembed_skipped,
    mark_summary_reembed_skipped, set_chunk_embedding, set_chunk_embedding_for_signature,
    tree_active_signature, upsert_chunks, ListChunksQuery, DB_DIR,
    GLOBAL_TOPIC_PURGE_MIGRATION_VERSION,
};
use crate::memory::chunks::collections::{CHUNK_REEMBED_SKIPS, SUMMARY_REEMBED_SKIPS};
use crate::memory::chunks::versioned;
use crate::memory::config::MemoryConfig;
use crate::memory::store::content::read::{
    LEGACY_EMPTY_CHUNK_CONTENT_REASON_PREFIX, LEGACY_EMPTY_CONTENT_POINTER_REASON_PREFIX,
    LEGACY_NO_CONTENT_POINTER_REASON_PREFIX,
};
use chrono::{TimeZone, Utc};
use rusqlite::params;
use std::sync::Arc;
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = super::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn sample_chunk(source_id: &str, seq: u32, ts_ms: i64) -> Chunk {
    let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
    Chunk {
        id: chunk_id(SourceKind::Chat, source_id, seq, "test-content"),
        content: format!("content {source_id} {seq}"),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: source_id.to_string(),
            owner: "alice@example.com".to_string(),
            timestamp: ts,
            time_range: (ts, ts),
            tags: vec!["eng".into()],
            source_ref: Some(SourceRef::new(format!("slack://{source_id}/{seq}"))),
            path_scope: None,
        },
        token_count: 12,
        seq_in_source: seq,
        created_at: ts,
        partial_message: false,
    }
}

#[test]
fn clear_chunk_reembed_skipped_is_idempotent() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = tree_active_signature(&cfg);
    mark_chunk_reembed_skipped(&cfg, &c.id, &sig, "test orphan").unwrap();
    clear_chunk_reembed_skipped(&cfg, &c.id, &sig).unwrap();
    clear_chunk_reembed_skipped(&cfg, &c.id, &sig).unwrap();
    let count: i64 = with_connection(&cfg, |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM mem_tree_chunk_reembed_skipped
              WHERE chunk_id = ?1 AND model_signature = ?2",
            params![c.id, sig],
            |r| r.get(0),
        )?)
    })
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn setting_chunk_embedding_clears_matching_skip_marker() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = tree_active_signature(&cfg);
    mark_chunk_reembed_skipped(&cfg, &c.id, &sig, "body read failed: no content pointer").unwrap();

    set_chunk_embedding_for_signature(&cfg, &c.id, &sig, &[0.1, 0.2]).unwrap();

    let count: i64 = with_connection(&cfg, |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM mem_tree_chunk_reembed_skipped
              WHERE chunk_id = ?1 AND model_signature = ?2",
            params![c.id, sig],
            |r| r.get(0),
        )?)
    })
    .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn summary_reembed_tombstone_roundtrips_and_clears() {
    let (_tmp, cfg) = test_config();
    mark_summary_reembed_skipped(&cfg, "summary-1", "model@3", "oversize").unwrap();
    let stored = versioned::list_state(&cfg, SUMMARY_REEMBED_SKIPS).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].1["summary_id"], "summary-1");
    assert_eq!(stored[0].1["model_signature"], "model@3");
    assert_eq!(stored[0].1["reason"], "oversize");
    clear_summary_reembed_skipped(&cfg, "summary-1", "model@3").unwrap();
    clear_summary_reembed_skipped(&cfg, "summary-1", "model@3").unwrap();
    assert!(versioned::list_state(&cfg, SUMMARY_REEMBED_SKIPS)
        .unwrap()
        .is_empty());
}

#[test]
fn clear_reembed_skipped_for_signature_removes_all_tombstones_for_sig() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("slack:#a", 0, 1_700_000_000_000);
    let c2 = sample_chunk("slack:#b", 1, 1_700_000_000_001);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone()]).unwrap();
    let sig = tree_active_signature(&cfg);
    let other_sig = "provider=other;model=x;dims=8";
    mark_chunk_reembed_skipped(&cfg, &c1.id, &sig, "r1").unwrap();
    mark_chunk_reembed_skipped(&cfg, &c2.id, &sig, "r2").unwrap();
    mark_chunk_reembed_skipped(&cfg, &c1.id, other_sig, "other").unwrap();
    // A re-embed tombstone belongs to the model signature and can be cleared
    // independently of the summary projection that produced it.
    let summary_id = "summary-bulk-clear-test";
    mark_summary_reembed_skipped(&cfg, summary_id, &sig, "summary tombstone").unwrap();

    let deleted = clear_reembed_skipped_for_signature(&cfg, &sig).unwrap();
    assert_eq!(deleted, 3);

    let remaining_chunks = versioned::list_state(&cfg, CHUNK_REEMBED_SKIPS).unwrap();
    assert!(!remaining_chunks
        .iter()
        .any(|(_, value)| value["model_signature"] == sig));
    let remaining_summaries = versioned::list_state(&cfg, SUMMARY_REEMBED_SKIPS).unwrap();
    assert!(!remaining_summaries
        .iter()
        .any(|(_, value)| value["model_signature"] == sig));
    assert_eq!(
        remaining_chunks
            .iter()
            .filter(|(_, value)| value["chunk_id"] == c1.id && value["model_signature"] == other_sig)
            .count(),
        1
    );
    assert_eq!(clear_reembed_skipped_for_signature(&cfg, &sig).unwrap(), 0);
}

#[test]
fn validate_reembed_skip_key_rejects_empty_and_oversized() {
    use super::embeddings::{validate_reembed_skip_key, REEMBED_SKIP_KEY_MAX_LEN};
    assert!(validate_reembed_skip_key("chunk_id", "  ").is_err());
    let huge = "a".repeat(REEMBED_SKIP_KEY_MAX_LEN + 1);
    assert!(validate_reembed_skip_key("chunk_id", &huge).is_err());
    assert!(validate_reembed_skip_key("chunk_id", "ok\0bad").is_err());
    assert_eq!(
        validate_reembed_skip_key("chunk_id", "  trimmed  ").unwrap(),
        "trimmed"
    );
}

#[test]
fn get_chunks_batch_returns_present_ids_in_map() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    let c2 = sample_chunk("slack:#eng", 1, 1_700_000_000_000);
    let c3 = sample_chunk("slack:#ops", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone(), c3.clone()]).unwrap();

    let ids = vec![c1.id.clone(), c2.id.clone(), c3.id.clone()];
    let map = get_chunks_batch(&cfg, &ids).unwrap();
    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&c1.id), Some(&c1));
    assert_eq!(map.get(&c2.id), Some(&c2));
    assert_eq!(map.get(&c3.id), Some(&c3));
}

#[test]
fn get_chunks_batch_empty_input_and_missing_ids() {
    let (_tmp, cfg) = test_config();
    let empty = get_chunks_batch(&cfg, &[]).unwrap();
    assert!(empty.is_empty());

    let c = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let ids = vec![
        c.id.clone(),
        "ghost:no-such-1".into(),
        "ghost:no-such-2".into(),
    ];
    let map = get_chunks_batch(&cfg, &ids).unwrap();
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&c.id), Some(&c));
    assert!(!map.contains_key("ghost:no-such-1"));
    assert!(!map.contains_key("ghost:no-such-2"));
}

#[test]
fn batch_embedding_lookup_returns_only_signature_scoped_rows() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    let c2 = sample_chunk("slack:#eng", 1, 1_700_000_000_000);
    let c3 = sample_chunk("slack:#eng", 2, 1_700_000_000_000);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone(), c3.clone()]).unwrap();

    let sig_a = "openai/text-embedding-3-small@1536";
    let sig_b = "local/bge-small@384";
    set_chunk_embedding_for_signature(&cfg, &c1.id, sig_a, &[0.1, 0.2]).unwrap();
    set_chunk_embedding_for_signature(&cfg, &c2.id, sig_a, &[0.3, 0.4]).unwrap();
    set_chunk_embedding_for_signature(&cfg, &c3.id, sig_b, &[0.5, 0.6, 0.7]).unwrap();

    let ids = vec![c1.id.clone(), c2.id.clone(), c3.id.clone()];
    let map_a = get_chunk_embeddings_for_signature_batch(&cfg, &ids, sig_a).unwrap();
    assert_eq!(map_a.len(), 2, "only c1 and c2 are under sig_a");
    assert_eq!(map_a.get(&c1.id).cloned(), Some(vec![0.1, 0.2]));
    assert_eq!(map_a.get(&c2.id).cloned(), Some(vec![0.3, 0.4]));
    assert!(!map_a.contains_key(&c3.id), "c3 has only sig_b");

    let map_b = get_chunk_embeddings_for_signature_batch(&cfg, &ids, sig_b).unwrap();
    assert_eq!(map_b.len(), 1);
    assert_eq!(map_b.get(&c3.id).cloned(), Some(vec![0.5, 0.6, 0.7]));
}

#[test]
fn batch_embedding_lookup_empty_input_returns_empty_map() {
    let (_tmp, cfg) = test_config();
    let map = get_chunk_embeddings_for_signature_batch(&cfg, &[], "any/sig@1").unwrap();
    assert!(map.is_empty());
}

#[test]
fn batch_embedding_lookup_unknown_ids_absent_from_map() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = "openai/text-embedding-3-small@1536";
    set_chunk_embedding_for_signature(&cfg, &c.id, sig, &[0.1]).unwrap();

    let ids = vec![
        c.id.clone(),
        "ghost:no-such-chunk-1".into(),
        "ghost:no-such-chunk-2".into(),
    ];
    let map = get_chunk_embeddings_for_signature_batch(&cfg, &ids, sig).unwrap();
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&c.id).cloned(), Some(vec![0.1]));
}

#[test]
fn legacy_body_read_skip_does_not_hide_reembed_work() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("persona/communication", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = "bge-m3@1024";
    mark_chunk_reembed_skipped(
        &cfg,
        &c.id,
        sig,
        &format!(
            "body read failed: {LEGACY_NO_CONTENT_POINTER_REASON_PREFIX}{}",
            c.id
        ),
    )
    .unwrap();

    assert!(has_uncovered_reembed_work(&cfg, sig).unwrap());
}

#[test]
fn legacy_empty_pointer_skip_does_not_hide_reembed_work() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("persona/communication", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = "bge-m3@1024";
    mark_chunk_reembed_skipped(
        &cfg,
        &c.id,
        sig,
        &format!(
            "body read failed: {LEGACY_EMPTY_CONTENT_POINTER_REASON_PREFIX}{}",
            c.id
        ),
    )
    .unwrap();

    assert!(has_uncovered_reembed_work(&cfg, sig).unwrap());
}

#[test]
fn empty_legacy_content_skip_hides_reembed_work() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("persona/communication", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = "bge-m3@1024";
    mark_chunk_reembed_skipped(
        &cfg,
        &c.id,
        sig,
        &format!(
            "body read failed: {LEGACY_EMPTY_CHUNK_CONTENT_REASON_PREFIX}{}",
            c.id
        ),
    )
    .unwrap();

    assert!(!has_uncovered_reembed_work(&cfg, sig).unwrap());
}

#[test]
fn non_legacy_skip_still_hides_reembed_work() {
    let (_tmp, cfg) = test_config();
    let c = sample_chunk("persona/communication", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&c)).unwrap();
    let sig = "bge-m3@1024";
    mark_chunk_reembed_skipped(&cfg, &c.id, sig, "embed failed: provider rejected input").unwrap();

    assert!(!has_uncovered_reembed_work(&cfg, sig).unwrap());
}

#[test]
fn batch_embedding_lookup_splits_id_list_above_per_batch_threshold() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("slack:#a", 0, 1_700_000_000_000);
    let c2 = sample_chunk("slack:#b", 0, 1_700_000_000_000);
    let c3 = sample_chunk("slack:#c", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone(), c3.clone()]).unwrap();
    let sig = "openai/text-embedding-3-small@1536";
    set_chunk_embedding_for_signature(&cfg, &c1.id, sig, &[1.0]).unwrap();
    set_chunk_embedding_for_signature(&cfg, &c2.id, sig, &[2.0]).unwrap();
    set_chunk_embedding_for_signature(&cfg, &c3.id, sig, &[3.0]).unwrap();

    let mut ids: Vec<String> = (0..498).map(|i| format!("ghost:{i}")).collect();
    ids.push(c1.id.clone());
    ids.push(c2.id.clone());
    ids.push(c3.id.clone());
    assert_eq!(ids.len(), 501);

    let map = get_chunk_embeddings_for_signature_batch(&cfg, &ids, sig).unwrap();
    assert_eq!(map.len(), 3, "only the 3 real ids should be present");
    assert_eq!(map.get(&c1.id).cloned(), Some(vec![1.0]));
    assert_eq!(map.get(&c2.id).cloned(), Some(vec![2.0]));
    assert_eq!(map.get(&c3.id).cloned(), Some(vec![3.0]));
}

#[test]
fn global_topic_purge_removes_only_global_and_topic() {
    let (_tmp, cfg) = test_config();
    upsert_chunks(&cfg, &[sample_chunk("slack:#eng", 0, 1_700_000_000_000)]).unwrap();

    let summaries = content_root(&cfg).join("wiki").join("summaries");
    for d in [
        "global-2026-05-28",
        "global",
        "topic-alice",
        "source-slack-eng",
    ] {
        std::fs::create_dir_all(summaries.join(d).join("L0")).unwrap();
    }

    with_connection(&cfg, |conn| {
        for (id, kind) in [("source:s1", "source"), ("global:g1", "global"), ("topic:t1", "topic")] {
            conn.execute(
                "INSERT INTO mem_tree_trees (id, kind, scope, max_level, status, created_at_ms) \
                 VALUES (?1, ?2, ?2, 0, 'active', 0)",
                params![id, kind],
            )?;
            conn.execute(
                "INSERT INTO mem_tree_summaries \
                 (id, tree_id, tree_kind, level, content, token_count, \
                  time_range_start_ms, time_range_end_ms, sealed_at_ms) \
                 VALUES (?1, ?2, ?3, 0, 'x', 1, 0, 0, 0)",
                params![format!("sum-{id}"), id, kind],
            )?;
        }
        for (jid, kind) in [("j1", "topic_route"), ("j2", "digest_daily"), ("j3", "extract_chunk")] {
            conn.execute(
                "INSERT INTO mem_tree_jobs (id, kind, payload_json, available_at_ms, created_at_ms) \
                 VALUES (?1, ?2, '{}', 0, 0)",
                params![jid, kind],
            )?;
        }
        conn.pragma_update(None, "user_version", 1i64)?;
        purge_global_topic_trees(conn, &cfg)?;

        let trees: i64 = conn.query_row("SELECT COUNT(*) FROM mem_tree_trees", [], |r| r.get(0))?;
        assert_eq!(trees, 1, "only the source tree should remain");
        let kind: String = conn.query_row("SELECT kind FROM mem_tree_trees", [], |r| r.get(0))?;
        assert_eq!(kind, "source");

        let summaries_left: i64 =
            conn.query_row("SELECT COUNT(*) FROM mem_tree_summaries", [], |r| r.get(0))?;
        assert_eq!(summaries_left, 1);

        let jobs_left: Vec<String> = {
            let mut stmt = conn.prepare("SELECT kind FROM mem_tree_jobs ORDER BY kind")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        assert_eq!(jobs_left, vec!["extract_chunk".to_string()]);

        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        assert_eq!(version, 2);
        Ok(())
    })
    .unwrap();

    assert!(!summaries.join("global-2026-05-28").exists());
    assert!(!summaries.join("global").exists());
    assert!(!summaries.join("topic-alice").exists());
    assert!(
        summaries.join("source-slack-eng").exists(),
        "source summary folder must survive"
    );
}

#[test]
fn extraction_coverage_empty_store_is_zero() {
    let (_tmp, cfg) = test_config();
    assert_eq!(extraction_coverage(&cfg).unwrap(), 0.0);
}

#[test]
fn extraction_coverage_reflects_indexed_fraction() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("slack:#eng", 0, 1_700_000_000_000);
    let c2 = sample_chunk("slack:#eng", 1, 1_700_000_001_000);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone()]).unwrap();

    let index = crate::memory::store::entity_index::EntityIndex::for_memory_config(&cfg).unwrap();
    index
        .index_entity(
            &crate::memory::store::entity_index::types::CanonicalEntity {
                canonical_id: "person:Alice".into(),
                kind: crate::memory::store::entity_index::types::EntityKind::Person,
                surface: "Alice".into(),
                span_start: 0,
                span_end: 5,
                score: 0.9,
            },
            &c1.id,
            "leaf",
            1,
            None,
        )
        .unwrap();

    let cov = extraction_coverage(&cfg).unwrap();
    assert!((cov - 0.5).abs() < 1e-6, "expected 0.5, got {cov}");

    index
        .index_entity(
            &crate::memory::store::entity_index::types::CanonicalEntity {
                canonical_id: "person:Bob".into(),
                kind: crate::memory::store::entity_index::types::EntityKind::Person,
                surface: "Bob".into(),
                span_start: 0,
                span_end: 3,
                score: 0.9,
            },
            &c2.id,
            "leaf",
            2,
            None,
        )
        .unwrap();
    assert!((extraction_coverage(&cfg).unwrap() - 1.0).abs() < 1e-6);
}

#[test]
fn a_vector_written_under_the_legacy_spelling_is_readable_under_the_active_signature() {
    // The regression this exists for: the tree used to key sidecars by
    // `{model}@{dims}`. After the convention was unified on the canonical
    // `provider=…;model=…;dims=…` string, an exact-match read stopped seeing
    // those rows — hundreds of live chunks scored against nothing, from a
    // format change that was never a model change. Nothing is rewritten on
    // disk; the read simply accepts both spellings.
    let (_tmp, mut cfg) = test_config();
    cfg.embedding.provider = "cloud".into();
    cfg.embedding.model = "embedding-v1".into();
    cfg.embedding.dim = 3;

    let chunk = sample_chunk("gmail:inbox", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&chunk)).unwrap();
    set_chunk_embedding_for_signature(&cfg, &chunk.id, "embedding-v1@3", &[0.1, 0.2, 0.3]).unwrap();

    let active = tree_active_signature(&cfg);
    assert_eq!(active, "provider=cloud;model=embedding-v1;dims=3");
    assert_eq!(
        get_chunk_embedding(&cfg, &chunk.id).unwrap(),
        Some(vec![0.1, 0.2, 0.3]),
        "a legacy-spelled vector must be visible under the canonical signature"
    );

    // Batch read — the retrieval hot path — sees it too.
    let batch =
        get_chunk_embeddings_for_signature_batch(&cfg, std::slice::from_ref(&chunk.id), &active)
            .unwrap();
    assert_eq!(batch.get(&chunk.id), Some(&vec![0.1, 0.2, 0.3]));

    // …and it counts as covered, so the re-embed chain does not pay to
    // recompute a vector that is already on disk.
    assert!(
        !super::has_uncovered_reembed_work(&cfg, &active).unwrap(),
        "a legacy-spelled vector must count as coverage"
    );

    // A genuinely different space is still separate.
    assert_eq!(
        get_chunk_embedding_for_signature(&cfg, &chunk.id, "provider=cloud;model=other-v2;dims=3")
            .unwrap(),
        None
    );
}
