//! Unit tests for the deletes `super::store_delete` owns beyond the
//! source-scoped ones already covered in `store_tests`: the by-id delete and
//! the whole-tier purge.

use super::types::{chunk_id, Chunk, Metadata, SourceKind, SourceRef};
use super::{
    count_chunks, delete_chunk_by_id, get_chunk, is_source_ingested, purge_all, upsert_chunks,
    with_connection,
};
use crate::memory::config::MemoryConfig;
use chrono::{TimeZone, Utc};
use rusqlite::params;
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = super::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn test_config_with_faults() -> (TempDir, MemoryConfig, tinystoragedrivers_testkit::FaultPlan) {
    use tinystoragedrivers_api::Storage;
    use tinystoragedrivers_testkit::Failing;
    let tmp = TempDir::new().expect("tempdir");
    let scope = tinystoragedrivers_types::Scope::new(&format!(
        "test:tinycortex-purge-{}",
        uuid::Uuid::new_v4()
    ))
    .expect("scope");
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let failing = Failing::new(raw.documents().clone());
    let faults = failing.faults.clone();
    let storage = Storage::new(
        std::sync::Arc::new(failing),
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    let workspace = tmp.path().to_path_buf();
    let config = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(MemoryConfig::open_with_storage(workspace, storage))
    })
    .join()
    .expect("storage setup thread")
    .expect("open host storage");
    (tmp, config, faults)
}

fn chunk_with(source_id: &str, seq: u32, ts_ms: i64, content: &str) -> Chunk {
    let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
    Chunk {
        id: chunk_id(SourceKind::Chat, source_id, seq, content),
        content: content.to_string(),
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

fn count_rows(cfg: &MemoryConfig, table: &str) -> i64 {
    with_connection(cfg, |conn| {
        Ok(
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?,
        )
    })
    .unwrap()
}

fn index_entity(cfg: &MemoryConfig, node_id: &str) {
    with_connection(cfg, |conn| {
        conn.execute(
            "INSERT INTO mem_tree_entity_index (
                entity_id, node_id, node_kind, entity_kind, surface, score, timestamp_ms
             ) VALUES ('entity:a', ?1, 'leaf', 'org', 'Acme', 0.9, 1700000000000)",
            params![node_id],
        )?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn delete_chunk_by_id_takes_one_chunk_and_leaves_the_source() {
    let (_tmp, cfg) = test_config();
    let target = chunk_with("slack:#eng", 0, 2_000, "target");
    let sibling = chunk_with("slack:#eng", 1, 1_000, "sibling");
    upsert_chunks(&cfg, &[target.clone(), sibling.clone()]).unwrap();
    index_entity(&cfg, &target.id);
    index_entity(&cfg, &sibling.id);
    with_connection(&cfg, |conn| {
        conn.execute(
            "INSERT INTO mem_tree_ingested_sources (source_kind, source_id, ingested_at_ms)
             VALUES ('chat', 'slack:#eng', 1700000000000)",
            [],
        )?;
        Ok(())
    })
    .unwrap();

    assert_eq!(delete_chunk_by_id(&cfg, &target.id).unwrap(), 1);
    assert!(get_chunk(&cfg, &target.id).unwrap().is_none());
    assert!(get_chunk(&cfg, &sibling.id).unwrap().is_some());
    assert_eq!(count_rows(&cfg, "mem_tree_entity_index"), 1);
    assert!(
        is_source_ingested(&cfg, SourceKind::Chat, "slack:#eng").unwrap(),
        "a source with surviving chunks keeps its ingest gate"
    );

    // Idempotent, and an unknown id is not an error.
    assert_eq!(delete_chunk_by_id(&cfg, &target.id).unwrap(), 0);
    assert_eq!(delete_chunk_by_id(&cfg, "no-such-chunk").unwrap(), 0);
}

#[test]
fn deleting_a_chunk_tombstones_lifecycle_and_removes_versioned_sidecars() {
    let (_tmp, cfg) = test_config();
    let chunk = chunk_with("delete-sidecars", 0, 3_000, "sidecar body");
    upsert_chunks(&cfg, std::slice::from_ref(&chunk)).unwrap();
    let signature = "delete-sidecars-model";
    super::set_chunk_embedding_for_signature(&cfg, &chunk.id, signature, &[0.1, 0.2]).unwrap();
    super::set_chunk_raw_refs(
        &cfg,
        &chunk.id,
        &[super::RawRef {
            path: "raw/delete-sidecars.md".into(),
            start: 0,
            end: None,
        }],
    )
    .unwrap();
    let entity = crate::memory::store::entity_index::types::CanonicalEntity {
        canonical_id: "email:sidecar@example.com".into(),
        kind: crate::memory::store::entity_index::types::EntityKind::Email,
        surface: "sidecar@example.com".into(),
        span_start: 0,
        span_end: 20,
        score: 1.0,
    };
    let entity_index =
        crate::memory::store::entity_index::EntityIndex::for_memory_config(&cfg).unwrap();
    entity_index
        .index_entity(&entity, &chunk.id, "leaf", 3_000, None)
        .unwrap();

    assert_eq!(delete_chunk_by_id(&cfg, &chunk.id).unwrap(), 1);
    assert_eq!(
        super::get_chunk_lifecycle_status(&cfg, &chunk.id)
            .unwrap()
            .as_deref(),
        Some("deleted")
    );
    assert!(
        super::get_chunk_embedding_for_signature(&cfg, &chunk.id, signature)
            .unwrap()
            .is_none()
    );
    assert!(super::get_chunk_raw_refs(&cfg, &chunk.id)
        .unwrap()
        .is_none());
    assert!(entity_index
        .list_entity_ids_for_node(&chunk.id)
        .unwrap()
        .is_empty());
}

#[test]
fn purge_removes_tree_gate_and_queue_state_when_there_are_no_chunks() {
    let (_tmp, cfg) = test_config();
    let source_id = "purge-empty-source";
    assert!(super::claim_source_ingest(&cfg, SourceKind::Chat, source_id, 123).unwrap());
    super::versioned::write_state(
        &cfg,
        super::versioned::StateChange {
            collection: super::collections::SUMMARIES.into(),
            key: "summary:orphan-without-chunk".into(),
            value: Some(serde_json::json!({"id":"orphan-without-chunk"})),
        },
    )
    .unwrap();
    let job_id = crate::memory::queue::store::enqueue(
        &cfg,
        &crate::memory::queue::NewJob {
            id: Some("purge-empty-job".into()),
            kind: crate::memory::queue::JobKind::ExtractChunk,
            payload_json: "{}".into(),
            dedupe_key: None,
            available_at_ms: None,
            max_attempts: None,
        },
    )
    .unwrap()
    .expect("new job is persisted");

    assert_eq!(count_chunks(&cfg).unwrap(), 0);
    assert_eq!(purge_all(&cfg).unwrap(), 3);
    assert!(
        super::versioned::list_state(&cfg, super::collections::SUMMARIES)
            .unwrap()
            .is_empty()
    );
    assert!(!is_source_ingested(&cfg, SourceKind::Chat, source_id).unwrap());
    assert!(crate::memory::queue::store::get_job(&cfg, &job_id)
        .unwrap()
        .is_none());
    assert_eq!(purge_all(&cfg).unwrap(), 0);
}

#[test]
fn enqueue_advancing_the_head_invalidates_a_captured_purge_snapshot() {
    let (_tmp, cfg) = test_config();
    let collection = super::collections::SUMMARIES;
    let change = super::versioned::StateChange {
        collection: collection.into(),
        key: "summary:before-enqueue".into(),
        value: Some(serde_json::json!({"id":"before-enqueue"})),
    };
    super::versioned::write_state(&cfg, change.clone()).unwrap();
    let snapshot = super::versioned::capture_head(&cfg).unwrap();
    crate::memory::queue::store::enqueue(
        &cfg,
        &crate::memory::queue::NewJob {
            id: Some("purge-concurrent-enqueue".into()),
            kind: crate::memory::queue::JobKind::ExtractChunk,
            payload_json: "{}".into(),
            dedupe_key: None,
            available_at_ms: None,
            max_attempts: None,
        },
    )
    .unwrap();

    let committed = super::versioned::commit_if_state_matches_at_snapshot(
        &cfg,
        &snapshot,
        std::slice::from_ref(&change),
        &[super::versioned::StateChange {
            value: None,
            ..change.clone()
        }],
    )
    .unwrap();

    assert!(
        !committed,
        "new queue intent must invalidate the old purge head"
    );
    assert_eq!(
        super::versioned::list_state(&cfg, collection)
            .unwrap()
            .len(),
        1,
        "a stale purge snapshot must not partially clear tree state"
    );
    assert_eq!(purge_all(&cfg).unwrap(), 2);
}

/// Seed one row in every table `purge_all` empties, so a table name that does
/// not exist in the schema fails the test rather than passing silently.
fn seed_full_tier(cfg: &MemoryConfig, chunk: &Chunk) {
    with_connection(cfg, |conn| {
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO mem_tree_score (
                chunk_id, total, token_count_signal, unique_words_signal, metadata_weight,
                source_weight, interaction_weight, entity_density, dropped, reason, computed_at_ms
             ) VALUES (?1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0, NULL, 1700000000000)",
            params![chunk.id],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_entity_index (
                entity_id, node_id, node_kind, entity_kind, surface, score, timestamp_ms
             ) VALUES ('entity:a', ?1, 'leaf', 'org', 'Acme', 0.9, 1700000000000)",
            params![chunk.id],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_entity_edges (entity_a, entity_b, weight, updated_ms)
             VALUES ('entity:a', 'entity:b', 1, 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_entity_hotness (entity_id, last_updated_ms)
             VALUES ('entity:a', 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_jobs (id, kind, payload_json, available_at_ms, created_at_ms)
             VALUES ('job-1', 'score', '{}', 1700000000000, 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_trees (id, kind, scope, max_level, created_at_ms)
             VALUES ('tree-1', 'source', 'slack:#eng', 0, 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_buffers (tree_id, level, updated_at_ms)
             VALUES ('tree-1', 0, 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_summaries (
                id, tree_id, tree_kind, level, content, token_count,
                time_range_start_ms, time_range_end_ms, sealed_at_ms
             ) VALUES ('sum-1', 'tree-1', 'source', 1, 'summary', 3,
                       1700000000000, 1700000000000, 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_summary_embeddings (
                summary_id, model_signature, vector, dim, created_at
             ) VALUES ('sum-1', 'test/model@3', ?1, 3, 1700000000.0)",
            params![vec![1_u8, 2, 3]],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_summary_reembed_skipped (
                summary_id, model_signature, reason, skipped_at_ms
             ) VALUES ('sum-1', 'test/model@3', 'terminal', 1700000000000)",
            [],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_chunk_embeddings (
                chunk_id, model_signature, vector, dim, created_at
             ) VALUES (?1, 'test/model@3', ?2, 3, 1700000000.0)",
            params![chunk.id, vec![1_u8, 2, 3]],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_chunk_reembed_skipped (
                chunk_id, model_signature, reason, skipped_at_ms
             ) VALUES (?1, 'test/model@3', 'terminal', 1700000000000)",
            params![chunk.id],
        )?;
        tx.execute(
            "INSERT INTO mem_tree_ingested_sources (source_kind, source_id, ingested_at_ms)
             VALUES ('chat', 'slack:#eng', 1700000000000)",
            [],
        )?;
        tx.commit()?;
        Ok(())
    })
    .unwrap();
}

const PURGED_TABLES: [&str; 14] = [
    "mem_tree_score",
    "mem_tree_entity_index",
    "mem_tree_entity_edges",
    "mem_tree_entity_hotness",
    "mem_tree_jobs",
    "mem_tree_buffers",
    "mem_tree_summary_embeddings",
    "mem_tree_summary_reembed_skipped",
    "mem_tree_summaries",
    "mem_tree_trees",
    "mem_tree_chunk_embeddings",
    "mem_tree_chunk_reembed_skipped",
    "mem_tree_chunks",
    "mem_tree_ingested_sources",
];

/// A failed host write cannot expose any part of a multi-collection purge.
#[test]
fn purge_failure_keeps_the_captured_owner_state_visible() {
    let (_tmp, cfg, faults) = test_config_with_faults();
    let summary = super::versioned::StateChange {
        collection: super::collections::SUMMARIES.into(),
        key: "summary:purge-atomicity".into(),
        value: Some(serde_json::json!({"id":"purge-atomicity"})),
    };
    let gate = super::versioned::StateChange {
        collection: super::collections::INGESTED_SOURCES.into(),
        key: "chat\0purge-atomicity".into(),
        value: Some(serde_json::json!({"source_kind":"chat","source_id":"purge-atomicity"})),
    };
    assert!(super::versioned::commit_if_state_matches(
        &cfg,
        &[],
        &[],
        &[summary.clone(), gate.clone()],
    )
    .unwrap());
    faults.fail_operation_nth(
        "documents.put_with",
        3,
        tinystoragedrivers_types::ErrorKind::Backend,
    );

    assert!(purge_all(&cfg).is_err());
    assert_eq!(
        super::versioned::read_state(&cfg, &summary.collection, &summary.key).unwrap(),
        summary.value,
        "a failed purge must leave summary state visible"
    );
    assert_eq!(
        super::versioned::read_state(&cfg, &gate.collection, &gate.key).unwrap(),
        gate.value,
        "a failed purge must leave the source gate visible"
    );

    faults.clear();
    assert_eq!(purge_all(&cfg).unwrap(), 2);
    assert!(
        super::versioned::list_state(&cfg, super::collections::SUMMARIES)
            .unwrap()
            .is_empty()
    );
    assert!(
        super::versioned::list_state(&cfg, super::collections::INGESTED_SOURCES)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn purge_all_leaves_the_write_audit_trail_alone() {
    let (_tmp, cfg) = test_config();
    let chunk = chunk_with("slack:#eng", 0, 1_000, "body");
    upsert_chunks(&cfg, std::slice::from_ref(&chunk)).unwrap();
    with_connection(&cfg, |conn| {
        conn.execute(
            "INSERT INTO mcp_writes (timestamp_ms, client_info, tool_name, success)
             VALUES (1700000000000, 'test-client', 'remember', 1)",
            [],
        )?;
        Ok(())
    })
    .unwrap();

    let expected: i64 = PURGED_TABLES
        .iter()
        .map(|table| count_rows(&cfg, table))
        .sum();
    assert_eq!(purge_all(&cfg).unwrap() as i64, expected);
    assert_eq!(
        count_rows(&cfg, "mcp_writes"),
        1,
        "an audit record of a write is not the memory it wrote"
    );
}
