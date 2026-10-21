//! Unit tests for the deletes `super::store_delete` owns beyond the
//! source-scoped ones already covered in `store_tests`: the by-id delete and
//! the whole-tier purge.

use super::types::{chunk_id, Chunk, Metadata, SourceKind, SourceRef};
use super::{
    content_root, count_chunks, delete_chunk_by_id, delete_chunks_by_source, get_chunk,
    is_source_ingested, purge_all, purge_all_async, upsert_chunks, upsert_staged_chunks,
};
use crate::memory::config::MemoryConfig;
use chrono::{TimeZone, Utc};
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

fn index_entity(cfg: &MemoryConfig, node_id: &str) {
    let index = crate::memory::store::entity_index::EntityIndex::for_memory_config(cfg).unwrap();
    let entity = crate::memory::store::entity_index::types::CanonicalEntity {
        canonical_id: "org:acme".into(),
        kind: crate::memory::store::entity_index::types::EntityKind::Organization,
        surface: "Acme".into(),
        span_start: 0,
        span_end: 4,
        score: 0.9,
    };
    index
        .index_entity(&entity, node_id, "leaf", 1_700_000_000_000, None)
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
    assert!(super::store_sources::claim_source_ingest(
        &cfg,
        SourceKind::Chat,
        "slack:#eng",
        1_700_000_000_000,
    )
    .unwrap());

    assert_eq!(delete_chunk_by_id(&cfg, &target.id).unwrap(), 1);
    assert!(get_chunk(&cfg, &target.id).unwrap().is_none());
    assert!(get_chunk(&cfg, &sibling.id).unwrap().is_some());
    assert_eq!(
        crate::memory::score::store_query::count_entity_index(&cfg).unwrap(),
        1
    );
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

#[tokio::test]
async fn purge_all_leaves_host_audit_collection_alone() {
    use tinystoragedrivers_api::DocumentStore;
    use tinystoragedrivers_types::{CollectionSpec, Precondition};

    let (_tmp, cfg) = test_config();
    let chunk = chunk_with("slack:#eng", 0, 1_000, "body");
    upsert_chunks_async(&cfg, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    let docs = cfg.storage().unwrap().documents();
    docs.ensure_collection(&CollectionSpec::new("host_audit"))
        .await
        .unwrap();
    docs.put(
        "host_audit",
        "write:1",
        serde_json::json!({"tool":"remember"}),
        Precondition::None,
    )
    .await
    .unwrap();

    assert_eq!(purge_all_async(&cfg).await.unwrap(), 1);
    assert_eq!(
        docs.get("host_audit", "write:1")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"tool":"remember"}),
        "an audit record of a write is not the memory it wrote"
    );
}

#[cfg(unix)]
#[test]
fn delete_chunks_by_source_removes_symlink_entry_not_target_file() {
    let (_tmp, cfg) = test_config();
    let chunk = chunk_with("slack:c-1", 0, 1_700_000_000_000, "body");
    upsert_staged_chunks(
        &cfg,
        &[super::types::StagedChunk {
            chunk: chunk.clone(),
            content_path: "chunks/link.md".into(),
            content_sha256: "link-hash".into(),
        }],
    )
    .unwrap();
    let root = content_root(&cfg);
    let target_path = root.join("chunks/target.md");
    let link_path = root.join("chunks/link.md");
    std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
    std::fs::write(&target_path, "target").unwrap();
    std::os::unix::fs::symlink("target.md", &link_path).unwrap();

    assert_eq!(
        delete_chunks_by_source(&cfg, SourceKind::Chat, "slack:c-1").unwrap(),
        1
    );
    assert!(target_path.exists());
    assert!(!link_path.exists());
}
