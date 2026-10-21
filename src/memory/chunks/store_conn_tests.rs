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
use super::types::{chunk_id, Chunk, Metadata, SourceKind, SourceRef, StagedChunk};
use super::{
    claim_source_ingest_tx, clear_chunk_reembed_skipped, clear_reembed_skipped_for_signature,
    content_root, count_chunks, db_path_for, delete_chunks_by_owner, delete_chunks_by_source,
    extraction_coverage, get_chunk, get_chunk_embedding, get_chunk_embedding_for_signature,
    get_chunk_embedding_for_signature_async,
    get_chunk_embeddings_for_signature_batch, get_chunks_batch, is_source_ingested, list_chunks,
    import_legacy_batch, mark_chunk_reembed_skipped, set_chunk_embedding,
    set_chunk_embedding_for_signature, tree_active_signature, upsert_chunks,
    upsert_staged_chunks, LegacyChunkRecord, LegacyEmbedding, LegacyImportBatch, ListChunksQuery,
    DB_DIR, GLOBAL_TOPIC_PURGE_MIGRATION_VERSION, CHUNK_STATUS_ADMITTED,
};
use crate::memory::config::MemoryConfig;
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
fn with_connection_serialises_concurrent_schema_init() {
    use std::sync::atomic::Ordering;

    let (_tmp, cfg) = test_config();
    let db_path = db_path_for(&cfg);
    let errors = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let cfg = cfg.clone();
            let errors = errors.clone();
            std::thread::spawn(move || {
                if with_connection(&cfg, |_| Ok(())).is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("worker thread panicked");
    }

    assert_eq!(
        errors.load(Ordering::Relaxed),
        0,
        "concurrent with_connection callers must all succeed"
    );
    let applied = schema_apply_count_for_path_for_tests(&db_path);
    assert!(
        applied >= 1,
        "apply_schema must run before concurrent callers can use the DB"
    );
}

#[test]
fn is_transient_cold_start_classifies_known_extended_codes() {
    use rusqlite::ffi;
    use rusqlite::ErrorCode;

    for extended in [14, 1546, 4618, 4874, 5386, 8714] {
        let err = anyhow::Error::from(rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ErrorCode::SystemIoFailure,
                extended_code: extended,
            },
            None,
        ));
        assert!(
            is_transient_cold_start(&err),
            "extended_code {extended} must classify as transient cold-start"
        );
    }

    let busy = anyhow::Error::from(rusqlite::Error::SqliteFailure(
        ffi::Error {
            code: ErrorCode::DatabaseBusy,
            extended_code: 5,
        },
        None,
    ));
    assert!(
        !is_transient_cold_start(&busy),
        "DatabaseBusy must not classify as cold-start"
    );

    let other: anyhow::Error = anyhow::anyhow!("not a sqlite error");
    assert!(
        !is_transient_cold_start(&other),
        "non-SQLite errors must not classify"
    );
}

#[test]
fn with_connection_keeps_foreign_keys_on_for_every_call() {
    let (_tmp, cfg) = test_config();
    let fk_on_first: i64 = with_connection(&cfg, |conn| {
        Ok(conn.query_row("PRAGMA foreign_keys;", params![], |r| r.get::<_, i64>(0))?)
    })
    .unwrap();
    assert_eq!(
        fk_on_first, 1,
        "foreign_keys must be ON on first connection"
    );
    let fk_on_second: i64 = with_connection(&cfg, |conn| {
        Ok(conn.query_row("PRAGMA foreign_keys;", params![], |r| r.get::<_, i64>(0))?)
    })
    .unwrap();
    assert_eq!(
        fk_on_second, 1,
        "foreign_keys must be ON on fast-path connection"
    );
}

#[tokio::test]
async fn typed_legacy_embedding_import_preserves_signature_dimension_and_idempotence() {
    let (_tmp, cfg) = test_config();
    let chunk = sample_chunk("legacy-embedding-source", 0, 1_700_000_000_000);
    let vector = vec![0.25f32, 0.5, 0.75];
    let signature = "provider=fixture;model=embedding;dims=3";
    let batch = LegacyImportBatch {
        chunks: vec![LegacyChunkRecord {
            chunk: chunk.clone(),
            content: None,
            raw_refs: Vec::new(),
            score: None,
            lifecycle_status: Some(CHUNK_STATUS_ADMITTED.into()),
            embeddings: vec![LegacyEmbedding {
                model_signature: signature.into(),
                vector: vector.clone(),
                created_at_ms: 1_699_999_000_000,
            }],
            entities: Vec::new(),
        }],
        ..Default::default()
    };
    import_legacy_batch(&cfg, &batch).await.unwrap();
    assert_eq!(
        get_chunk_embedding_for_signature_async(&cfg, &chunk.id, signature)
            .await
            .unwrap(),
        Some(vector.clone()),
        "typed import must preserve the embedding values under their historical signature"
    );
    let rows = super::versioned::list_state_async(&cfg, super::collections::CHUNK_EMBEDDINGS)
        .await
        .unwrap();
    let (_, value) = rows.first().unwrap();
    assert_eq!(value["dim"], 3);
    assert_eq!(value["created_at_ms"], 1_699_999_000_000i64);

    let before_retry = super::versioned::capture_state_snapshot_async(&cfg)
        .await
        .unwrap();
    import_legacy_batch(&cfg, &batch).await.unwrap();
    let after_retry = super::versioned::capture_state_snapshot_async(&cfg)
        .await
        .unwrap();
    assert_eq!(before_retry, after_retry, "retry must not append another owner commit");
}

#[test]
fn connection_cache_returns_same_arc_for_same_workspace() {
    let (_tmp, cfg) = test_config();

    let arc1 = get_or_init_connection(&cfg).expect("first get_or_init");
    let arc2 = get_or_init_connection(&cfg).expect("second get_or_init");
    assert!(
        Arc::ptr_eq(&arc1, &arc2),
        "expected the same Arc from the connection cache"
    );
}

#[test]
fn connection_cache_uses_separate_connections_for_different_workspaces() {
    let (_tmp1, cfg1) = test_config();
    let (_tmp2, cfg2) = test_config();

    let arc1 = get_or_init_connection(&cfg1).expect("workspace 1");
    let arc2 = get_or_init_connection(&cfg2).expect("workspace 2");
    assert!(
        !Arc::ptr_eq(&arc1, &arc2),
        "different workspaces must have independent connections"
    );

    let c = sample_chunk("s", 0, 1_700_000_000_000);
    upsert_chunks(&cfg1, std::slice::from_ref(&c)).unwrap();
    assert_eq!(count_chunks(&cfg1).unwrap(), 1);
    assert_eq!(count_chunks(&cfg2).unwrap(), 0);
}

#[test]
fn circuit_breaker_trips_after_threshold() {
    let tmp = TempDir::new().expect("tempdir");

    // Create a regular file where the memory_tree *directory* would be.
    let blocker = tmp.path().join(DB_DIR);
    std::fs::write(&blocker, b"not a dir").expect("write blocker file");

    let cfg = super::test_support::host_config(tmp.path());

    for i in 0..CB_THRESHOLD {
        let result = get_or_init_connection(&cfg);
        assert!(
            result.is_err(),
            "call {i}: expected error before breaker trips"
        );
    }

    let cb_err =
        get_or_init_connection(&cfg).expect_err("expected circuit breaker error after threshold");
    let msg = format!("{cb_err:#}").to_ascii_lowercase();
    assert!(
        msg.contains("circuit breaker"),
        "expected circuit breaker message, got: {msg}"
    );
}

#[test]
fn stale_shm_cleanup_removes_files() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("chunks.db");

    std::fs::write(&db_path, b"").expect("create db file");
    let shm = tmp.path().join("chunks.db-shm");
    let wal = tmp.path().join("chunks.db-wal");
    std::fs::write(&shm, b"stale shm").expect("create shm");
    std::fs::write(&wal, b"stale wal").expect("create wal");

    let cleaned = try_cleanup_stale_files(&db_path);
    assert!(
        cleaned,
        "cleanup should return true when files were removed"
    );
    assert!(!shm.exists(), "shm must be removed");
    assert!(!wal.exists(), "wal must be removed");
}

#[test]
fn memory_tree_uses_truncate_journal_not_wal() {
    let (_tmp, cfg) = test_config();

    with_connection(&cfg, |conn| {
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        assert!(
            mode.eq_ignore_ascii_case("truncate"),
            "journal_mode must be TRUNCATE, got '{mode}'"
        );
        let sync: i64 = conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
        assert_eq!(sync, 2, "rollback journal requires synchronous=FULL (2)");
        Ok(())
    })
    .expect("with_connection");

    let shm = cfg.workspace.join("memory_tree").join("chunks.db-shm");
    assert!(
        !shm.exists(),
        "no -shm file must exist under TRUNCATE journal"
    );
}

#[test]
fn existing_wal_db_migrates_to_truncate() {
    let (_tmp, cfg) = test_config();
    let db_path = cfg.workspace.join("memory_tree").join("chunks.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).expect("mkdir");

    {
        let conn = rusqlite::Connection::open(&db_path).expect("open wal db");
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .expect("set wal");
        assert!(mode.eq_ignore_ascii_case("wal"), "precondition: db in WAL");
        conn.execute_batch("CREATE TABLE legacy_marker(x); INSERT INTO legacy_marker VALUES (1);")
            .expect("seed");
    }

    clear_connection_cache_for(&cfg);
    with_connection(&cfg, |conn| {
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        assert!(
            mode.eq_ignore_ascii_case("truncate"),
            "WAL db must migrate to TRUNCATE, got '{mode}'"
        );
        let marker: i64 = conn.query_row("SELECT x FROM legacy_marker", [], |r| r.get(0))?;
        assert_eq!(marker, 1, "row committed under WAL must survive migration");
        Ok(())
    })
    .expect("with_connection migrates");

    assert!(
        !db_path.with_file_name("chunks.db-shm").exists(),
        "-shm must be gone"
    );
    assert!(
        !db_path.with_file_name("chunks.db-wal").exists(),
        "-wal must be gone"
    );
}

#[cfg(unix)]
#[test]
fn delete_chunks_by_source_removes_symlink_entry_not_target_file() {
    let (_tmp, cfg) = test_config();
    let linked_chunk = sample_chunk("slack:c-1", 0, 1_700_000_000_000);
    upsert_staged_chunks(
        &cfg,
        &[StagedChunk {
            chunk: linked_chunk.clone(),
            content_path: "chunks/link.md".into(),
            content_sha256: "link-hash".into(),
        }],
    )
    .unwrap();

    let root = content_root(&cfg);
    let target_path = root.join("chunks/target.md");
    let link_rel = "chunks/link.md";
    let link_path = root.join(link_rel);
    std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
    std::fs::write(&target_path, "target").unwrap();
    std::os::unix::fs::symlink("target.md", &link_path).unwrap();

    let deleted = delete_chunks_by_source(&cfg, SourceKind::Chat, "slack:c-1").unwrap();

    assert_eq!(deleted, 1);
    assert!(target_path.exists());
    assert!(!link_path.exists());
}
