use super::*;
use crate::memory::chunks::types::{Metadata, SourceKind};
use chrono::{TimeZone, Utc};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinystoragedrivers_types::{Query, Scope};

#[derive(Debug)]
struct LoseHeadAck {
    inner: std::sync::Arc<dyn tinystoragedrivers_api::DocumentStore>,
    lose_next_head_ack: AtomicBool,
    get_calls: std::sync::Arc<AtomicUsize>,
    chunk_version_queries: std::sync::Arc<AtomicUsize>,
    state_version_queries: std::sync::Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl tinystoragedrivers_api::DocumentStore for LoseHeadAck {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(
        &self,
        spec: &tinystoragedrivers_types::CollectionSpec,
    ) -> tinystoragedrivers_types::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn validate_collection_spec(
        &self,
        required: &tinystoragedrivers_types::CollectionSpec,
    ) -> tinystoragedrivers_types::Result<()> {
        self.inner.validate_collection_spec(required).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_types::Result<
        Option<tinystoragedrivers_types::Versioned<serde_json::Value>>,
    > {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: serde_json::Value,
        precondition: Precondition,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::Version> {
        self.inner.put(collection, id, value, precondition).await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: serde_json::Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::Version> {
        let version = self
            .inner
            .put_with(collection, id, value, precondition, options)
            .await?;
        if collection == COMMIT_HEADS && self.lose_next_head_ack.swap(false, Ordering::SeqCst) {
            return Err(tinystoragedrivers_types::StorageError::new(
                tinystoragedrivers_types::ErrorKind::Unavailable,
                "injected lost acknowledgement after head commit",
            ));
        }
        Ok(version)
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> tinystoragedrivers_types::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> tinystoragedrivers_types::Result<
        tinystoragedrivers_types::Page<tinystoragedrivers_types::Versioned<serde_json::Value>>,
    > {
        if collection == CHUNK_VERSIONS {
            self.chunk_version_queries.fetch_add(1, Ordering::SeqCst);
        }
        if collection == STATE_VERSIONS {
            self.state_version_queries.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
        sort: &[tinystoragedrivers_types::Sort],
        patch: &serde_json::Value,
    ) -> tinystoragedrivers_types::Result<
        Option<tinystoragedrivers_types::Versioned<serde_json::Value>>,
    > {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_types::Result<()> {
        self.inner.drop_collection(collection).await
    }
}

fn chunk(id: &str, content: &str) -> Chunk {
    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    Chunk {
        id: id.to_owned(),
        content: content.to_owned(),
        metadata: Metadata::point_in_time(SourceKind::Chat, "room", "owner", timestamp),
        token_count: 2,
        seq_in_source: 0,
        created_at: timestamp,
        partial_message: false,
    }
}

async fn config() -> MemoryConfig {
    let scope = Scope::new("test:chunk-versions").unwrap();
    MemoryConfig::open_with_storage(
        "workspace",
        tinystoragedrivers_testkit::memory_storage(&scope),
    )
    .await
    .unwrap()
}

async fn config_with_document_faults() -> (MemoryConfig, tinystoragedrivers_testkit::FaultPlan) {
    use tinystoragedrivers_api::Storage;
    use tinystoragedrivers_testkit::Failing;
    let scope = Scope::new("test:chunk-version-faults").unwrap();
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
    (
        MemoryConfig::open_with_storage("workspace", storage)
            .await
            .unwrap(),
        faults,
    )
}

async fn config_with_lost_head_ack() -> MemoryConfig {
    use tinystoragedrivers_api::Storage;
    let scope = Scope::new("test:chunk-lost-ack").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let documents = std::sync::Arc::new(LoseHeadAck {
        inner: raw.documents().clone(),
        lose_next_head_ack: AtomicBool::new(true),
        get_calls: std::sync::Arc::new(AtomicUsize::new(0)),
        chunk_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
        state_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
    });
    let storage = Storage::new(
        documents,
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    MemoryConfig::open_with_storage("workspace", storage)
        .await
        .unwrap()
}

async fn config_with_get_counter() -> (MemoryConfig, std::sync::Arc<AtomicUsize>) {
    use tinystoragedrivers_api::Storage;
    let scope = Scope::new("test:chunk-read-call-bound").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let get_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let documents = std::sync::Arc::new(LoseHeadAck {
        inner: raw.documents().clone(),
        lose_next_head_ack: AtomicBool::new(false),
        get_calls: get_calls.clone(),
        chunk_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
        state_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
    });
    let storage = Storage::new(
        documents,
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    (
        MemoryConfig::open_with_storage("workspace", storage)
            .await
            .unwrap(),
        get_calls,
    )
}

async fn config_with_chunk_version_query_counter() -> (MemoryConfig, std::sync::Arc<AtomicUsize>) {
    use tinystoragedrivers_api::Storage;
    let scope = Scope::new("test:chunk-list-query-count").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let query_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let documents = std::sync::Arc::new(LoseHeadAck {
        inner: raw.documents().clone(),
        lose_next_head_ack: AtomicBool::new(false),
        get_calls: std::sync::Arc::new(AtomicUsize::new(0)),
        chunk_version_queries: query_calls.clone(),
        state_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
    });
    let storage = Storage::new(
        documents,
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    (
        MemoryConfig::open_with_storage("workspace", storage)
            .await
            .unwrap(),
        query_calls,
    )
}

async fn config_with_state_version_query_counter() -> (MemoryConfig, std::sync::Arc<AtomicUsize>) {
    use tinystoragedrivers_api::Storage;
    let scope = Scope::new("test:chunk-state-list-query-count").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let query_calls = std::sync::Arc::new(AtomicUsize::new(0));
    let documents = std::sync::Arc::new(LoseHeadAck {
        inner: raw.documents().clone(),
        lose_next_head_ack: AtomicBool::new(false),
        get_calls: std::sync::Arc::new(AtomicUsize::new(0)),
        chunk_version_queries: std::sync::Arc::new(AtomicUsize::new(0)),
        state_version_queries: query_calls.clone(),
    });
    let storage = Storage::new(
        documents,
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    (
        MemoryConfig::open_with_storage("workspace", storage)
            .await
            .unwrap(),
        query_calls,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_chunks_resolves_versions_in_bounded_queries() {
    let (config, version_queries) = config_with_chunk_version_query_counter().await;
    let chunks = (0..64)
        .map(|index| chunk(&format!("chunk-{index:04}"), "listing fixture"))
        .collect::<Vec<_>>();
    super::super::store::upsert_chunks(&config, &chunks).unwrap();

    let listed = super::list_chunks(&config).unwrap();

    assert_eq!(listed.len(), chunks.len());
    assert!(chunks.iter().all(|chunk| listed.contains(chunk)));
    assert!(
        version_queries.load(Ordering::SeqCst) <= 4,
        "listing {} chunks issued {} per-version queries",
        chunks.len(),
        version_queries.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_chunk_state_resolves_many_lifecycle_keys_in_bounded_queries() {
    let (config, state_version_queries) = config_with_state_version_query_counter().await;
    let chunks = (0..64)
        .map(|index| chunk(&format!("state-list-{index:04}"), "state listing fixture"))
        .collect::<Vec<_>>();
    super::super::store::upsert_chunks(&config, &chunks).unwrap();
    let score_rows = chunks
        .iter()
        .map(|chunk| StateChange {
            collection: super::super::collections::SCORES.into(),
            key: chunk.id.clone(),
            value: Some(serde_json::json!({"chunk_id":chunk.id,"total":0.5})),
        })
        .chain(chunks.iter().map(|chunk| StateChange {
            collection: CHUNK_LIFECYCLE.into(),
            key: chunk.id.clone(),
            value: Some(serde_json::json!({"status":"active"})),
        }))
        .collect::<Vec<_>>();
    assert!(commit_if_state_matches(&config, &[], &[], &score_rows).unwrap());
    state_version_queries.store(0, Ordering::SeqCst);

    let (listed, states) = list_chunk_snapshot(
        &config,
        &[CHUNK_LIFECYCLE, super::super::collections::SCORES],
    )
    .unwrap();

    assert_eq!(listed.len(), chunks.len());
    assert_eq!(states[CHUNK_LIFECYCLE].len(), chunks.len());
    assert_eq!(
        states[super::super::collections::SCORES].len(),
        chunks.len()
    );
    assert!(
        state_version_queries.load(Ordering::SeqCst) <= 8,
        "listing {} lifecycle and score keys issued {} state-version queries",
        chunks.len(),
        state_version_queries.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upserting_many_chunks_checks_deleted_lifecycles_in_bounded_queries() {
    let (config, state_version_queries) = config_with_state_version_query_counter().await;
    let chunks = (0..64)
        .map(|index| chunk(&format!("lifecycle-write-{index:04}"), "write fixture"))
        .collect::<Vec<_>>();

    assert_eq!(super::super::store::upsert_chunks(&config, &chunks).unwrap(), chunks.len());

    assert!(
        state_version_queries.load(Ordering::SeqCst) <= 8,
        "upserting {} chunks issued {} per-key lifecycle queries",
        chunks.len(),
        state_version_queries.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_chunk_write_and_read_use_the_host_document_store() {
    let config = config().await;
    let original = chunk("chunk-1", "first");

    assert_eq!(
        super::super::store::upsert_chunks(&config, std::slice::from_ref(&original)).unwrap(),
        1
    );
    assert_eq!(
        super::super::store::get_chunk(&config, &original.id).unwrap(),
        Some(original)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_batch_read_returns_only_committed_chunks() {
    let config = config().await;
    let first = chunk("chunk-1", "first");
    let second = chunk("chunk-2", "second");
    super::super::store::upsert_chunks(&config, &[first.clone(), second.clone()]).unwrap();

    let found = super::super::store::get_chunks_batch(
        &config,
        &["chunk-1".into(), "chunk-2".into(), "absent".into()],
    )
    .unwrap();

    assert_eq!(found.len(), 2);
    assert_eq!(found.get("chunk-1"), Some(&first));
    assert_eq!(found.get("chunk-2"), Some(&second));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_embedding_commit_clears_its_skip_in_the_same_head() {
    let config = config().await;
    let chunk = chunk("chunk-embedding", "embedding fixture");
    super::super::store::upsert_chunks(&config, std::slice::from_ref(&chunk)).unwrap();
    let signature = "openai/test-model@2";
    super::super::embeddings::mark_chunk_reembed_skipped(
        &config,
        "chunk-embedding",
        signature,
        "missing content",
    )
    .unwrap();
    let before_embedding = capture_head(&config).unwrap().revision;

    super::super::embeddings::set_chunk_embedding_for_signature(
        &config,
        "chunk-embedding",
        signature,
        &[0.25, -0.5],
    )
    .unwrap();
    assert_eq!(
        capture_head(&config).unwrap().revision,
        before_embedding + 1
    );

    assert_eq!(
        super::super::embeddings_query::get_chunk_embedding_for_signature(
            &config,
            "chunk-embedding",
            signature,
        )
        .unwrap(),
        Some(vec![0.25, -0.5])
    );
    assert_eq!(
        read_state(
            &config,
            super::super::collections::CHUNK_REEMBED_SKIPS,
            &super::super::embeddings::chunk_embedding_key("chunk-embedding", signature),
        )
        .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_chunk_preview_and_content_pointer_commit_together() {
    let config = config().await;
    let mut staged_chunk = chunk("chunk-staged", &"x".repeat(700));
    staged_chunk.content = format!("{}body", "p".repeat(700));
    let staged = crate::memory::chunks::types::StagedChunk {
        chunk: staged_chunk,
        content_path: "chunks/source/chunk.md".into(),
        content_sha256: "sha256-fixture".into(),
    };

    super::super::store::upsert_staged_chunks(&config, std::slice::from_ref(&staged)).unwrap();

    let stored = super::super::store::get_chunk(&config, "chunk-staged")
        .unwrap()
        .unwrap();
    assert_eq!(stored.content, "p".repeat(500));
    assert_eq!(
        super::super::raw_refs::get_chunk_content_pointers(&config, "chunk-staged").unwrap(),
        Some(("chunks/source/chunk.md".into(), "sha256-fixture".into()))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_chunk_read_works_on_current_thread_runtime() {
    let config = config().await;

    assert_eq!(
        super::super::store::get_chunk_async(&config, "absent")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn updated_chunk_keeps_old_snapshot_while_new_head_is_visible() {
    let config = config().await;
    let old = chunk("chunk-1", "before");
    let new = chunk("chunk-1", "after");
    super::super::store::upsert_chunks(&config, std::slice::from_ref(&old)).unwrap();
    let snapshot = capture_head(&config).unwrap();

    super::super::store::upsert_chunks(&config, std::slice::from_ref(&new)).unwrap();

    let docs = config.storage().unwrap().documents().clone();
    let old_at_snapshot = config
        .storage_blocking()
        .unwrap()
        .run(async move { read_chunk_at(&*docs, &snapshot, "chunk-1").await })
        .unwrap()
        .unwrap();
    assert_eq!(old_at_snapshot, Some(old));
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(new)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retrying_current_committed_batch_is_idempotent() {
    let config = config().await;
    let value = chunk("chunk-1", "same");
    assert_eq!(
        super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).unwrap(),
        1
    );
    let first_snapshot = capture_head(&config).unwrap();
    let docs = config.storage().unwrap().documents().clone();
    let (first_operations, first_versions) = config
        .storage_blocking()
        .unwrap()
        .run(async move {
            let operations = docs.query(OPERATIONS, &Query::all()).await?.items.len();
            let versions = docs.query(CHUNK_VERSIONS, &Query::all()).await?.items.len();
            Ok::<_, tinystoragedrivers_types::StorageError>((operations, versions))
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).unwrap(),
        1
    );

    let snapshot = capture_head(&config).unwrap();
    assert_eq!(snapshot, first_snapshot);
    let docs = config.storage().unwrap().documents().clone();
    let (operations, versions) = config
        .storage_blocking()
        .unwrap()
        .run(async move {
            let operations = docs.query(OPERATIONS, &Query::all()).await?.items.len();
            let versions = docs.query(CHUNK_VERSIONS, &Query::all()).await?.items.len();
            Ok::<_, tinystoragedrivers_types::StorageError>((operations, versions))
        })
        .unwrap()
        .unwrap();
    assert_eq!((operations, versions), (first_operations, first_versions));
    assert_eq!((operations, versions), (1, 1));
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(value)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_and_state_changes_publish_at_the_same_commit_head() {
    let config = config().await;
    let value = chunk("chunk-atomic", "same publication");
    let state = StateChange {
        collection: "memory_ingested_sources".into(),
        key: "document:doc-1".into(),
        value: Some(serde_json::json!({"ingested_at_ms": 1})),
    };

    commit_operation(&config, &[value.clone()], std::slice::from_ref(&state)).unwrap();

    assert_eq!(
        super::super::store::get_chunk(&config, &value.id).unwrap(),
        Some(value)
    );
    assert_eq!(
        read_state(&config, &state.collection, &state.key).unwrap(),
        state.value
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interruption_before_head_keeps_chunk_and_state_changes_invisible() {
    let (config, faults) = config_with_document_faults().await;
    let value = chunk("chunk-atomic", "same publication");
    let state = StateChange {
        collection: "memory_ingested_sources".into(),
        key: "document:doc-1".into(),
        value: Some(serde_json::json!({"ingested_at_ms": 1})),
    };
    faults.fail_operation_nth(
        "documents.put_with",
        4,
        tinystoragedrivers_types::ErrorKind::Backend,
    );

    assert!(commit_operation(&config, &[value.clone()], std::slice::from_ref(&state)).is_err());
    assert_eq!(
        super::super::store::get_chunk(&config, &value.id).unwrap(),
        None
    );
    assert_eq!(
        read_state(&config, &state.collection, &state.key).unwrap(),
        None
    );

    faults.clear();
    commit_operation(&config, &[value.clone()], std::slice::from_ref(&state)).unwrap();
    assert_eq!(
        super::super::store::get_chunk(&config, &value.id).unwrap(),
        Some(value)
    );
    assert_eq!(
        read_state(&config, &state.collection, &state.key).unwrap(),
        state.value
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_commit_cycle_is_rejected_instead_of_looping() {
    let config = config().await;
    let docs = config.storage().unwrap().documents().clone();
    let cycle = OperationManifest {
        op_id: "chunk-write:cycle".into(),
        parent_op: Some("chunk-write:cycle".into()),
        revision: 2,
        ancestors: vec!["chunk-write:cycle".into()],
        writes: Vec::new(),
        state_writes: Vec::new(),
    };
    docs.put_with(
        OPERATIONS,
        "chunk-write:cycle",
        serde_json::to_value(&cycle).unwrap(),
        Precondition::Absent,
        WriteOptions::new().schema(RECORD_SCHEMA),
    )
    .await
    .unwrap();
    let target = OperationManifest {
        op_id: "chunk-write:old".into(),
        parent_op: None,
        revision: 1,
        ancestors: Vec::new(),
        writes: Vec::new(),
        state_writes: Vec::new(),
    };
    let error = manifest_is_ancestor(&*docs, cycle, &target, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("id or revision"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_writer_cannot_publish_over_a_newer_head() {
    let config = config().await;
    let base = chunk("chunk-1", "base");
    super::super::store::upsert_chunks(&config, std::slice::from_ref(&base)).unwrap();
    let stale_snapshot = capture_head(&config).unwrap();
    let winner = chunk("chunk-1", "winner");
    let loser = chunk("chunk-1", "loser");
    let winner_batch = [winner.clone()];
    let loser_batch = [loser];
    let normalized_winner = winner_batch.clone();
    let normalized_loser = loser_batch.clone();

    upsert_chunks_at(
        &config,
        &stale_snapshot,
        &winner_batch,
        &normalized_winner,
        &[],
    )
    .unwrap();
    let error = upsert_chunks_at(
        &config,
        &stale_snapshot,
        &loser_batch,
        &normalized_loser,
        &[],
    )
    .unwrap_err();

    assert!(error.to_string().contains("commit chunk version batch"));
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(winner)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_unconditional_upserts_retry_and_preserve_disjoint_rows() {
    let config = std::sync::Arc::new(config().await);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let first = chunk("chunk-a", "alpha");
    let second = chunk("chunk-b", "beta");
    let first_config = config.clone();
    let first_barrier = barrier.clone();
    let first_thread = std::thread::spawn(move || {
        first_barrier.wait();
        super::super::store::upsert_chunks(&first_config, std::slice::from_ref(&first))
            .map(|_| first)
    });
    let second_config = config.clone();
    let second_barrier = barrier.clone();
    let second_thread = std::thread::spawn(move || {
        second_barrier.wait();
        super::super::store::upsert_chunks(&second_config, std::slice::from_ref(&second))
            .map(|_| second)
    });
    barrier.wait();

    let first = first_thread.join().unwrap().unwrap();
    let second = second_thread.join().unwrap().unwrap();
    assert_eq!(
        super::super::store::get_chunk(&config, &first.id).unwrap(),
        Some(first)
    );
    assert_eq!(
        super::super::store::get_chunk(&config, &second.id).unwrap(),
        Some(second)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_staging_is_invisible_and_retry_commits_once() {
    let (config, faults) = config_with_document_faults().await;
    let value = chunk("chunk-1", "recover me");
    faults.fail_operation_nth(
        "documents.put_with",
        3,
        tinystoragedrivers_types::ErrorKind::Backend,
    );

    assert!(super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).is_err());
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        None
    );

    faults.clear();
    assert_eq!(
        super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).unwrap(),
        1
    );
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(value)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_after_lost_commit_ack_does_not_append_a_second_operation() {
    let config = config_with_lost_head_ack().await;
    let value = chunk("chunk-1", "ack lost");
    assert!(super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).is_err());
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(value.clone())
    );

    let before_retry = capture_head(&config).unwrap();
    let docs = config.storage().unwrap().documents().clone();
    let operation_count = config
        .storage_blocking()
        .unwrap()
        .run(async move {
            docs.query(OPERATIONS, &Query::all())
                .await
                .map(|page| page.items.len())
        })
        .unwrap()
        .unwrap();
    assert_eq!(operation_count, 1);

    assert_eq!(
        super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).unwrap(),
        1
    );
    assert_eq!(capture_head(&config).unwrap(), before_retry);
    let docs = config.storage().unwrap().documents().clone();
    let operation_count = config
        .storage_blocking()
        .unwrap()
        .run(async move {
            docs.query(OPERATIONS, &Query::all())
                .await
                .map(|page| page.items.len())
        })
        .unwrap()
        .unwrap();
    assert_eq!(operation_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_chunk_lookup_uses_logarithmic_commit_ancestry_reads() {
    let (config, get_calls) = config_with_get_counter().await;
    let first = chunk("chunk-1", "old value");
    super::super::store::upsert_chunks(&config, std::slice::from_ref(&first)).unwrap();
    for revision in 0..128 {
        write_state_changes(
            &config,
            &[StateChange {
                collection: "test-state".into(),
                key: format!("key-{revision}"),
                value: Some(serde_json::json!(revision)),
            }],
        )
        .unwrap();
    }

    get_calls.store(0, Ordering::SeqCst);
    assert_eq!(
        super::super::store::get_chunk(&config, "chunk-1").unwrap(),
        Some(first)
    );
    assert!(
        get_calls.load(Ordering::SeqCst) <= 18,
        "expected logarithmic ancestry reads, got {}",
        get_calls.load(Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_refs_are_visible_at_the_same_host_commit_head_as_chunks() {
    let config = config().await;
    let value = chunk("chunk-raw", "email preview");
    super::super::store::upsert_chunks(&config, std::slice::from_ref(&value)).unwrap();

    let refs = vec![super::super::raw_refs::RawRef {
        path: "raw/mail/one.md".into(),
        start: 4,
        end: Some(19),
    }];
    super::super::raw_refs::set_chunk_raw_refs(&config, &value.id, &refs).unwrap();
    assert_eq!(
        super::super::raw_refs::get_chunk_raw_refs(&config, &value.id).unwrap(),
        Some(refs)
    );
    assert!(
        super::super::raw_refs::list_chunk_raw_ref_paths_with_prefix(&config, "raw/mail/")
            .unwrap()
            .contains("raw/mail/one.md")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_ingest_gate_uses_conditional_commit_and_preserves_first_time() {
    let config = config().await;
    use super::super::types::SourceKind;

    assert!(super::super::store_sources::claim_source_ingest(
        &config,
        SourceKind::Document,
        "doc-1",
        111,
    )
    .unwrap());
    assert!(!super::super::store_sources::claim_source_ingest(
        &config,
        SourceKind::Document,
        "doc-1",
        222,
    )
    .unwrap());
    assert!(super::super::store_sources::is_source_ingested(
        &config,
        SourceKind::Document,
        "doc-1"
    )
    .unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_queue_worker_cannot_commit_after_a_newer_claim_epoch() {
    let config = config().await;
    let pending = StateChange {
        collection: "memory_queue_outbox".into(),
        key: "extract:chunk-1".into(),
        value: Some(serde_json::json!({"state": "pending"})),
    };
    let chunk = chunk("chunk-1", "queued body");
    commit_operation(
        &config,
        std::slice::from_ref(&chunk),
        std::slice::from_ref(&pending),
    )
    .unwrap();

    let first_claim = StateChange {
        collection: pending.collection.clone(),
        key: pending.key.clone(),
        value: Some(serde_json::json!({"state": "running", "lease": "old"})),
    };
    assert!(commit_if_state_matches(
        &config,
        std::slice::from_ref(&pending),
        &[],
        std::slice::from_ref(&first_claim),
    )
    .unwrap());

    let newer_claim = StateChange {
        collection: pending.collection.clone(),
        key: pending.key.clone(),
        value: Some(serde_json::json!({"state": "running", "lease": "new"})),
    };
    assert!(commit_if_state_matches(
        &config,
        std::slice::from_ref(&first_claim),
        &[],
        std::slice::from_ref(&newer_claim),
    )
    .unwrap());

    let stale_result = StateChange {
        collection: "test-domain-results".into(),
        key: "chunk-1".into(),
        value: Some(serde_json::json!({"published_by": "old"})),
    };
    assert!(!commit_if_state_matches(
        &config,
        std::slice::from_ref(&first_claim),
        &[],
        std::slice::from_ref(&stale_result),
    )
    .unwrap());
    assert_eq!(
        read_state(&config, "test-domain-results", "chunk-1").unwrap(),
        None
    );
}
