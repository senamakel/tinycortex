use super::*;
use crate::memory::chunks::{upsert_chunks, SourceKind};
use chrono::{TimeZone, Utc};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tempfile::TempDir;
use tinystoragedrivers_api::{DocumentStore, Storage};
use tinystoragedrivers_types::{
    Capabilities, CollectionSpec, Filter, Page, Precondition, Query, SearchHit, Sort, StorageError,
    Version, Versioned,
};

#[derive(Debug)]
struct EnsureRefusingDocuments {
    inner: Arc<dyn DocumentStore>,
    ensure_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl DocumentStore for EnsureRefusingDocuments {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(
        &self,
        spec: &CollectionSpec,
    ) -> tinystoragedrivers_types::Result<()> {
        let _ = spec;
        self.ensure_calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::backend(
            "inspection must not prepare collections",
        ))
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_types::Result<Option<Versioned<serde_json::Value>>> {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: serde_json::Value,
        precondition: Precondition,
    ) -> tinystoragedrivers_types::Result<Version> {
        let _ = (collection, id, value, precondition);
        Err(StorageError::backend("inspection must not write documents"))
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> tinystoragedrivers_types::Result<bool> {
        let _ = (collection, id, precondition);
        Err(StorageError::backend(
            "inspection must not delete documents",
        ))
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> tinystoragedrivers_types::Result<Page<Versioned<serde_json::Value>>> {
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        let _ = (collection, filter);
        Err(StorageError::backend(
            "inspection must not delete documents",
        ))
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &serde_json::Value,
    ) -> tinystoragedrivers_types::Result<Option<Versioned<serde_json::Value>>> {
        let _ = (collection, filter, sort, patch);
        Err(StorageError::backend("inspection must not claim documents"))
    }

    async fn search(
        &self,
        collection: &str,
        text: &str,
        limit: usize,
    ) -> tinystoragedrivers_types::Result<Vec<SearchHit>> {
        self.inner.search(collection, text, limit).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_types::Result<()> {
        let _ = collection;
        Err(StorageError::backend(
            "inspection must not drop collections",
        ))
    }
}

fn refuse_document_writes(storage: &Storage) -> (Storage, Arc<AtomicUsize>) {
    let ensure_calls = Arc::new(AtomicUsize::new(0));
    let documents = Arc::new(EnsureRefusingDocuments {
        inner: storage.documents().clone(),
        ensure_calls: ensure_calls.clone(),
    });
    (
        Storage::new(
            documents,
            storage.streams().clone(),
            storage.kv().clone(),
            storage.blobs().clone(),
            storage.queues().clone(),
            storage.secrets().clone(),
        ),
        ensure_calls,
    )
}

#[test]
fn snapshot_reads_owner_kv_and_chunk_records_from_explicit_storage() {
    let workspace = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let storage = config.storage().unwrap().clone();
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_namespace(
        "skilldoc_notion",
        "document-1",
        &json!({
            "toolkit": "notion",
            "document_id": "document-1",
            "title": "Owner stored",
            "content": "loaded through the KV owner",
            "metadata": {"source": "test"}
        }),
    )
    .unwrap();

    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let chunk = Chunk {
        id: "chunk-1".into(),
        content: "owner chunk preview".into(),
        metadata: crate::memory::chunks::Metadata {
            source_kind: SourceKind::Document,
            source_id: "notion:document-1".into(),
            owner: "owner".into(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: vec!["skill".into()],
            source_ref: None,
            path_scope: None,
        },
        token_count: 4,
        seq_in_source: 0,
        created_at: timestamp,
        partial_message: false,
    };
    upsert_chunks(&config, std::slice::from_ref(&chunk)).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (inspection_storage, ensure_calls) = refuse_document_writes(&storage);
    let snapshot = runtime
        .block_on(read_snapshot(workspace.path(), inspection_storage, 50))
        .unwrap();

    assert_eq!(snapshot.skill_documents.len(), 1);
    assert_eq!(
        snapshot.skill_documents[0].namespace.as_deref(),
        Some("skilldoc_notion")
    );
    assert_eq!(snapshot.skill_documents[0].key, "document-1");
    assert_eq!(snapshot.chunks, vec![chunk]);
    assert_eq!(snapshot.chunk_total, 1);
    assert!(snapshot.trees.is_empty());
    assert!(snapshot.summaries.is_empty());
    assert_eq!(ensure_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn snapshot_includes_summary_chunks_outside_the_newest_page() {
    use crate::memory::chunks::versioned::commit_if_state_matches_async;
    use crate::memory::tree::store::{insert_tree_async, SummaryNode, Tree, TreeKind, TreeStatus};

    let workspace = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let storage = config.storage().unwrap().clone();
    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let referenced = Chunk {
        id: "chunk-old-referenced".into(),
        content: "referenced by a summary".into(),
        metadata: crate::memory::chunks::Metadata {
            source_kind: SourceKind::Document,
            source_id: "source-old".into(),
            owner: "owner".into(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: Vec::new(),
            source_ref: None,
            path_scope: None,
        },
        token_count: 5,
        seq_in_source: 0,
        created_at: timestamp,
        partial_message: false,
    };
    let newest = Chunk {
        id: "chunk-newest".into(),
        content: "newest page item".into(),
        metadata: crate::memory::chunks::Metadata {
            source_kind: SourceKind::Document,
            source_id: "source-new".into(),
            owner: "owner".into(),
            timestamp: timestamp + chrono::Duration::seconds(1),
            time_range: (
                timestamp + chrono::Duration::seconds(1),
                timestamp + chrono::Duration::seconds(1),
            ),
            tags: Vec::new(),
            source_ref: None,
            path_scope: None,
        },
        token_count: 4,
        seq_in_source: 0,
        created_at: timestamp + chrono::Duration::seconds(1),
        partial_message: false,
    };
    upsert_chunks(&config, &[referenced.clone(), newest.clone()]).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let tree = Tree {
            id: "tree-inspection".into(),
            kind: TreeKind::Source,
            scope: "source-old".into(),
            root_id: Some("summary-inspection".into()),
            max_level: 1,
            status: TreeStatus::Active,
            created_at: timestamp,
            last_sealed_at: Some(timestamp),
            ask: None,
        };
        insert_tree_async(&config, &tree).await.unwrap();
        let summary = SummaryNode {
            id: "summary-inspection".into(),
            tree_id: tree.id,
            tree_kind: TreeKind::Source,
            level: 1,
            parent_id: None,
            child_ids: vec![referenced.id.clone()],
            content: "An older chunk remains part of the summary graph".into(),
            token_count: 9,
            entities: Vec::new(),
            topics: Vec::new(),
            time_range_start: timestamp,
            time_range_end: timestamp,
            score: 0.0,
            sealed_at: timestamp,
            deleted: false,
            embedding: None,
            doc_id: None,
            version_ms: None,
        };
        let staged =
            crate::memory::tree::store::stage_summary_changes(&summary, None, "test/model")
                .unwrap();
        assert!(
            commit_if_state_matches_async(&config, &staged.expected, &[], &staged.changes,)
                .await
                .unwrap()
        );

        let snapshot = read_snapshot(workspace.path(), storage, 1).await.unwrap();
        assert_eq!(snapshot.chunks, vec![newest]);
        assert_eq!(snapshot.graph_chunks, vec![referenced]);
        assert_eq!(snapshot.chunk_total, 2);
    });
}
