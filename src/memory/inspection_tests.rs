use super::*;
use crate::memory::chunks::{upsert_chunks, SourceKind};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tempfile::TempDir;

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
    upsert_chunks(&config, &[chunk.clone()]).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let snapshot = runtime
        .block_on(read_snapshot(workspace.path(), storage, 50))
        .unwrap();

    assert_eq!(snapshot.skill_documents.len(), 1);
    assert_eq!(snapshot.skill_documents[0].namespace.as_deref(), Some("skilldoc_notion"));
    assert_eq!(snapshot.skill_documents[0].key, "document-1");
    assert_eq!(snapshot.chunks, vec![chunk]);
    assert_eq!(snapshot.chunk_total, 1);
    assert!(snapshot.trees.is_empty());
    assert!(snapshot.summaries.is_empty());
}
