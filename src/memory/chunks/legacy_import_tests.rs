use super::*;
use chrono::{TimeZone, Utc};

fn chunk() -> Chunk {
    let timestamp = Utc.timestamp_millis_opt(1_750_000_000_000).unwrap();
    Chunk {
        id: super::super::types::chunk_id(SourceKind::Chat, "legacy-source", 0, "legacy body"),
        content: "legacy body".into(),
        metadata: super::super::types::Metadata {
            source_kind: SourceKind::Chat,
            source_id: "legacy-source".into(),
            owner: "legacy-owner".into(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: vec!["legacy".into()],
            source_ref: Some(super::super::types::SourceRef::new("legacy://source/1")),
            path_scope: None,
        },
        token_count: 2,
        seq_in_source: 0,
        created_at: timestamp,
        partial_message: false,
    }
}

fn tree_and_summary(chunk: &Chunk) -> (Tree, SummaryNode) {
    let timestamp = chunk.created_at;
    let tree = Tree {
        id: "legacy-tree".into(),
        kind: crate::memory::tree::store::TreeKind::Source,
        scope: chunk.metadata.source_id.clone(),
        root_id: Some("legacy-summary".into()),
        max_level: 1,
        status: crate::memory::tree::store::TreeStatus::Active,
        created_at: timestamp,
        last_sealed_at: Some(timestamp),
        ask: None,
    };
    let summary = SummaryNode {
        id: "legacy-summary".into(),
        tree_id: tree.id.clone(),
        tree_kind: tree.kind,
        level: 1,
        parent_id: None,
        child_ids: vec![chunk.id.clone()],
        content: "Imported summary".into(),
        token_count: 2,
        entities: vec!["person:ada".into()],
        topics: vec!["migration".into()],
        time_range_start: timestamp,
        time_range_end: timestamp,
        score: 0.8,
        sealed_at: timestamp,
        deleted: false,
        embedding: None,
        doc_id: None,
        version_ms: None,
    };
    (tree, summary)
}

fn fixture_batch() -> LegacyImportBatch {
    let chunk = chunk();
    let (tree, summary) = tree_and_summary(&chunk);
    let occurrence = LegacyEntityOccurrence {
        entity_id: "person:ada".into(),
        node_id: chunk.id.clone(),
        node_kind: "leaf".into(),
        entity_kind: "person".into(),
        surface: "Ada".into(),
        score: 0.9,
        timestamp_ms: chunk.created_at.timestamp_millis(),
        tree_id: Some(tree.id.clone()),
        is_user: false,
    };
    let job = ImportedJob {
        id: "legacy-extract-job".into(),
        kind: crate::memory::queue::JobKind::ExtractChunk,
        payload_json: serde_json::to_string(&crate::memory::queue::ExtractChunkPayload {
            chunk_id: chunk.id.clone(),
        })
        .unwrap(),
        dedupe_key: None,
        status: crate::memory::queue::JobStatus::Ready,
        attempts: 2,
        max_attempts: 7,
        available_at_ms: 1_749_999_999_000,
        created_at_ms: 1_749_000_000_000,
        started_at_ms: Some(1_749_999_000_000),
        completed_at_ms: None,
        last_error: Some("historical retry".into()),
        failure_reason: None,
        failure_class: None,
    };
    LegacyImportBatch {
        chunks: vec![LegacyChunkRecord {
            chunk,
            content: Some(LegacyContentPointer {
                path: "chat/legacy-source/chunk.md".into(),
                sha256: "a".repeat(64),
            }),
            raw_refs: vec![RawRef {
                path: "raw/legacy-source.md".into(),
                start: 2,
                end: Some(10),
            }],
            score: Some(ScoreRow {
                chunk_id: "legacy-chunk-id-is-replaced-below".into(),
                total: 0.7,
                signals: Default::default(),
                dropped: false,
                reason: Some("legacy rationale".into()),
                computed_at_ms: 1_750_000_000_000,
                llm_importance_reason: None,
            }),
            lifecycle_status: Some("pending_extraction".into()),
            embeddings: vec![
                LegacyEmbedding {
                    model_signature: "fixture/model@1".into(),
                    vector: vec![0.25, 0.5],
                    created_at_ms: 1_750_000_000_000,
                },
                LegacyEmbedding {
                    model_signature: "fixture/model@2".into(),
                    vector: vec![0.125, 0.25, 0.5],
                    created_at_ms: 1_749_999_999_000,
                },
            ],
            entities: vec![occurrence.clone()],
        }],
        trees: vec![tree.clone()],
        summaries: vec![summary],
        summary_content: vec![LegacySummaryContent {
            summary_id: "legacy-summary".into(),
            path: "summaries/legacy-source/l1.md".into(),
            sha256: "b".repeat(64),
        }],
        summary_embeddings: vec![LegacySummaryEmbedding {
            summary_id: "legacy-summary".into(),
            model_signature: "fixture/summary-model@1".into(),
            vector: vec![0.75, 1.0, 1.25],
            created_at_ms: 1_750_000_000_123,
        }],
        entities: Vec::new(),
        edges: vec![LegacyEntityEdge {
            entity_a: "person:ada".into(),
            entity_b: "topic:migration".into(),
            weight: 3,
            updated_at_ms: 1_750_000_000_000,
        }],
        source_gates: vec![LegacySourceGate {
            source_kind: SourceKind::Chat,
            source_id: "legacy-source".into(),
            ingested_at_ms: 1_750_000_000_000,
        }],
        jobs: vec![job],
    }
}

#[tokio::test]
async fn typed_import_commits_chunk_adjuncts_tree_entities_and_jobs_together() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let mut batch = fixture_batch();
    batch.chunks[0].score.as_mut().unwrap().chunk_id = batch.chunks[0].chunk.id.clone();
    let chunk_id = batch.chunks[0].chunk.id.clone();

    let counts = import_legacy_batch(&config, &batch).await.unwrap();

    assert_eq!(counts.chunks, 1);
    assert_eq!(counts.scores, 1);
    assert_eq!(counts.entities, 1);
    assert_eq!(counts.trees, 2);
    assert_eq!(counts.jobs, 1);
    assert_eq!(
        super::super::versioned::get_chunk_async(&config, &chunk_id)
            .await
            .unwrap(),
        Some(batch.chunks[0].chunk.clone())
    );
    assert_eq!(
        super::super::versioned::read_state_async(&config, c::RAW_REFS, &chunk_id)
            .await
            .unwrap()
            .unwrap()[0]["path"],
        "raw/legacy-source.md"
    );
    assert!(
        super::super::versioned::read_state_async(&config, c::SCORES, &chunk_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(super::super::versioned::read_state_async(
        &config,
        c::CHUNK_EMBEDDINGS,
        &format!("[\"{chunk_id}\",\"fixture/model@1\"]")
    )
    .await
    .unwrap()
    .is_some());
    assert_eq!(
        super::super::get_chunk_embedding_for_signature_async(
            &config,
            &chunk_id,
            "fixture/model@1"
        )
        .await
        .unwrap(),
        Some(vec![0.25, 0.5])
    );
    assert!(super::super::versioned::read_state_async(
        &config,
        c::CHUNK_EMBEDDINGS,
        &format!("[\"{chunk_id}\",\"fixture/model@2\"]")
    )
    .await
    .unwrap()
    .is_some());
    let summary_embedding = super::super::versioned::read_state_async(
        &config,
        c::SUMMARY_EMBEDDINGS,
        "[\"legacy-summary\",\"fixture/summary-model@1\"]",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary_embedding["dim"], 3);
    assert_eq!(summary_embedding["created_at_ms"], 1_750_000_000_123i64);
    assert!(super::super::versioned::read_state_async(
        &config,
        c::ENTITY_INDEX,
        &format!("occ:{}:{}", hex("person:ada"), hex(&chunk_id))
    )
    .await
    .unwrap()
    .is_some());
    assert!(
        super::super::versioned::read_state_async(&config, c::TREES, "tree:legacy-tree")
            .await
            .unwrap()
            .is_some()
    );
    assert!(super::super::versioned::read_state_async(
        &config,
        c::SUMMARIES,
        "summary:legacy-summary"
    )
    .await
    .unwrap()
    .is_some());
    assert!(super::super::versioned::read_state_async(
        &config,
        c::INGESTED_SOURCES,
        "chat\0legacy-source"
    )
    .await
    .unwrap()
    .is_some());
    let imported_job = crate::memory::queue::store::get_job_async(&config, "legacy-extract-job")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(imported_job.created_at_ms, 1_749_000_000_000);
    assert_eq!(imported_job.attempts, 2);
    assert_eq!(imported_job.max_attempts, 7);
    assert_eq!(imported_job.last_error.as_deref(), Some("historical retry"));

    let before_retry = super::super::versioned::capture_state_snapshot_async(&config)
        .await
        .unwrap();
    let repeated = import_legacy_batch(&config, &batch).await.unwrap();
    let after_retry = super::super::versioned::capture_state_snapshot_async(&config)
        .await
        .unwrap();
    assert_eq!(repeated.chunks, 1);
    assert_eq!(
        before_retry, after_retry,
        "an idempotent retry must not create another owner commit"
    );
}

#[tokio::test]
async fn typed_import_rejects_path_escape_before_writes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let mut batch = fixture_batch();
    batch.chunks[0].content.as_mut().unwrap().path = "../escape.md".into();

    assert!(import_legacy_batch(&config, &batch).await.is_err());
    assert!(
        super::super::versioned::list_chunk_snapshot_with_head_async(&config, &[])
            .await
            .unwrap()
            .1
            .is_empty()
    );
}

#[tokio::test]
async fn typed_import_rejects_malformed_existing_content_pointer_without_mutation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let batch = fixture_batch();
    let chunk_id = batch.chunks[0].chunk.id.clone();
    let pointer = StateChange {
        collection: super::super::collections::CHUNK_CONTENT_POINTERS.to_owned(),
        key: chunk_id.clone(),
        value: Some(serde_json::json!({
            "chunk_id": chunk_id,
            "content_path": "../escape.md",
            "content_sha256": "a".repeat(64),
        })),
    };
    assert!(super::super::versioned::commit_if_state_matches_async(
        &config,
        &[],
        &[],
        std::slice::from_ref(&pointer),
    )
    .await
    .unwrap());
    let before = super::super::versioned::capture_state_snapshot_async(&config)
        .await
        .unwrap();

    let error = import_legacy_batch(&config, &batch).await.unwrap_err();

    assert!(error.to_string().contains("unsafe path or invalid SHA-256"));
    assert_eq!(
        super::super::versioned::capture_state_snapshot_async(&config)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn typed_import_rejects_conflicting_existing_entity_occurrence_without_mutating_it() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let mut batch = fixture_batch();
    batch.chunks[0].score.as_mut().unwrap().chunk_id = batch.chunks[0].chunk.id.clone();

    let original = LegacyEntityOccurrence {
        entity_id: "person:ada".into(),
        node_id: batch.chunks[0].chunk.id.clone(),
        node_kind: "leaf".into(),
        entity_kind: "person".into(),
        surface: "A. Lovelace".into(),
        score: 0.8,
        timestamp_ms: 100,
        tree_id: None,
        is_user: false,
    };
    let key = format!("occ:{}:{}", hex("person:ada"), hex(&original.node_id));
    let original_change = super::super::versioned::StateChange {
        collection: c::ENTITY_INDEX.into(),
        key: key.clone(),
        value: Some(serde_json::to_value(&original).unwrap()),
    };
    let snapshot = super::super::versioned::capture_state_snapshot_async(&config)
        .await
        .unwrap();
    super::super::versioned::commit_if_state_matches_at_snapshot_async(
        &config,
        &snapshot,
        &[],
        &[],
        &[original_change],
    )
    .await
    .unwrap();
    let before = super::super::versioned::read_state_async(&config, c::ENTITY_INDEX, &key)
        .await
        .unwrap();

    assert!(import_legacy_batch(&config, &batch).await.is_err());
    assert_eq!(
        super::super::versioned::read_state_async(&config, c::ENTITY_INDEX, &key)
            .await
            .unwrap(),
        before
    );
    assert!(
        super::super::versioned::get_chunk_async(&config, &batch.chunks[0].chunk.id)
            .await
            .unwrap()
            .is_none()
    );
}

fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
