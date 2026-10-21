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
    let mut job = NewJob::extract_chunk(&crate::memory::queue::ExtractChunkPayload {
        chunk_id: chunk.id.clone(),
    })
    .unwrap();
    job.id = Some("legacy-extract-job".into());
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
            embedding: Some(LegacyEmbedding {
                model_signature: "fixture/model@1".into(),
                vector: vec![0.25, 0.5],
                created_at_ms: 1_750_000_000_000,
            }),
            entities: vec![occurrence.clone()],
        }],
        trees: vec![tree.clone()],
        summaries: vec![summary],
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
    assert_eq!(super::super::versioned::get_chunk_async(&config, &chunk_id).await.unwrap(), Some(batch.chunks[0].chunk.clone()));
    assert_eq!(
        super::super::versioned::read_state_async(
            &config,
            c::RAW_REFS,
            &chunk_id
        )
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
    assert!(
        super::super::versioned::read_state_async(&config, c::CHUNK_EMBEDDINGS, &format!("[\"{chunk_id}\",\"fixture/model@1\"]"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::super::versioned::read_state_async(&config, c::ENTITY_INDEX, &format!("occ:{}:{}", hex("person:ada"), hex(&chunk_id)))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::super::versioned::read_state_async(&config, c::TREES, "tree:legacy-tree")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::super::versioned::read_state_async(&config, c::SUMMARIES, "summary:legacy-summary")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        crate::memory::queue::store::get_job_async(&config, "legacy-extract-job")
            .await
            .unwrap()
            .is_some()
    );

    let repeated = import_legacy_batch(&config, &batch).await.unwrap();
    assert_eq!(repeated.chunks, 1);
    assert_eq!(
        super::super::versioned::list_state_async(&config, c::CHUNK_VERSIONS)
            .await
            .unwrap()
            .len(),
        0,
        "the idempotent retry must not create unrelated chunk-state records"
    );
}

#[tokio::test]
async fn typed_import_rejects_path_escape_and_mismatched_score_before_writes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let mut batch = fixture_batch();
    batch.chunks[0].content.as_mut().unwrap().path = "../escape.md".into();

    assert!(import_legacy_batch(&config, &batch).await.is_err());
    assert!(
        super::super::versioned::list_chunks_async(&config)
            .await
            .unwrap()
            .is_empty()
    );
}

fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
