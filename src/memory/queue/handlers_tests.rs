use super::*;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::test_support::RecordingDelegates;
use crate::memory::queue::types::{JobStatus, ReembedBackfillPayload};
use std::collections::VecDeque;
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn mk_running_job(kind: JobKind, payload_json: String) -> Job {
    let now_ms = chrono::Utc::now().timestamp_millis();
    Job {
        id: "test-job-id".into(),
        kind,
        payload_json,
        dedupe_key: None,
        status: JobStatus::Running,
        attempts: 1,
        max_attempts: 5,
        available_at_ms: now_ms,
        locked_until_ms: Some(now_ms + 60_000),
        last_error: None,
        created_at_ms: now_ms,
        started_at_ms: Some(now_ms),
        completed_at_ms: None,
        lease_epoch: 1,
        queue_receipt: None,
        failure_reason: None,
        failure_class: None,
    }
}

#[tokio::test]
async fn extract_admits_and_plans_append_buffer() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.outcome, JobOutcome::Done);
    assert_eq!(plan.follow_ups.len(), 1);
    assert_eq!(plan.follow_ups[0].kind, JobKind::AppendBuffer);
    // Covered space → no reembed chain armed.
    assert!(!plan
        .follow_ups
        .iter()
        .any(|job| job.kind == JobKind::ReembedBackfill));
}

#[test]
fn extract_plan_returns_domain_mutations_without_persisting_followups() {
    let (_tmp, cfg) = test_config();
    let mut delegates = RecordingDelegates::admitting();
    delegates.extract_mutations.push(JobMutation {
        collection: JobStateCollection::ChunkScores,
        key: "c1".into(),
        expected: None,
        value: Some(serde_json::json!({"score": 0.75})),
    });
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let plan = runtime.block_on(plan_job(&cfg, &job, &delegates)).unwrap();

    assert_eq!(plan.outcome, JobOutcome::Done);
    assert_eq!(plan.follow_ups.len(), 1);
    assert_eq!(plan.mutations.len(), 1);
    assert_eq!(plan.mutations[0].collection, "memory_chunk_scores");
    assert_eq!(plan.mutations[0].key, "c1");
    assert_eq!(plan.expected[0].value, None);
    assert_eq!(
        plan.mutations[0].value,
        Some(serde_json::json!({"score": 0.75}))
    );
    // The returned append intent remains unpersisted until the worker commits
    // it with this job's lease receipt.
    drop(runtime);
    let queued = crate::memory::queue::store::count_total(&cfg).unwrap();
    assert_eq!(queued, 0);
}

#[tokio::test]
async fn extract_missing_chunk_is_noop_done() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.extract = None; // simulate missing chunk row
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "gone".into(),
        })
        .unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.outcome, JobOutcome::Done);
    assert!(plan.follow_ups.is_empty());
}

#[tokio::test]
async fn extract_dropped_chunk_plans_nothing() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.extract = Some(ExtractDecision {
        kept: false,
        uses_document_subtree: false,
        tree_scope: "slack:#eng".into(),
    });
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert!(plan.follow_ups.is_empty());
}

#[tokio::test]
async fn extract_admits_doc_subtree_skips_flat_append() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.extract = Some(ExtractDecision {
        kept: true,
        uses_document_subtree: true,
        tree_scope: "notion:conn1".into(),
    });
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert!(
        plan.follow_ups.is_empty(),
        "document-subtree sources don't use the flat L0 buffer"
    );
}

#[tokio::test]
async fn extract_arms_reembed_when_uncovered() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.uncovered = true;
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.follow_ups.len(), 2);
    assert!(plan
        .follow_ups
        .iter()
        .any(|job| job.kind == JobKind::ReembedBackfill));
}

#[tokio::test]
async fn append_buffer_plans_seal_when_gate_met() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    let payload = AppendBufferPayload {
        node: NodeRef::Leaf {
            chunk_id: "c1".into(),
        },
        target: AppendTarget::Source {
            source_id: "slack:#eng".into(),
        },
    };
    let job = mk_running_job(
        JobKind::AppendBuffer,
        serde_json::to_string(&payload).unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.follow_ups.len(), 1);
    assert_eq!(plan.follow_ups[0].kind, JobKind::Seal);
}

#[tokio::test]
async fn append_buffer_no_seal_when_gate_unmet() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.append = Some(AppendDecision {
        tree_id: "tree:slack".into(),
        should_seal: false,
    });
    let payload = AppendBufferPayload {
        node: NodeRef::Leaf {
            chunk_id: "c1".into(),
        },
        target: AppendTarget::Source {
            source_id: "slack:#eng".into(),
        },
    };
    let job = mk_running_job(
        JobKind::AppendBuffer,
        serde_json::to_string(&payload).unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert!(plan.follow_ups.is_empty());
}

#[tokio::test]
async fn seal_plans_parent_when_cascading() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.seal_mutations.push(JobMutation {
        collection: JobStateCollection::Trees,
        key: "tree:slack".into(),
        expected: Some(serde_json::json!({"root_id": "summary:0"})),
        value: Some(serde_json::json!({"root_id": "summary:1"})),
    });
    *d.seal_parent.lock() = Some(SealPayload {
        tree_id: "tree:slack".into(),
        level: 1,
        force_now_ms: None,
    });
    let payload = SealPayload {
        tree_id: "tree:slack".into(),
        level: 0,
        force_now_ms: None,
    };
    let job = mk_running_job(JobKind::Seal, serde_json::to_string(&payload).unwrap());
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    // The level-1 parent seal is represented as an intent for worker settlement.
    assert_eq!(plan.follow_ups.len(), 1);
    assert_eq!(plan.follow_ups[0].kind, JobKind::Seal);
    assert_eq!(plan.mutations.len(), 1);
    assert_eq!(plan.mutations[0].collection, "memory_trees");
    assert_eq!(plan.expected[0].collection, plan.mutations[0].collection);
    assert_eq!(plan.expected[0].key, plan.mutations[0].key);
    assert_eq!(
        plan.expected[0].value,
        Some(serde_json::json!({"root_id": "summary:0"}))
    );
}

#[tokio::test]
async fn flush_stale_plans_a_seal_per_stale_buffer() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.stale = vec![
        StaleBuffer {
            tree_id: "tree:a".into(),
            level: 0,
        },
        StaleBuffer {
            tree_id: "tree:b".into(),
            level: 0,
        },
    ];
    let job = mk_running_job(
        JobKind::FlushStale,
        serde_json::to_string(&FlushStalePayload::default()).unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.follow_ups.len(), 2);
    assert!(plan.follow_ups.iter().all(|job| job.kind == JobKind::Seal));
}

#[tokio::test]
async fn direct_handle_rejects_plans_with_effects() {
    let (_tmp, cfg) = test_config();
    let delegates = RecordingDelegates::admitting();
    let job = mk_running_job(
        JobKind::ExtractChunk,
        serde_json::to_string(&ExtractChunkPayload {
            chunk_id: "c1".into(),
        })
        .unwrap(),
    );

    let error = handle_job(&cfg, &job, &delegates).await.unwrap_err();

    assert!(error.to_string().contains("lease-fenced worker settlement"));
}

#[tokio::test]
async fn seal_document_empty_is_noop() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    let payload = SealDocumentPayload {
        tree_scope: "notion:conn1".into(),
        doc_id: "notion:conn1:p".into(),
        version_ms: Some(1),
        chunk_ids: vec![],
    };
    let job = mk_running_job(
        JobKind::SealDocument,
        serde_json::to_string(&payload).unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.outcome, JobOutcome::Done);
    assert!(plan.follow_ups.is_empty());
    assert_eq!(
        d.counts
            .seal_document
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn seal_document_nonempty_calls_delegate() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.document_mutations.push(JobMutation {
        collection: JobStateCollection::Summaries,
        key: "summary:doc:v1".into(),
        expected: None,
        value: Some(serde_json::json!({"tree_id": "tree:doc"})),
    });
    let payload = SealDocumentPayload {
        tree_scope: "notion:conn1".into(),
        doc_id: "notion:conn1:p".into(),
        version_ms: Some(1),
        chunk_ids: vec!["c0".into()],
    };
    let job = mk_running_job(
        JobKind::SealDocument,
        serde_json::to_string(&payload).unwrap(),
    );
    let plan = plan_job(&cfg, &job, &d).await.unwrap();
    assert_eq!(plan.outcome, JobOutcome::Done);
    assert_eq!(plan.mutations.len(), 1);
    assert_eq!(plan.mutations[0].collection, "memory_summaries");
    assert_eq!(
        d.counts
            .seal_document
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn reembed_defers_when_more_pending_then_done() {
    let (_tmp, cfg) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.reembed_mutations.push(JobMutation {
        collection: JobStateCollection::ChunkEmbeddings,
        key: "[\"c1\",\"sig\"]".into(),
        expected: None,
        value: Some(serde_json::json!({"dim": 3})),
    });
    *d.reembed.lock() = VecDeque::from([
        ReembedProgress::Wrote { more_pending: true },
        ReembedProgress::Covered,
    ]);
    let job = mk_running_job(
        JobKind::ReembedBackfill,
        serde_json::to_string(&ReembedBackfillPayload {
            signature: "sig".into(),
        })
        .unwrap(),
    );
    let first_plan = plan_job(&cfg, &job, &d).await.unwrap();
    let out = first_plan.outcome;
    assert!(
        matches!(out, JobOutcome::Defer { .. }),
        "more pending → Defer"
    );
    assert_eq!(first_plan.mutations.len(), 1);
    assert_eq!(
        first_plan.mutations[0].collection,
        "memory_chunk_embeddings"
    );

    let out2 = plan_job(&cfg, &job, &d).await.unwrap().outcome;
    assert_eq!(out2, JobOutcome::Done, "covered → Done");
}

#[tokio::test]
async fn reembed_no_provider_completes_without_defer() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    *d.reembed.lock() = VecDeque::from([ReembedProgress::NoProvider]);
    let job = mk_running_job(
        JobKind::ReembedBackfill,
        serde_json::to_string(&ReembedBackfillPayload {
            signature: "sig".into(),
        })
        .unwrap(),
    );
    assert_eq!(
        plan_job(&cfg, &job, &d).await.unwrap().outcome,
        JobOutcome::Done
    );
}
