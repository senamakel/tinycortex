use super::*;
use crate::memory::chunks::{
    chunk_id, upsert_chunks_async, Chunk, Metadata, SourceKind, SourceRef,
};
use crate::memory::queue::{
    AppendBufferPayload, AppendDecision, AppendTarget, DelegateOutput, ExtractDecision,
    JobStateCollection, NewJob, NodeRef, QueueDelegates, ReembedProgress, SealDocumentPayload,
    SealPayload, StaleBuffer,
};
use crate::memory::store::entity_index::{CanonicalEntity, EntityIndex, EntityKind};
use crate::memory::tree::store::{self, TreeKind};
use crate::memory::tree::summarise::ConcatSummariser;
use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use tempfile::TempDir;

fn chunk(seq: u32, ts_ms: i64) -> Chunk {
    let timestamp = Utc.timestamp_millis_opt(ts_ms).unwrap();
    Chunk {
        id: chunk_id(SourceKind::Chat, "queue-seal", seq, "queue seal body"),
        content: "queue seal body proves planner output reaches settlement".into(),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: "queue-seal".into(),
            owner: "alice@example.com".into(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: Vec::new(),
            source_ref: Some(SourceRef::new("chat://queue-seal/1")),
            path_scope: None,
        },
        token_count: 12,
        seq_in_source: seq,
        created_at: timestamp,
        partial_message: false,
    }
}

struct SealDelegate {
    summariser: ConcatSummariser,
    observer: NoopSealObserver,
    stale_append: Option<DelegateOutput<AppendDecision>>,
    #[cfg(feature = "tokio")]
    invalidate_job_lease: Option<String>,
}

#[async_trait]
impl QueueDelegates for SealDelegate {
    async fn extract_chunk(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<Option<DelegateOutput<ExtractDecision>>> {
        bail!("not an extract job")
    }

    async fn append_node(
        &self,
        config: &crate::memory::config::MemoryConfig,
        node: &NodeRef,
        target: &AppendTarget,
    ) -> Result<Option<DelegateOutput<AppendDecision>>> {
        if let Some(output) = &self.stale_append {
            return Ok(Some(output.clone()));
        }
        plan_append_node_for_queue(config, node, target).await
    }

    async fn seal_level(
        &self,
        config: &crate::memory::config::MemoryConfig,
        payload: &SealPayload,
    ) -> Result<DelegateOutput<Option<SealPayload>>> {
        let tree = store::get_tree_async(config, &payload.tree_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing tree {}", payload.tree_id))?;
        let buffer = store::get_buffer_async(config, &payload.tree_id, payload.level).await?;
        let services = SealServices {
            summariser: &self.summariser,
            embedder: None,
            observer: &self.observer,
        };
        let output = plan_seal_one_level_for_queue(
            config,
            &tree,
            &buffer,
            &services,
            &LabelStrategy::UnionFromChildren,
        )
        .await?;
        #[cfg(feature = "tokio")]
        if let Some(job_id) = &self.invalidate_job_lease {
            crate::memory::queue::store::update_test_record_async(config, job_id, |record| {
                record.lease_epoch = record.lease_epoch.saturating_add(1);
            })
            .await?;
        }
        Ok(output)
    }

    async fn list_stale_buffers(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: i64,
    ) -> Result<Vec<StaleBuffer>> {
        bail!("not a flush job")
    }

    async fn seal_document(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &SealDocumentPayload,
    ) -> Result<DelegateOutput<crate::memory::tree::DocumentSealPlanResult>> {
        bail!("not a document seal job")
    }

    async fn reembed_batch(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<DelegateOutput<ReembedProgress>> {
        bail!("not a re-embed job")
    }

    fn active_signature(&self, _: &crate::memory::config::MemoryConfig) -> String {
        String::new()
    }

    fn has_uncovered_reembed_work(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<bool> {
        Ok(false)
    }
}

fn config() -> (TempDir, crate::memory::config::MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, config)
}

#[tokio::test(flavor = "current_thread")]
async fn queued_append_publishes_buffer_with_lease_settlement() {
    let (_tmp, config) = config();
    let chunk = chunk(3, 1_700_000_000_000);
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    let payload = AppendBufferPayload {
        node: NodeRef::Leaf {
            chunk_id: chunk.id.clone(),
        },
        target: AppendTarget::Source {
            source_id: "queue-append-source".to_owned(),
        },
    };
    let delegate = SealDelegate {
        summariser: ConcatSummariser,
        observer: NoopSealObserver,
        stale_append: None,
        #[cfg(feature = "tokio")]
        invalidate_job_lease: None,
    };

    let planned = delegate
        .append_node(&config, &payload.node, &payload.target)
        .await
        .unwrap()
        .unwrap();
    assert!(!planned.mutations.is_empty());
    assert!(
        store::get_tree_by_scope_async(&config, TreeKind::Source, "queue-append-source",)
            .await
            .unwrap()
            .is_none()
    );

    let job_id = crate::memory::queue::store::enqueue_async(
        &config,
        &NewJob::append_buffer(&payload).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(crate::memory::queue::run_once(&config, &delegate)
        .await
        .unwrap());
    let tree = store::get_tree_by_scope_async(&config, TreeKind::Source, "queue-append-source")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store::get_buffer_async(&config, &tree.id, 0)
            .await
            .unwrap()
            .item_ids,
        vec![chunk.id]
    );
    assert_eq!(
        crate::memory::queue::store::get_job_async(&config, &job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crate::memory::queue::JobStatus::Done
    );
}

#[tokio::test(flavor = "current_thread")]
async fn queued_seal_keeps_plan_invisible_until_lease_settlement() {
    let (_tmp, mut config) = config();
    config.tree.summary_fanout = 1;
    let tree =
        crate::memory::tree::get_or_create_tree_async(&config, TreeKind::Source, "queue-seal")
            .await
            .unwrap();
    let chunk = chunk(1, 1_700_000_000_000);
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    let entity = CanonicalEntity {
        canonical_id: "email:alice@example.com".into(),
        kind: EntityKind::Email,
        surface: "alice@example.com".into(),
        span_start: 0,
        span_end: 17,
        score: 1.0,
    };
    EntityIndex::for_memory_config(&config)
        .unwrap()
        .index_entity_async(
            &entity,
            &chunk.id,
            "leaf",
            chunk.created_at.timestamp_millis(),
            Some(&tree.id),
        )
        .await
        .unwrap();
    append_to_buffer_async(
        &config,
        &tree.id,
        0,
        &chunk.id,
        i64::from(chunk.token_count),
        chunk.created_at,
    )
    .await
    .unwrap();

    let delegate = SealDelegate {
        summariser: ConcatSummariser,
        observer: NoopSealObserver,
        stale_append: None,
        #[cfg(feature = "tokio")]
        invalidate_job_lease: None,
    };
    let payload = SealPayload {
        tree_id: tree.id.clone(),
        level: 0,
        force_now_ms: Some(1_700_000_001_000),
    };

    // Planning the real tree mutation does not publish it before the worker's
    // lease-fenced settlement.
    let planned = delegate.seal_level(&config, &payload).await.unwrap();
    assert!(!planned.mutations.is_empty());
    assert_eq!(
        store::get_buffer_async(&config, &tree.id, 0)
            .await
            .unwrap()
            .item_ids,
        vec![chunk.id.clone()]
    );
    assert!(store::list_summaries_at_level_async(&config, &tree.id, 1)
        .await
        .unwrap()
        .is_empty());

    let queue_job_id =
        crate::memory::queue::store::enqueue_async(&config, &NewJob::seal(&payload).unwrap())
            .await
            .unwrap()
            .unwrap();
    assert!(crate::memory::queue::run_once(&config, &delegate)
        .await
        .unwrap());

    let sealed = store::list_summaries_at_level_async(&config, &tree.id, 1)
        .await
        .unwrap();
    assert_eq!(sealed.len(), 1);
    assert!(store::get_buffer_async(&config, &tree.id, 0)
        .await
        .unwrap()
        .is_empty());
    let updated_tree = store::get_tree_async(&config, &tree.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated_tree.root_id.as_deref(), Some(sealed[0].id.as_str()));
    let original_job = crate::memory::queue::store::get_job_async(&config, &queue_job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original_job.status, crate::memory::queue::JobStatus::Done);
    assert_eq!(
        crate::memory::queue::store::count_by_status_async(
            &config,
            crate::memory::queue::JobStatus::Ready,
        )
        .await
        .unwrap(),
        1,
        "the parent seal intent is durable with the completed worker receipt"
    );
    let entity_hits =
        crate::memory::score::store::lookup_entity_async(&config, &entity.canonical_id, None)
            .await
            .unwrap();
    assert!(entity_hits.iter().any(|hit| hit.node_id == sealed[0].id));
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn stale_seal_lease_cannot_publish_tree_mutations() {
    let (_tmp, config) = config();
    let tree = crate::memory::tree::get_or_create_tree_async(
        &config,
        TreeKind::Source,
        "stale-queue-seal",
    )
    .await
    .unwrap();
    let chunk = chunk(2, 1_700_000_000_000);
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    append_to_buffer_async(
        &config,
        &tree.id,
        0,
        &chunk.id,
        i64::from(chunk.token_count),
        chunk.created_at,
    )
    .await
    .unwrap();
    let payload = SealPayload {
        tree_id: tree.id.clone(),
        level: 0,
        force_now_ms: Some(1_700_000_001_000),
    };
    let job_id =
        crate::memory::queue::store::enqueue_async(&config, &NewJob::seal(&payload).unwrap())
            .await
            .unwrap()
            .unwrap();
    let delegate = SealDelegate {
        summariser: ConcatSummariser,
        observer: NoopSealObserver,
        stale_append: None,
        invalidate_job_lease: Some(job_id.clone()),
    };

    // A competing owner advances the lease epoch after the actual planner
    // returns but before settlement. The single settlement CAS must reject
    // both completion and every planned tree mutation.
    assert!(crate::memory::queue::run_once(&config, &delegate)
        .await
        .unwrap());
    assert!(store::list_summaries_at_level_async(&config, &tree.id, 1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store::get_tree_async(&config, &tree.id)
            .await
            .unwrap()
            .unwrap(),
        tree
    );
    assert_eq!(
        store::get_buffer_async(&config, &tree.id, 0)
            .await
            .unwrap()
            .item_ids,
        vec![chunk.id]
    );
    let job = crate::memory::queue::store::get_job_async(&config, &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(job.status, crate::memory::queue::JobStatus::Done);
}

#[tokio::test(flavor = "current_thread")]
async fn a_preplanned_append_cannot_resurrect_a_deleted_chunk() {
    let (_tmp, config) = config();
    let chunk = chunk(8, 1_700_000_000_000);
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    let payload = AppendBufferPayload {
        node: NodeRef::Leaf {
            chunk_id: chunk.id.clone(),
        },
        target: AppendTarget::Source {
            source_id: "delete-fences-append".to_owned(),
        },
    };
    let planning_delegate = SealDelegate {
        summariser: ConcatSummariser,
        observer: NoopSealObserver,
        stale_append: None,
        #[cfg(feature = "tokio")]
        invalidate_job_lease: None,
    };
    let stale_plan = planning_delegate
        .append_node(&config, &payload.node, &payload.target)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store::get_tree_by_scope_async(&config, TreeKind::Source, "delete-fences-append")
            .await
            .unwrap()
            .is_none()
    );

    crate::memory::chunks::delete_chunk_by_id_async(&config, &chunk.id)
        .await
        .unwrap();
    let job_id = crate::memory::queue::store::enqueue_async(
        &config,
        &NewJob::append_buffer(&payload).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    let stale_delegate = SealDelegate {
        summariser: ConcatSummariser,
        observer: NoopSealObserver,
        stale_append: Some(stale_plan),
        #[cfg(feature = "tokio")]
        invalidate_job_lease: None,
    };
    crate::memory::queue::run_once(&config, &stale_delegate)
        .await
        .unwrap();
    assert!(
        store::get_tree_by_scope_async(&config, TreeKind::Source, "delete-fences-append")
            .await
            .unwrap()
            .is_none()
    );
    assert_ne!(
        crate::memory::queue::store::get_job_async(&config, &job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crate::memory::queue::JobStatus::Done,
        "a stale append plan must not settle as done"
    );
}

#[test]
fn staged_seal_merge_rejects_missing_preconditions_and_conflicting_writes() {
    use crate::memory::chunks::versioned::StateChange;
    let change = StateChange {
        collection: crate::memory::chunks::collections::TREES.to_owned(),
        key: "tree:one".into(),
        value: Some(serde_json::json!({"status":"archived"})),
    };
    assert!(merge_staged_seal_changes(Vec::new(), vec![change.clone()]).is_err());

    let expected = StateChange {
        value: Some(serde_json::json!({"status":"active"})),
        ..change.clone()
    };
    let other = StateChange {
        value: Some(serde_json::json!({"status":"deleted"})),
        ..change.clone()
    };
    assert!(merge_staged_seal_changes(vec![expected.clone()], vec![change, other]).is_err());

    let mutations =
        merge_staged_seal_changes(vec![expected.clone()], vec![expected.clone()]).unwrap();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].collection, JobStateCollection::Trees);
    assert_eq!(mutations[0].expected, expected.value);
    assert_eq!(mutations[0].value, expected.value);
}
