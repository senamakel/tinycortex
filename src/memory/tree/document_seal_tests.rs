use super::*;
use crate::memory::chunks::{chunk_id, upsert_chunks_async, Chunk, Metadata, SourceKind, SourceRef};
use crate::memory::queue::{
    AppendDecision, AppendTarget, DelegateOutput, ExtractDecision, JobStatus,
    NewJob, NodeRef, QueueDelegates, ReembedProgress, SealDocumentPayload, SealPayload, StaleBuffer,
};
use crate::memory::tree::store::{self, TreeKind};
use crate::memory::tree::summarise::ConcatSummariser;
use anyhow::{bail, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

fn test_chunk() -> Chunk {
    let timestamp = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    Chunk {
        id: chunk_id(SourceKind::Chat, "document-seal", 1, "document content"),
        content: "document content for owner-state sealing".into(),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: "document-seal".into(),
            owner: "alice@example.com".into(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: vec!["fixture".into()],
            source_ref: Some(SourceRef::new("chat://document-seal/1")),
            path_scope: None,
        },
        token_count: 8,
        seq_in_source: 1,
        created_at: timestamp,
        partial_message: false,
    }
}

struct DocumentDelegate {
    summariser: ConcatSummariser,
    projected: Arc<AtomicUsize>,
    invalidate_job_lease: Option<String>,
}

#[async_trait]
impl QueueDelegates for DocumentDelegate {
    async fn extract_chunk(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<Option<DelegateOutput<ExtractDecision>>> {
        bail!("unexpected extract")
    }

    async fn append_node(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &NodeRef,
        _: &AppendTarget,
    ) -> Result<Option<DelegateOutput<AppendDecision>>> {
        bail!("unexpected append")
    }

    async fn seal_level(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &SealPayload,
    ) -> Result<DelegateOutput<Option<SealPayload>>> {
        bail!("unexpected seal")
    }

    async fn list_stale_buffers(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: i64,
    ) -> Result<Vec<StaleBuffer>> {
        bail!("unexpected stale buffer scan")
    }

    async fn seal_document(
        &self,
        config: &crate::memory::config::MemoryConfig,
        payload: &SealDocumentPayload,
    ) -> Result<DelegateOutput<crate::memory::tree::DocumentSealPlanResult>> {
        let tree = store::get_tree_by_scope_async(config, TreeKind::Source, &payload.tree_scope)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing document tree"))?;
        let services = SealServices {
            summariser: &self.summariser,
            embedder: None,
            observer: &super::types::NoopSealObserver,
        };
        let output = plan_document_subtree_for_queue(
            config,
            &tree,
            payload,
            &services,
            &LabelStrategy::UnionFromChildren,
        )
        .await?;
        if let Some(job_id) = &self.invalidate_job_lease {
            crate::memory::queue::store::update_test_record_async(config, job_id, |record| {
                record.lease_epoch = record.lease_epoch.saturating_add(1);
            })
            .await?;
        }
        Ok(output)
    }

    async fn document_summaries_committed(
        &self,
        config: &crate::memory::config::MemoryConfig,
        tree_id: &str,
        summaries: &[SummaryNode],
    ) -> Result<()> {
        for summary in summaries {
            let stored = store::get_summary_async(config, &summary.id).await?;
            anyhow::ensure!(
                stored.is_some_and(|stored| stored.tree_id == tree_id),
                "projection ran before summary {} became durable",
                summary.id
            );
        }
        self.projected.fetch_add(summaries.len(), Ordering::SeqCst);
        Ok(())
    }

    async fn reembed_batch(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<DelegateOutput<ReembedProgress>> {
        bail!("unexpected re-embed")
    }

    fn active_signature(&self, _: &crate::memory::config::MemoryConfig) -> String {
        String::new()
    }

    async fn has_uncovered_reembed_work(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<bool> {
        Ok(false)
    }
}

async fn test_setup() -> (TempDir, crate::memory::config::MemoryConfig, store::Tree, Chunk) {
    let temp = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let tree = crate::memory::tree::get_or_create_tree_async(
        &config,
        TreeKind::Source,
        "document-seal-scope",
    )
    .await
    .unwrap();
    let chunk = test_chunk();
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    (temp, config, tree, chunk)
}

fn delegate(invalidate_job_lease: Option<String>) -> DocumentDelegate {
    DocumentDelegate {
        summariser: ConcatSummariser,
        projected: Arc::new(AtomicUsize::new(0)),
        invalidate_job_lease,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn document_planning_is_invisible_until_worker_settlement_and_projects_after_commit() {
    let (_temp, config, tree, chunk) = test_setup().await;
    let payload = SealDocumentPayload {
        tree_scope: tree.scope.clone(),
        doc_id: "doc-1".into(),
        version_ms: Some(1_700_000_000_000),
        chunk_ids: vec![chunk.id],
    };
    let service = delegate(None);
    let services = SealServices {
        summariser: &service.summariser,
        embedder: None,
        observer: &super::types::NoopSealObserver,
    };
    let planned = plan_document_subtree_for_queue(
        &config,
        &tree,
        &payload,
        &services,
        &LabelStrategy::UnionFromChildren,
    )
    .await
    .unwrap();
    assert!(!planned.mutations.is_empty());
    assert!(store::get_summary_async(&config, &planned.result.root_id)
        .await
        .unwrap()
        .is_none());
    assert!(store::get_tree_async(&config, &tree.id)
        .await
        .unwrap()
        .unwrap()
        .root_id
        .is_none());

    let job_id = crate::memory::queue::store::enqueue_async(
        &config,
        &NewJob::seal_document(&payload).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(crate::memory::queue::run_once(&config, &service)
        .await
        .unwrap());
    assert_eq!(
        crate::memory::queue::store::get_job_async(&config, &job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Done
    );
    assert!(store::get_summary_async(&config, &planned.result.root_id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        service.projected.load(Ordering::SeqCst),
        planned.result.summaries.len()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stale_document_lease_publishes_neither_summaries_nor_projection() {
    let (_temp, config, tree, chunk) = test_setup().await;
    let payload = SealDocumentPayload {
        tree_scope: tree.scope.clone(),
        doc_id: "doc-stale".into(),
        version_ms: Some(2),
        chunk_ids: vec![chunk.id],
    };
    let job_id = crate::memory::queue::store::enqueue_async(
        &config,
        &NewJob::seal_document(&payload).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    let service = delegate(Some(job_id.clone()));

    assert!(crate::memory::queue::run_once(&config, &service)
        .await
        .unwrap());
    assert_eq!(service.projected.load(Ordering::SeqCst), 0);
    assert!(crate::memory::chunks::versioned::list_state_async(
        &config,
        crate::memory::chunks::collections::SUMMARIES,
    )
    .await
    .unwrap()
    .is_empty());
    assert_ne!(
        crate::memory::queue::store::get_job_async(&config, &job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Done
    );
}
