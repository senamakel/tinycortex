use super::*;
use crate::memory::queue::{
    AppendDecision, AppendTarget, DelegateOutput, ExtractDecision, NewJob, NodeRef, QueueDelegates,
    ReembedBackfillPayload, ReembedProgress, SealDocumentPayload, SealPayload, StaleBuffer,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use tempfile::TempDir;

struct ReembedDelegate {
    chunk_id: String,
}

#[async_trait]
impl QueueDelegates for ReembedDelegate {
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
        _: &crate::memory::config::MemoryConfig,
        _: &SealDocumentPayload,
    ) -> Result<DelegateOutput<()>> {
        bail!("unexpected document seal")
    }

    async fn reembed_batch(
        &self,
        config: &crate::memory::config::MemoryConfig,
        signature: &str,
    ) -> Result<DelegateOutput<ReembedProgress>> {
        let output =
            plan_chunk_embedding_for_queue(config, &self.chunk_id, signature, &[0.25, -0.5, 0.75])
                .await?;
        Ok(DelegateOutput {
            result: ReembedProgress::Covered,
            mutations: output.mutations,
        })
    }

    fn active_signature(&self, _: &crate::memory::config::MemoryConfig) -> String {
        "queue-reembed-model".to_owned()
    }

    fn has_uncovered_reembed_work(
        &self,
        _: &crate::memory::config::MemoryConfig,
        _: &str,
    ) -> Result<bool> {
        Ok(false)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn queued_reembed_planner_keeps_vector_invisible_until_settlement() {
    let tmp = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(tmp.path());
    let timestamp = chrono::Utc::now();
    let chunk = Chunk {
        id: chunk_id(SourceKind::Chat, "queue-reembed", 1, "body"),
        content: "body".to_owned(),
        metadata: Metadata {
            source_kind: SourceKind::Chat,
            source_id: "queue-reembed".to_owned(),
            owner: "alice@example.com".to_owned(),
            timestamp,
            time_range: (timestamp, timestamp),
            tags: Vec::new(),
            source_ref: None,
            path_scope: None,
        },
        token_count: 1,
        seq_in_source: 1,
        created_at: timestamp,
        partial_message: false,
    };
    upsert_chunks_async(&config, std::slice::from_ref(&chunk))
        .await
        .unwrap();
    let signature = "queue-reembed-model";
    let planned =
        plan_chunk_embedding_for_queue(&config, &chunk.id, signature, &[0.25, -0.5, 0.75])
            .await
            .unwrap();
    assert_eq!(planned.mutations.len(), 3);
    assert!(
        get_chunk_embedding_for_signature_async(&config, &chunk.id, signature)
            .await
            .unwrap()
            .is_none()
    );

    let job_id = crate::memory::queue::store::enqueue_async(
        &config,
        &NewJob::reembed_backfill(&ReembedBackfillPayload {
            signature: signature.to_owned(),
        })
        .unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    let delegate = ReembedDelegate {
        chunk_id: chunk.id.clone(),
    };
    assert!(crate::memory::queue::run_once(&config, &delegate)
        .await
        .unwrap());
    assert_eq!(
        get_chunk_embedding_for_signature_async(&config, &chunk.id, signature)
            .await
            .unwrap()
            .unwrap(),
        vec![0.25, -0.5, 0.75]
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
