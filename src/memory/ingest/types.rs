//! Types for the on-demand ingest orchestration: the job-sink seam, per-call
//! options, and the result summary.

use crate::memory::chunks::RawRef;
use crate::memory::config::MemoryConfig;
use anyhow::Result;
use tinystoragedrivers_types::NewMessage;

/// Durable queue intent included in the owner-scoped ingest commit.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct ExtractJobIntent {
    pub(crate) id: String,
    pub(crate) chunk_id: String,
    pub(crate) max_attempts: u32,
    #[serde(default)]
    pub(crate) published: bool,
}

/// Result of publishing a durable queue intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePublication {
    /// A new queue hint was created.
    Published,
    /// The same hint was already accepted, usually after a lost acknowledgement.
    AlreadyPublished,
    /// This sink intentionally declines queue work; the durable intent stays pending.
    Declined,
}

/// Sink for the tree jobs the ingest path produces.
///
/// Implementations publish a queue hint after the job intent is committed to
/// the host document store. The durable intent is authoritative; the queue is
/// an at-least-once hint and a failed publication can be retried safely.
#[async_trait::async_trait]
pub trait TreeJobSink: Send + Sync {
    /// Publish an `extract_chunk` queue hint for one committed intent. Repeated
    /// calls carry the same `intent_id`, allowing adapters to suppress replay.
    async fn enqueue_extract(
        &self,
        config: &MemoryConfig,
        intent_id: &str,
        chunk_id: &str,
        default_max_attempts: u32,
    ) -> Result<QueuePublication>;

    /// Whether this sink wants newly committed extract intents at all.
    fn accepts_extract_jobs(&self) -> bool {
        true
    }
}

/// A no-op [`TreeJobSink`] that drops every job. Useful when a caller only wants
/// chunks persisted and scored without driving the tree.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullJobSink;

#[async_trait::async_trait]
impl TreeJobSink for NullJobSink {
    async fn enqueue_extract(
        &self,
        _config: &MemoryConfig,
        _intent_id: &str,
        _chunk_id: &str,
        _default_max_attempts: u32,
    ) -> Result<QueuePublication> {
        Ok(QueuePublication::Declined)
    }

    fn accepts_extract_jobs(&self) -> bool {
        false
    }
}

/// Production sink backed by the host's scoped durable queue.
///
/// Publication requires the queue's dedupe feature so replaying an outbox
/// intent after a lost acknowledgement cannot enqueue a second active hint.
#[derive(Debug, Default, Clone, Copy)]
pub struct QueueJobSink;

#[async_trait::async_trait]
impl TreeJobSink for QueueJobSink {
    async fn enqueue_extract(
        &self,
        config: &MemoryConfig,
        intent_id: &str,
        chunk_id: &str,
        default_max_attempts: u32,
    ) -> Result<QueuePublication> {
        let payload = serde_json::json!({
            "kind": crate::memory::queue::types::JobKind::ExtractChunk.as_str(),
            "chunk_id": chunk_id,
            "max_attempts": default_max_attempts,
        });
        let message = NewMessage::new(payload).dedupe_key(intent_id.to_owned());
        let queues = config.storage()?.queues().clone();
        if !queues.features().dedupe {
            anyhow::bail!("host queue must support dedupe for recoverable ingest publication");
        }
        let enqueued = queues
            .enqueue(crate::memory::queue::QUEUE_NAME, &[message])
            .await?;
        let result = enqueued
            .first()
            .ok_or_else(|| anyhow::anyhow!("host queue returned no enqueue result"))?;
        Ok(if !result.duplicate {
            QueuePublication::Published
        } else {
            QueuePublication::AlreadyPublished
        })
    }
}

/// Per-call ingest knobs.
#[derive(Debug, Default, Clone)]
pub struct IngestOptions {
    /// When set, the document source gate is keyed by `{source_id}@{version_ms}`
    /// so a later revision of the same document is admitted non-destructively.
    /// Ignored for chat/email (which have no source-level gate).
    pub gate_version_ms: Option<i64>,
    /// When set, every persisted chunk is annotated with these raw-archive refs
    /// so a worker can resolve the chunk body from a verbatim source file.
    pub raw_refs: Option<Vec<RawRef>>,
}

/// Outcome of one ingest call.
///
/// `extract_jobs_enqueued` counts the leaves handed to the [`TreeJobSink`] for
/// downstream tree append; the actual buffer append and summary seal happen in
/// the (separately ported) async worker, so this summary reports work *pending*,
/// not completed.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestSummary {
    /// Logical source id this call ingested.
    pub source_id: String,
    /// Number of chunks persisted to the chunk store.
    pub chunks_written: usize,
    /// Number of chunks the cheap fast-score path would drop. Final admission
    /// still happens later in the extract worker.
    pub chunks_dropped: usize,
    /// Ids of all chunks produced by this call.
    pub chunk_ids: Vec<String>,
    /// Number of `extract_chunk` jobs enqueued (leaves pending tree append).
    pub extract_jobs_enqueued: usize,
    /// True when this ingest was a no-op because `(source_kind, source_id)` was
    /// already ingested. Documents are append-only — the summariser tree must
    /// not see the same source twice.
    pub already_ingested: bool,
}

impl IngestSummary {
    /// A no-op summary for empty/no-chunk payloads.
    pub(super) fn empty(source_id: &str) -> Self {
        Self {
            source_id: source_id.to_string(),
            chunks_written: 0,
            chunks_dropped: 0,
            chunk_ids: Vec::new(),
            extract_jobs_enqueued: 0,
            already_ingested: false,
        }
    }

    /// A summary marking the source as already ingested (gate lost / dup).
    pub(super) fn already_ingested(source_id: &str) -> Self {
        Self {
            source_id: source_id.to_string(),
            chunks_written: 0,
            chunks_dropped: 0,
            chunk_ids: Vec::new(),
            extract_jobs_enqueued: 0,
            already_ingested: true,
        }
    }
}
