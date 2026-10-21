//! On-demand ingest orchestration.
//!
//! Runs the full ingest path for a source-scoped payload supplied by the host:
//!
//! ```text
//! canonicalize -> write raw markdown -> chunk -> score/extract
//!   -> persist chunk metadata -> enqueue tree jobs (-> append/seal in worker)
//! ```
//!
//! TinyCortex does not own live memory sync, so this module assumes the host
//! already produced a `ChatBatch` / `EmailThread` / `DocumentInput`. The buffer
//! append and summary seal happen in the async extract worker driven off the
//! [`TreeJobSink`]; this hot path stops at enqueue.

use anyhow::{anyhow, bail, Result};
use chrono::Utc;

use crate::memory::chunks::collections::{
    CHUNK_LIFECYCLE, INGESTED_SOURCES, INGEST_OUTBOX, RAW_REFS, SCORES,
};
use crate::memory::chunks::versioned::{
    commit_if_state_matches_async, commit_staged_chunks_if_state_matches_async, list_state_async,
    read_state_async, read_states_async, StateChange,
};
use crate::memory::chunks::{
    self, chunk_markdown, ChunkerInput, ChunkerOptions, SourceKind, CHUNK_STATUS_PENDING_EXTRACTION,
};
use crate::memory::config::MemoryConfig;
use crate::memory::score::{score_chunks_fast, ScoringConfig};
use crate::memory::store::content;

use super::canonicalize::chat::{self, ChatBatch};
use super::canonicalize::document::{self, DocumentInput};
use super::canonicalize::email::{self, EmailThread};
use super::canonicalize::CanonicalisedSource;
use super::types::{ExtractJobIntent, IngestOptions, IngestSummary, QueuePublication, TreeJobSink};

/// Publish queue hints for every committed ingest intent that has not yet
/// received a durable publication receipt. Re-running this function is safe:
/// queue adapters receive the same stable intent id, including after a lost
/// enqueue acknowledgement.
/// Async-facade variant that performs host-store reads and receipt writes via
/// the storage bridge's async path while keeping the borrowed sink on caller.
pub async fn dispatch_pending_extract_jobs_async(
    config: &MemoryConfig,
    sink: &dyn TreeJobSink,
) -> Result<usize> {
    let pending = list_state_async(config, INGEST_OUTBOX).await?;
    let mut published_count = 0;
    for (key, value) in pending {
        let intent: ExtractJobIntent = serde_json::from_value(value.clone())?;
        if intent.published {
            continue;
        }
        let publication = sink
            .enqueue_extract(config, &intent.id, &intent.chunk_id, intent.max_attempts)
            .await?;
        if publication == QueuePublication::Declined {
            continue;
        }
        let mut completed = intent;
        completed.published = true;
        let expected = StateChange {
            collection: INGEST_OUTBOX.into(),
            key: key.clone(),
            value: Some(value),
        };
        let update = StateChange {
            collection: INGEST_OUTBOX.into(),
            key,
            value: Some(serde_json::to_value(completed)?),
        };
        commit_if_state_matches_async(config, &[expected], &[], &[update]).await?;
        published_count += usize::from(publication == QueuePublication::Published);
    }
    Ok(published_count)
}

/// Run the full ingest path for an already-canonicalised source.
///
/// For documents an authoritative source-level gate is claimed in the same
/// host commit as chunk and outbox state; chat and email have no such gate (their
/// `source_id` is a stream identifier under which many batches accumulate) and
/// rely on deterministic chunk ids for replay idempotency.
///
/// # Atomicity (see `docs/spec/audit/04-queue-ingest.md`)
///
/// - The document gate, chunk versions, scores, lifecycle, raw references, and
///   extract-job outbox intents commit through one host commit-head CAS.
/// - Queue publication follows the commit. Stable hint ids and retained
///   outbox receipts make lost acknowledgements repairable.
/// - Chat/email logical-unit ids derive from complete message content rather
///   than batch-local sequence, so overlapping deliveries reuse persisted rows
///   and active queue dedupe keys.
pub async fn ingest_canonical(
    config: &MemoryConfig,
    source_id: &str,
    canonical: CanonicalisedSource,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
    opts: &IngestOptions,
) -> Result<IngestSummary> {
    let source_kind = canonical.metadata.source_kind;
    dispatch_pending_extract_jobs_async(config, sink).await?;

    // 1. Chunk the canonical markdown.
    let input = ChunkerInput {
        source_kind,
        source_id: source_id.to_string(),
        markdown: canonical.markdown,
        metadata: canonical.metadata,
    };
    let chunks = chunk_markdown(&input, &ChunkerOptions::default());
    if chunks.is_empty() {
        return Ok(IngestSummary::empty(source_id));
    }

    persist_score_enqueue(config, source_id, source_kind, chunks, sink, scoring, opts).await
}

/// Persist chunk bodies + rows, fast-score, and enqueue extract jobs.
///
async fn persist_score_enqueue(
    config: &MemoryConfig,
    source_id: &str,
    source_kind: SourceKind,
    chunks: Vec<crate::memory::chunks::Chunk>,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
    opts: &IngestOptions,
) -> Result<IngestSummary> {
    // 3. Write each chunk body to the content store (atomic write + sha256).
    let content_root = chunks::content_root(config);
    let staged = content::stage_chunks(&content_root, &chunks)
        .map_err(|e| anyhow!("stage_chunks failed: {e}"))?;

    // 4. Cheap fast-score (no LLM on the ingest hot path).
    let scores = score_chunks_fast(&chunks, scoring).await?;
    if scores.len() != chunks.len() {
        bail!(
            "scorer length mismatch: chunks={} scores={}",
            chunks.len(),
            scores.len()
        );
    }

    let publish_jobs = sink.accepts_extract_jobs();
    let intent_ids = persist_ingest_operation(
        config,
        IngestPersistence {
            source_id,
            source_kind,
            chunks: &chunks,
            staged: &staged,
            scores: &scores,
            opts,
            publish_jobs,
        },
    )
    .await?;
    let Some(intent_ids) = intent_ids else {
        let staged_paths = staged
            .iter()
            .map(|chunk| chunk.content_path.clone())
            .collect::<Vec<_>>();
        chunks::remove_unreferenced_content_files_async(config, &staged_paths).await?;
        return Ok(IngestSummary::already_ingested(source_id));
    };
    dispatch_pending_extract_jobs_async(config, sink).await?;
    let wanted = intent_ids
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let extract_jobs_enqueued = list_state_async(config, INGEST_OUTBOX)
        .await?
        .into_iter()
        .filter(|(key, value)| {
            wanted.contains(key)
                && value.get("published").and_then(serde_json::Value::as_bool) == Some(true)
        })
        .count();
    let chunks_written = staged.len();

    let chunks_dropped = scores.iter().filter(|result| !result.kept).count();
    let chunk_ids = chunks.iter().map(|chunk| chunk.id.clone()).collect();

    Ok(IngestSummary {
        source_id: source_id.to_string(),
        chunks_written,
        chunks_dropped,
        chunk_ids,
        extract_jobs_enqueued,
        already_ingested: false,
    })
}

struct IngestPersistence<'a> {
    source_id: &'a str,
    source_kind: SourceKind,
    chunks: &'a [crate::memory::chunks::Chunk],
    staged: &'a [crate::memory::chunks::StagedChunk],
    scores: &'a [crate::memory::score::ScoreResult],
    opts: &'a IngestOptions,
    publish_jobs: bool,
}

async fn persist_ingest_operation(
    config: &MemoryConfig,
    operation: IngestPersistence<'_>,
) -> Result<Option<Vec<String>>> {
    let IngestPersistence {
        source_id,
        source_kind,
        chunks,
        staged,
        scores,
        opts,
        publish_jobs,
    } = operation;
    let gate_key = match opts.gate_version_ms {
        Some(version) => format!("{source_id}@{version}"),
        None => source_id.to_string(),
    };
    let source_key = crate::memory::chunks::store_sources::source_gate_key(source_kind, &gate_key);
    if source_kind == SourceKind::Document
        && read_state_async(config, INGESTED_SOURCES, &source_key)
            .await?
            .is_some()
    {
        return Ok(None);
    }

    // The scoring result is ready before this retry loop. Retries only
    // recapture state preconditions and re-stage the same persistence values.
    for _ in 0..8 {
        let mut expected = Vec::with_capacity(chunks.len() + 1);
        let mut changes = Vec::new();
        let mut intent_ids = Vec::new();
        if source_kind == SourceKind::Document {
            if read_state_async(config, INGESTED_SOURCES, &source_key)
                .await?
                .is_some()
            {
                return Ok(None);
            }
            expected.push(StateChange {
                collection: INGESTED_SOURCES.into(),
                key: source_key.clone(),
                value: None,
            });
            changes.push(StateChange {
                collection: INGESTED_SOURCES.into(),
                key: source_key.clone(),
                value: Some(serde_json::json!({
                    "source_kind": source_kind.as_str(),
                    "source_id": gate_key,
                    "ingested_at_ms": Utc::now().timestamp_millis(),
                })),
            });
        }

        let chunk_ids = chunks
            .iter()
            .map(|chunk| chunk.id.clone())
            .collect::<Vec<_>>();
        let current_lifecycle = read_states_async(config, CHUNK_LIFECYCLE, &chunk_ids).await?;
        for (chunk, score) in chunks.iter().zip(scores) {
            let lifecycle = current_lifecycle.get(&chunk.id);
            expected.push(StateChange {
                collection: CHUNK_LIFECYCLE.into(),
                key: chunk.id.clone(),
                value: lifecycle.cloned(),
            });
            let status = lifecycle
                .and_then(|value| value.get("status"))
                .and_then(serde_json::Value::as_str);
            let needs_processing = matches!(status, None | Some(CHUNK_STATUS_PENDING_EXTRACTION));
            if needs_processing {
                changes.push(StateChange {
                    collection: SCORES.into(),
                    key: chunk.id.clone(),
                    value: Some(serde_json::to_value(
                        crate::memory::score::store::ScoreRow {
                            chunk_id: chunk.id.clone(),
                            total: score.total,
                            signals: score.signals.clone(),
                            dropped: !score.kept,
                            reason: score.drop_reason.clone(),
                            computed_at_ms: Utc::now().timestamp_millis(),
                            llm_importance_reason: score.extracted.llm_importance_reason.clone(),
                        },
                    )?),
                });
                changes.push(StateChange {
                    collection: CHUNK_LIFECYCLE.into(),
                    key: chunk.id.clone(),
                    value: Some(serde_json::json!({"status": CHUNK_STATUS_PENDING_EXTRACTION})),
                });
                if publish_jobs {
                    use sha2::Digest;
                    let mut digest = sha2::Sha256::new();
                    digest.update(chunk.id.as_bytes());
                    digest.update(item_content_hash(staged, &chunk.id).as_bytes());
                    let intent_id = format!("job:extract:{:x}", digest.finalize());
                    let intent = ExtractJobIntent {
                        id: intent_id.clone(),
                        chunk_id: chunk.id.clone(),
                        max_attempts: config.queue.max_attempts,
                        published: false,
                    };
                    changes.push(StateChange {
                        collection: INGEST_OUTBOX.into(),
                        key: intent_id.clone(),
                        value: Some(serde_json::to_value(intent)?),
                    });
                    intent_ids.push(intent_id);
                }
            }
            if let Some(refs) = opts.raw_refs.as_ref() {
                changes.push(StateChange {
                    collection: RAW_REFS.into(),
                    key: chunk.id.clone(),
                    value: Some(serde_json::to_value(refs)?),
                });
            }
        }
        if commit_staged_chunks_if_state_matches_async(config, staged, &expected, &changes).await? {
            return Ok(Some(intent_ids));
        }
    }
    if source_kind == SourceKind::Document
        && read_state_async(config, INGESTED_SOURCES, &source_key)
            .await?
            .is_some()
    {
        return Ok(None);
    }
    bail!("ingest persistence preconditions changed too many times")
}

fn item_content_hash(staged: &[crate::memory::chunks::StagedChunk], chunk_id: &str) -> String {
    staged
        .iter()
        .find(|item| item.chunk.id == chunk_id)
        .map(|item| item.content_sha256.clone())
        .unwrap_or_default()
}

/// Ingest a batch of chat messages. Returns a no-op summary on an empty batch.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_chat(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    batch: ChatBatch,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    let canonical =
        match chat::canonicalise(source_id, owner, &tags, batch).map_err(anyhow::Error::msg)? {
            Some(c) => c,
            None => return Ok(IngestSummary::empty(source_id)),
        };
    ingest_canonical(
        config,
        source_id,
        canonical,
        sink,
        scoring,
        &IngestOptions::default(),
    )
    .await
}

/// Ingest an email thread. Returns a no-op summary on an empty thread.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_email(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    thread: EmailThread,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    let canonical =
        match email::canonicalise(source_id, owner, &tags, thread).map_err(anyhow::Error::msg)? {
            Some(c) => c,
            None => return Ok(IngestSummary::empty(source_id)),
        };
    ingest_canonical(
        config,
        source_id,
        canonical,
        sink,
        scoring,
        &IngestOptions::default(),
    )
    .await
}

/// Ingest an email thread whose chunk bodies are backed by raw-archive files.
/// The `raw_refs` are attached to every persisted chunk.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_email_with_raw_refs(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    thread: EmailThread,
    raw_refs: Vec<crate::memory::chunks::RawRef>,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    let canonical =
        match email::canonicalise(source_id, owner, &tags, thread).map_err(anyhow::Error::msg)? {
            Some(c) => c,
            None => return Ok(IngestSummary::empty(source_id)),
        };
    let opts = IngestOptions {
        gate_version_ms: None,
        raw_refs: Some(raw_refs),
    };
    ingest_canonical(config, source_id, canonical, sink, scoring, &opts).await
}

/// Ingest a single document. Returns a no-op summary on empty input and an
/// already-ingested summary when the document source was previously ingested.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_document(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    doc: DocumentInput,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    ingest_document_versioned(
        config, source_id, owner, tags, doc, None, None, sink, scoring,
    )
    .await
}

/// Like [`ingest_document`] but with an explicit `path_scope` (groups multiple
/// items under one content directory while keeping `source_id` as the dedup key).
#[allow(clippy::too_many_arguments)]
pub async fn ingest_document_with_scope(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    doc: DocumentInput,
    path_scope: Option<String>,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    ingest_document_versioned(
        config, source_id, owner, tags, doc, path_scope, None, sink, scoring,
    )
    .await
}

/// Version-aware document ingest. When `version_ms` is `Some`, the source gate
/// is keyed by `{source_id}@{version_ms}`, so a later revision of the same
/// document is admitted non-destructively alongside the prior version.
#[allow(clippy::too_many_arguments)]
pub async fn ingest_document_versioned(
    config: &MemoryConfig,
    source_id: &str,
    owner: &str,
    tags: Vec<String>,
    doc: DocumentInput,
    path_scope: Option<String>,
    version_ms: Option<i64>,
    sink: &dyn TreeJobSink,
    scoring: &ScoringConfig,
) -> Result<IngestSummary> {
    // Best-effort pre-canonicalisation gate; the transactional claim inside
    // [`ingest_canonical`] is authoritative.
    let gate_key = match version_ms {
        Some(v) => format!("{source_id}@{v}"),
        None => source_id.to_string(),
    };
    let source_key = chunks::store_sources::source_gate_key(SourceKind::Document, &gate_key);
    if read_state_async(config, INGESTED_SOURCES, &source_key)
        .await?
        .is_some()
    {
        return Ok(IngestSummary::already_ingested(source_id));
    }

    let canonical = match document::canonicalise(source_id, owner, &tags, doc, path_scope)
        .map_err(anyhow::Error::msg)?
    {
        Some(c) => c,
        None => return Ok(IngestSummary::empty(source_id)),
    };
    let opts = IngestOptions {
        gate_version_ms: version_ms,
        raw_refs: None,
    };
    ingest_canonical(config, source_id, canonical, sink, scoring, &opts).await
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
