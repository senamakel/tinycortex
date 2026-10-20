//! Per-`JobKind` dispatch for the worker pool.
//!
//! The queue keeps its own control flow in this module — parsing payloads,
//! enqueuing follow-up jobs, deciding `Done` vs `Defer` — and pushes only the
//! genuinely external heavy work (LLM scoring/extraction, buffer pushes,
//! sealing, embedding) behind the [`QueueDelegates`] trait. The OpenHuman
//! handlers called directly into `memory_tree` / `memory_score` /
//! `memory_store`, but those operations are exposed only `pub(crate)` to their
//! own modules (and some — the write-path embedder, the re-embed worklist
//! probe, the on-disk body reader — are not part of this crate's ported
//! surface at all). The trait is that seam: a host that owns visibility into
//! those modules supplies a real implementation; tests supply a deterministic
//! one (see `handlers_tests.rs`).
//!
//! What stays in-crate (faithful to OpenHuman's pipeline shape):
//! - `extract_chunk` admits → enqueues `append_buffer`, then arms the re-embed
//!   backfill once anything was admitted.
//! - `append_buffer` pushes a leaf → enqueues `seal` when the gate is met.
//! - `seal` seals one level → enqueues the parent `seal` when cascading.
//! - `flush_stale` enqueues a force-`seal` per stale buffer.
//! - `reembed_backfill` maps a bounded batch to `Defer` (more pending) or
//!   `Done` (covered / no provider / stale signature), toggling the
//!   process-global backfill flag.

use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::memory::chunks::collections::{
    BUFFERS, CHUNK_LIFECYCLE, ENTITY_EDGES, ENTITY_HOTNESS, ENTITY_INDEX, SCORES, SUMMARIES, TREES,
};
use crate::memory::chunks::versioned::StateChange;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::ops::set_backfill_in_progress;
use crate::memory::queue::store;
use crate::memory::queue::types::{
    AppendBufferPayload, AppendTarget, ExtractChunkPayload, FlushStalePayload, Job, JobFailure,
    JobKind, JobOutcome, NewJob, NodeRef, ReembedBackfillPayload, SealDocumentPayload, SealPayload,
};

/// Deserialize a claimed job's `payload_json` into its typed payload,
/// classifying any parse failure as an **unrecoverable** [`JobFailure`].
///
/// A malformed payload is a deterministic input defect: the identical bytes can
/// never parse, so retrying is hopeless. The typed failure is attached to the
/// `anyhow` chain (the worker downcasts it back out at settle time via
/// [`mark_failed_typed`](crate::memory::queue::store_settle::mark_failed_typed))
/// so the poison job is parked as `failed` with `failure_class = unrecoverable`
/// on the first attempt — instead of surfacing as an untyped error that leaves
/// `failure_class` NULL and lets `self_heal`
/// ([`requeue_transient_failed`](crate::memory::queue::store_settle::requeue_transient_failed))
/// resurrect it every scheduler tick forever (QI-2).
fn parse_payload<T: DeserializeOwned>(job: &Job, what: &str) -> Result<T> {
    serde_json::from_str(&job.payload_json).map_err(|err| {
        anyhow::Error::new(err)
            .context(JobFailure::unrecoverable("malformed_payload"))
            .context(format!("parse {what} payload"))
    })
}

/// Default age for an L0 `flush_stale` when the payload doesn't override it.
/// One hour means low-volume sources get summaries within a working session.
pub const L0_DEFAULT_FLUSH_AGE_SECS: i64 = 60 * 60;

/// Delay before a deferred re-embed chain revisits its own row.
pub const REEMBED_BACKFILL_REVISIT_MS: i64 = 750;

/// Outcome of the external `extract_chunk` computation (LLM scoring +
/// admission). The queue uses it to decide the follow-up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractDecision {
    /// Whether the chunk was admitted into the tree pipeline.
    pub kept: bool,
    /// Whether the chunk's source uses the per-document rollup path (Notion),
    /// which builds its tree via `SealDocument` rather than the flat L0 buffer.
    pub uses_document_subtree: bool,
    /// The source-tree scope to append this leaf under (GitHub-aware / path
    /// scope). Only consulted when `kept && !uses_document_subtree`.
    pub tree_scope: String,
}

/// Outcome of the external `append_buffer` step (buffer push + gate check).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendDecision {
    /// The physical tree id the leaf landed in.
    pub tree_id: String,
    /// Whether the L0 buffer crossed its seal gate during this push.
    pub should_seal: bool,
}

/// A stale buffer that `flush_stale` should force-seal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleBuffer {
    /// Physical tree id of the stale buffer to force-seal.
    pub tree_id: String,
    /// Buffer level (0 = leaf L0; higher = summary tiers).
    pub level: u32,
}

/// Outcome of one external re-embed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReembedProgress {
    /// A batch was embedded; `more_pending` drives `Defer` vs `Done`.
    Wrote {
        /// Whether more rows remain to re-embed (drives `Defer` vs `Done`).
        more_pending: bool,
    },
    /// The signature space is fully covered — finish the chain.
    Covered,
    /// No usable embeddings provider — skip (rows stay re-embeddable).
    NoProvider,
    /// The active signature changed since this chain started — finish.
    StaleSignature,
}

/// A supported memory collection that a queue delegate may change as the
/// result of one job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStateCollection {
    /// Per-chunk lifecycle status.
    ChunkLifecycle,
    /// Chunk admission and extraction scores.
    ChunkScores,
    /// Tree roots and scope bindings.
    Trees,
    /// Sealed tree summaries.
    Summaries,
    /// Mutable tree buffers.
    TreeBuffers,
    /// Entity-to-node occurrences.
    EntityIndex,
    /// Entity co-occurrence edges.
    EntityEdges,
    /// Entity hotness summaries.
    EntityHotness,
}

impl JobStateCollection {
    fn name(self) -> &'static str {
        match self {
            Self::ChunkLifecycle => CHUNK_LIFECYCLE,
            Self::ChunkScores => SCORES,
            Self::Trees => TREES,
            Self::Summaries => SUMMARIES,
            Self::TreeBuffers => BUFFERS,
            Self::EntityIndex => ENTITY_INDEX,
            Self::EntityEdges => ENTITY_EDGES,
            Self::EntityHotness => ENTITY_HOTNESS,
        }
    }
}

/// One domain mutation returned by a compute-only queue delegate.
#[derive(Clone, Debug, PartialEq)]
pub struct JobMutation {
    /// The owner collection changed by the job.
    pub collection: JobStateCollection,
    /// Stable logical record key in that collection.
    pub key: String,
    /// The replacement record, or `None` to tombstone it.
    pub value: Option<serde_json::Value>,
}

impl JobMutation {
    fn into_state_change(self) -> StateChange {
        StateChange {
            collection: self.collection.name().to_owned(),
            key: self.key,
            value: self.value,
        }
    }
}

/// A delegate's computed result and the records it wants committed with the
/// job receipt and follow-up intents.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegateOutput<T> {
    /// Computed decision returned to the handler.
    pub result: T,
    /// Domain changes that the queue owner commits under the same head CAS.
    pub mutations: Vec<JobMutation>,
}

/// The external, heavy-work seam for the queue handlers. Every method is the
/// part the queue cannot do itself (it needs host-owned inference and domain
/// computation). Methods returning [`DelegateOutput`] are compute-only: their
/// mutations are committed together with the job receipt and follow-up
/// intents. Expensive inference may be repeated after a crash, so it should be
/// deterministic where possible, but delegates must not persist domain state.
#[async_trait]
pub trait QueueDelegates: Send + Sync {
    /// Score + admit one chunk and return score/lifecycle mutations without
    /// persisting them. `Ok(None)` when the chunk row is missing.
    async fn extract_chunk(
        &self,
        config: &MemoryConfig,
        chunk_id: &str,
    ) -> Result<Option<DelegateOutput<ExtractDecision>>>;

    /// Compute a node's buffer append and return its mutations without
    /// persisting them. `Ok(None)` when the node or target tree is missing.
    async fn append_node(
        &self,
        config: &MemoryConfig,
        node: &NodeRef,
        target: &AppendTarget,
    ) -> Result<Option<DelegateOutput<AppendDecision>>>;

    /// Seal one buffer level. Returns the parent-level `SealPayload` to enqueue
    /// when the seal cascades, else `None`.
    async fn seal_level(
        &self,
        config: &MemoryConfig,
        payload: &SealPayload,
    ) -> Result<Option<SealPayload>>;

    /// List buffers older than `max_age_secs` that should be force-sealed.
    async fn list_stale_buffers(
        &self,
        config: &MemoryConfig,
        max_age_secs: i64,
    ) -> Result<Vec<StaleBuffer>>;

    /// Build + merge one document version's subtree.
    async fn seal_document(
        &self,
        config: &MemoryConfig,
        payload: &SealDocumentPayload,
    ) -> Result<()>;

    /// Embed one bounded re-embed batch at `signature`.
    async fn reembed_batch(
        &self,
        config: &MemoryConfig,
        signature: &str,
    ) -> Result<ReembedProgress>;

    /// The active embedding signature (for the re-embed switch-path trigger).
    fn active_signature(&self, config: &MemoryConfig) -> String;

    /// Whether any chunk/summary lacks a vector at `signature`.
    fn has_uncovered_reembed_work(&self, config: &MemoryConfig, signature: &str) -> Result<bool>;
}

/// Handler outcome, compute-only domain mutations, and durable follow-up jobs.
/// The worker commits all three with the lease-fenced job receipt.
pub(crate) struct JobPlan {
    pub outcome: JobOutcome,
    pub follow_ups: Vec<NewJob>,
    pub mutations: Vec<StateChange>,
}

impl JobPlan {
    fn done(follow_ups: Vec<NewJob>) -> Self {
        Self {
            outcome: JobOutcome::Done,
            follow_ups,
            mutations: Vec::new(),
        }
    }

    fn outcome(outcome: JobOutcome) -> Self {
        Self {
            outcome,
            follow_ups: Vec::new(),
            mutations: Vec::new(),
        }
    }
}

/// Dispatch a claimed job to the matching per-kind handler.
pub async fn handle_job(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobOutcome> {
    let plan = plan_job(config, job, delegates).await?;
    if !plan.mutations.is_empty() {
        anyhow::bail!("domain mutations require lease-fenced worker settlement");
    }
    for follow_up in &plan.follow_ups {
        if store::enqueue(config, follow_up)?.is_some()
            && follow_up.kind == JobKind::ReembedBackfill
        {
            set_backfill_in_progress(true);
        }
    }
    Ok(plan.outcome)
}

/// Plan a handler result without persisting follow-ups. Used by the worker so
/// parent settlement and child enqueue cannot be separated by a crash.
pub(crate) async fn plan_job(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    match job.kind {
        JobKind::ExtractChunk => handle_extract(config, job, delegates).await,
        JobKind::AppendBuffer => handle_append_buffer(config, job, delegates).await,
        JobKind::Seal => handle_seal(config, job, delegates).await,
        JobKind::FlushStale => handle_flush_stale(config, job, delegates).await,
        JobKind::ReembedBackfill => handle_reembed_backfill(config, job, delegates).await,
        JobKind::SealDocument => handle_seal_document(config, job, delegates).await,
    }
}

/// Run the `ExtractChunk` handler.
///
/// Payload-parse failures here return a plain `anyhow::Error` (via
/// `.context(...)`), not a [`JobFailure::unrecoverable`] — the worker
/// (`settle_job` in [`super::worker`]) therefore records `failure_class =
/// NULL` on terminal failure. A malformed/legacy payload is a permanent
/// defect, but with `failure_class = NULL` it is indistinguishable from a
/// classifiable transient error to
/// [`requeue_transient_failed`](crate::memory::queue::store_settle::requeue_transient_failed)'s
/// predicate (`failure_class IS NULL OR != 'unrecoverable'`), so `self_heal`
/// resurrects it on every scheduler tick forever. Every payload-parse branch
/// in this module has the same gap; fixing it means classifying the parse
/// error as [`JobFailure::unrecoverable`] before returning it, so a bad
/// payload fails fast instead of retry-looping.
async fn handle_extract(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: ExtractChunkPayload = parse_payload(job, "ExtractChunk")?;
    let Some(output) = delegates.extract_chunk(config, &payload.chunk_id).await? else {
        // Chunk row vanished between enqueue and claim — nothing to do.
        return Ok(JobPlan::done(Vec::new()));
    };
    let decision = output.result;
    let mutations = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_change)
        .collect();

    let mut follow_ups = Vec::new();

    // Admitted, flat-buffer source: enqueue the append-buffer follow-up. The
    // per-document-versioned sources (Notion) skip the flat L0 buffer — their
    // tree is built by a SealDocument job enqueued at ingest.
    if decision.kept && !decision.uses_document_subtree {
        let follow_up = NewJob::append_buffer(&AppendBufferPayload {
            node: NodeRef::Leaf {
                chunk_id: payload.chunk_id.clone(),
            },
            target: AppendTarget::Source {
                source_id: decision.tree_scope.clone(),
            },
        })?;
        follow_ups.push(follow_up);
    }

    // Anything admitted arms the re-embed backfill so the embedding pass starts
    // promptly (extract no longer embeds inline).
    if decision.kept {
        if let Some(job) = crate::memory::queue::ops::planned_reembed_backfill(config, delegates)? {
            follow_ups.push(job);
        }
    }

    Ok(JobPlan {
        outcome: JobOutcome::Done,
        follow_ups,
        mutations,
    })
}

async fn handle_append_buffer(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: AppendBufferPayload = parse_payload(job, "AppendBuffer")?;
    let Some(output) = delegates
        .append_node(config, &payload.node, &payload.target)
        .await?
    else {
        // Missing chunk/summary, or the target topic tree was archived — drop.
        return Ok(JobPlan::done(Vec::new()));
    };
    let decision = output.result;
    let mutations = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_change)
        .collect();

    let mut follow_ups = Vec::new();
    if decision.should_seal {
        let seal = SealPayload {
            tree_id: decision.tree_id,
            level: 0,
            force_now_ms: None,
        };
        follow_ups.push(NewJob::seal(&seal)?);
    }
    Ok(JobPlan {
        outcome: JobOutcome::Done,
        follow_ups,
        mutations,
    })
}

/// Run the `Seal` handler for exactly one buffer level.
///
/// A cascading seal returns the parent payload so each level stays its own
/// crash-recovery checkpoint (see [`SealPayload::dedupe_key`]).
///
/// Settlement re-checks the live same-level buffer after releasing this job's
/// active dedupe key. Content appended while the seal was running therefore
/// restores a suppressed seal edge immediately when it crosses the gate.
async fn handle_seal(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: SealPayload = parse_payload(job, "Seal")?;
    // Seal exactly one level. A cascading seal returns the parent payload so
    // each level stays its own crash-recovery checkpoint.
    let follow_ups = delegates
        .seal_level(config, &payload)
        .await?
        .map(|parent| NewJob::seal(&parent))
        .transpose()?
        .into_iter()
        .collect();
    Ok(JobPlan::done(follow_ups))
}

async fn handle_flush_stale(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: FlushStalePayload = parse_payload(job, "FlushStale")?;
    let age_secs = payload.max_age_secs.unwrap_or(L0_DEFAULT_FLUSH_AGE_SECS);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut follow_ups = Vec::new();
    for buf in delegates.list_stale_buffers(config, age_secs).await? {
        let seal = SealPayload {
            tree_id: buf.tree_id,
            level: buf.level,
            force_now_ms: Some(now_ms),
        };
        follow_ups.push(NewJob::seal(&seal)?);
    }
    Ok(JobPlan::done(follow_ups))
}

async fn handle_seal_document(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: SealDocumentPayload = parse_payload(job, "SealDocument")?;
    if payload.chunk_ids.is_empty() {
        // Empty version set — nothing to seal.
        return Ok(JobPlan::done(Vec::new()));
    }
    delegates.seal_document(config, &payload).await?;
    Ok(JobPlan::done(Vec::new()))
}

/// Run one step of the `ReembedBackfill` chain.
///
/// Only the success paths (`Wrote`, `Covered`, `NoProvider`,
/// `StaleSignature`) clear [`set_backfill_in_progress`]; if
/// `delegates.reembed_batch` returns `Err` the job instead flows through
/// `settle_job` (in [`super::worker`]) and is marked `failed` — terminally so
/// once its attempt budget is exhausted — without this function ever running
/// again to clear the flag. [`crate::memory::queue::ops::backfill_in_progress`]
/// then stays `true` until the process restarts, and retrieval keeps treating
/// every empty vector-search result as "not searched yet" rather than "no
/// such memory" for the remainder of the process's life.
async fn handle_reembed_backfill(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: ReembedBackfillPayload = parse_payload(job, "ReembedBackfill")?;

    match delegates.reembed_batch(config, &payload.signature).await? {
        ReembedProgress::Wrote {
            more_pending: true, ..
        } => {
            set_backfill_in_progress(true);
            // More rows may remain — reschedule THIS row (no re-enqueue, so the
            // per-signature dedupe key stays valid).
            Ok(JobPlan::outcome(JobOutcome::Defer {
                until_ms: chrono::Utc::now().timestamp_millis() + REEMBED_BACKFILL_REVISIT_MS,
                reason: "re-embed backfill: batch done, more pending".to_string(),
            }))
        }
        ReembedProgress::Wrote {
            more_pending: false,
        }
        | ReembedProgress::Covered
        | ReembedProgress::NoProvider
        | ReembedProgress::StaleSignature => {
            set_backfill_in_progress(false);
            Ok(JobPlan::done(Vec::new()))
        }
    }
}

#[cfg(test)]
#[path = "handlers_tests.rs"]
mod tests;
