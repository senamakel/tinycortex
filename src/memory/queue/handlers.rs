//! Per-`JobKind` dispatch for the worker pool.
//!
//! The queue keeps its own control flow in this module — parsing payloads,
//! enqueuing follow-up jobs, deciding `Done` vs `Defer` — and pushes only the
//! genuinely external heavy work (LLM scoring/extraction, buffer pushes,
//! sealing, embedding) behind the [`QueueDelegates`] trait. The OpenHuman
//! handlers called directly into `memory_tree` / `memory_score` /
//! `memory_store`, but those operations are exposed only `pub(crate)` to their
//! own modules (and some — the write-path embedder, the re-embed worklist
//! probe, the on-disk body reader — remain host-owned computations). The trait
//! is that seam: a host that owns visibility into those modules supplies a
//! real implementation; tests supply a deterministic one (see
//! `handlers_tests.rs`).
//!
//! What stays in-crate (faithful to OpenHuman's pipeline shape):
//! - `extract_chunk` admits → plans `append_buffer`, then arms the re-embed
//!   backfill once anything was admitted.
//! - `append_buffer` plans a leaf push → plans `seal` when the gate is met.
//! - `seal` seals one level → plans the parent `seal` when cascading.
//! - `flush_stale` plans a force-`seal` per stale buffer.
//! - `reembed_backfill` maps a bounded batch to `Defer` (more pending) or
//!   `Done` (covered / no provider / stale signature); the worker updates the
//!   process-global backfill hint after lease-fenced settlement.

use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::memory::chunks::collections::{
    BUFFERS, CHUNK_EMBEDDINGS, CHUNK_LIFECYCLE, CHUNK_REEMBED_SKIPS, ENTITY_EDGES,
    ENTITY_HOTNESS, ENTITY_INDEX, SCORES, SUMMARIES, SUMMARY_CONTENT_POINTERS, TREES,
};
use crate::memory::chunks::versioned::StateChange;
use crate::memory::config::MemoryConfig;
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
    /// Per-signature summary vectors.
    SummaryEmbeddings,
    /// Content-addressed summary body pointers.
    SummaryContentPointers,
    /// Mutable tree buffers.
    TreeBuffers,
    /// Per-model chunk vectors.
    ChunkEmbeddings,
    /// Chunk re-embedding skip records.
    ChunkReembedSkips,
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
            Self::SummaryEmbeddings => crate::memory::chunks::collections::SUMMARY_EMBEDDINGS,
            Self::SummaryContentPointers => SUMMARY_CONTENT_POINTERS,
            Self::TreeBuffers => BUFFERS,
            Self::ChunkEmbeddings => CHUNK_EMBEDDINGS,
            Self::ChunkReembedSkips => CHUNK_REEMBED_SKIPS,
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
    /// Value observed by the planner. The worker rechecks it at the commit
    /// head so a slow delegate cannot overwrite a newer owner's update.
    pub expected: Option<serde_json::Value>,
    /// The replacement record, or `None` to tombstone it.
    pub value: Option<serde_json::Value>,
}

impl JobMutation {
    fn into_state_changes(self) -> (StateChange, StateChange) {
        let collection = self.collection.name().to_owned();
        (
            StateChange {
                collection: collection.clone(),
                key: self.key.clone(),
                value: self.expected,
            },
            StateChange {
                collection,
                key: self.key,
                value: self.value,
            },
        )
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

    /// Compute one buffer-level seal and return its state mutations plus a
    /// parent-level `SealPayload` when the seal cascades.
    async fn seal_level(
        &self,
        config: &MemoryConfig,
        payload: &SealPayload,
    ) -> Result<DelegateOutput<Option<SealPayload>>>;

    /// List buffers older than `max_age_secs` that should be force-sealed.
    async fn list_stale_buffers(
        &self,
        config: &MemoryConfig,
        max_age_secs: i64,
    ) -> Result<Vec<StaleBuffer>>;

    /// Build + merge one document version's subtree and return its mutations.
    async fn seal_document(
        &self,
        config: &MemoryConfig,
        payload: &SealDocumentPayload,
    ) -> Result<DelegateOutput<crate::memory::tree::DocumentSealPlanResult>>;

    /// Project committed document summaries into a host-owned mirror. Called
    /// only after queue receipt and domain state commit successfully together.
    /// A projection error is logged separately and never retries committed
    /// inference or owner mutations.
    async fn document_summaries_committed(
        &self,
        _config: &MemoryConfig,
        _tree_id: &str,
        _summaries: &[crate::memory::tree::SummaryNode],
    ) -> Result<()> {
        Ok(())
    }

    /// Embed one bounded re-embed batch at `signature` and return its mutations.
    async fn reembed_batch(
        &self,
        config: &MemoryConfig,
        signature: &str,
    ) -> Result<DelegateOutput<ReembedProgress>>;

    /// The active embedding signature (for the re-embed switch-path trigger).
    fn active_signature(&self, config: &MemoryConfig) -> String;

    /// Whether any chunk/summary lacks a vector at `signature`.
    fn has_uncovered_reembed_work(&self, config: &MemoryConfig, signature: &str) -> Result<bool>;
}

/// Handler outcome, expected state, compute-only mutations, and durable
/// follow-up jobs. The worker commits them with the lease-fenced job receipt.
pub(crate) struct JobPlan {
    pub outcome: JobOutcome,
    pub follow_ups: Vec<NewJob>,
    pub expected: Vec<StateChange>,
    pub mutations: Vec<StateChange>,
    pub document_projection: Option<DocumentProjection>,
}

pub(crate) struct DocumentProjection {
    pub tree_id: String,
    pub summaries: Vec<crate::memory::tree::SummaryNode>,
}

impl JobPlan {
    fn done(follow_ups: Vec<NewJob>) -> Self {
        Self {
            outcome: JobOutcome::Done,
            follow_ups,
            expected: Vec::new(),
            mutations: Vec::new(),
            document_projection: None,
        }
    }
}

/// Run a claimed job only when its plan has no persisted effects.
///
/// A plan with domain mutations or follow-up jobs must be settled by the
/// lease-fenced worker; this compatibility entrypoint returns an error for
/// those plans so it cannot publish state outside the owner commit.
pub async fn handle_job(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobOutcome> {
    let plan = plan_job(config, job, delegates).await?;
    if !plan.expected.is_empty()
        || !plan.mutations.is_empty()
        || !plan.follow_ups.is_empty()
        || matches!(plan.outcome, JobOutcome::Defer { .. })
    {
        anyhow::bail!("job effects require lease-fenced worker settlement; use run_once");
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
    let (expected, mutations): (Vec<_>, Vec<_>) = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_changes)
        .unzip();

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
        expected,
        mutations,
        document_projection: None,
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
    let (expected, mutations): (Vec<_>, Vec<_>) = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_changes)
        .unzip();

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
        expected,
        mutations,
        document_projection: None,
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
    let output = delegates.seal_level(config, &payload).await?;
    let (expected, mutations): (Vec<_>, Vec<_>) = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_changes)
        .unzip();
    let follow_ups = output
        .result
        .map(|parent| NewJob::seal(&parent))
        .transpose()?
        .into_iter()
        .collect();
    Ok(JobPlan {
        outcome: JobOutcome::Done,
        follow_ups,
        expected,
        mutations,
    })
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
    let output = delegates.seal_document(config, &payload).await?;
    let result = output.result;
    let (expected, mutations): (Vec<_>, Vec<_>) = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_changes)
        .unzip();
    Ok(JobPlan {
        outcome: JobOutcome::Done,
        follow_ups: result.follow_ups.clone(),
        expected,
        mutations,
        document_projection: Some(DocumentProjection {
            tree_id: result.tree_id,
            summaries: result.summaries,
        }),
    })
}

/// Run one step of the `ReembedBackfill` chain.
///
/// Computed embedding updates are returned with the job plan for atomic
/// settlement. The worker updates the process-wide backfill hint only after it
/// commits the receipt, so planning cannot expose an uncommitted completion.
async fn handle_reembed_backfill(
    config: &MemoryConfig,
    job: &Job,
    delegates: &dyn QueueDelegates,
) -> Result<JobPlan> {
    let payload: ReembedBackfillPayload = parse_payload(job, "ReembedBackfill")?;

    let output = delegates.reembed_batch(config, &payload.signature).await?;
    let (expected, mutations): (Vec<_>, Vec<_>) = output
        .mutations
        .into_iter()
        .map(JobMutation::into_state_changes)
        .unzip();
    let plan = |outcome| JobPlan {
        outcome,
        follow_ups: Vec::new(),
        expected,
        mutations,
        document_projection: None,
    };
    match output.result {
        ReembedProgress::Wrote {
            more_pending: true, ..
        } => {
            // More rows may remain — reschedule THIS row (no re-enqueue, so the
            // per-signature dedupe key stays valid).
            Ok(plan(JobOutcome::Defer {
                until_ms: chrono::Utc::now().timestamp_millis() + REEMBED_BACKFILL_REVISIT_MS,
                reason: "re-embed backfill: batch done, more pending".to_string(),
            }))
        }
        ReembedProgress::Wrote {
            more_pending: false,
        }
        | ReembedProgress::Covered
        | ReembedProgress::NoProvider
        | ReembedProgress::StaleSignature => Ok(plan(JobOutcome::Done)),
    }
}

#[cfg(test)]
#[path = "handlers_tests.rs"]
mod tests;
