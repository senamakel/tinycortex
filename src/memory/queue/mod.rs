//! Async job queue for memory-tree work.
//!
//! Replaces the synchronous `append_leaf → cascade_seal → LLM summarise` chain
//! on the ingest hot path with a host-backed job queue and a worker driver.
//! The shape is:
//!
//! ```text
//! ingest::persist
//!   └── writes chunk row (lifecycle = pending_extraction)
//!       enqueues `extract_chunk`
//!
//! run_once (driven in a host loop) claims jobs by kind:
//!   extract_chunk   → score/admit → enqueue append_buffer + arm reembed
//!   append_buffer   → push to L0 → enqueue seal if gate met
//!   seal            → seal one level → enqueue parent seal if cascading
//!   flush_stale     → enqueue force-seals for time-stale buffers
//!   reembed_backfill→ embed a bounded batch → Defer until covered
//!   seal_document   → build one document version's subtree
//! ```
//!
//! Queue jobs and chunk/tree state share one owner commit head. A worker
//! computes domain changes and follow-up intents first, then settles the job
//! lease and publishes those changes under one head compare-and-swap. The
//! external queue is a delivery hint; the owner records remain authoritative.
//!
//! ## Differences from OpenHuman
//!
//! - The `tokio` worker pool and wall-clock scheduler are optional under the
//!   `tokio` feature. Hosts can instead drive [`run_once`] and the plain
//!   scheduler functions themselves; [`drain_until_idle`] settles the queue
//!   deterministically in tests.
//! - The heavy per-kind work (LLM scoring/extraction, buffer push, sealing,
//!   embedding) sits behind the [`QueueDelegates`] trait because those
//!   operations are exposed only `pub(crate)` to `memory::tree` / `memory::score`
//!   / `memory::chunks` (and some are not part of the ported surface). The
//!   queue keeps payload parsing, follow-up planning, gating and defer logic
//!   in-crate; its worker commits returned mutations with the lease receipt.
//! - The typed-failure classifier is the self-contained [`JobFailure`] rather
//!   than the upstream `memory_tree::health::PipelineFailure`.

pub mod gate;
mod handlers;
mod ops;
mod payloads;
mod redact;
/// Always-on async background worker + scheduler loops (feature `tokio`).
///
/// Wraps the host-driven [`run_once`] / [`scheduler`] primitives into
/// cancellable `tokio` loops. Gated behind the `tokio` feature so the core
/// stays synchronous and dependency-light.
#[cfg(feature = "tokio")]
pub mod runtime;
pub mod scheduler;
pub mod store;
pub mod store_settle;
pub mod testing;
pub mod types;
pub mod worker;

/// TSD queue carrying persisted memory-tree worker jobs.
pub const QUEUE_NAME: &str = "memory-tree-jobs";

#[cfg(test)]
pub(crate) mod test_support;

pub use gate::{DEFAULT_LLM_PERMITS, LlmGate, Permit};
pub use handlers::{
    AppendDecision, DelegateOutput, ExtractDecision, JobMutation, JobStateCollection,
    QueueDelegates, ReembedProgress, StaleBuffer, handle_job,
};
pub use ops::{backfill_in_progress, plan_reembed_backfill, set_backfill_in_progress};
pub use redact::scrub_for_log;
pub use store::{
    DEFAULT_LOCK_DURATION_MS, claim_next, count_by_status, count_failed_unrecoverable, count_total,
    enqueue, get_job, is_retired_kind, purge_retired_jobs,
};
pub use store_settle::{
    mark_deferred, mark_done, mark_failed, mark_failed_typed, recover_stale_locks,
    release_running_locks, requeue_failed, requeue_transient_failed, retry_all_failed,
};
pub use testing::drain_until_idle;
pub use types::{
    AppendBufferPayload, AppendTarget, ExtractChunkPayload, FlushStalePayload, Job, JobFailure,
    JobKind, JobOutcome, JobStatus, NewJob, NodeRef, ReembedBackfillPayload, SealDocumentPayload,
    SealPayload,
};
pub use worker::{bootstrap, llm_gate, run_once};
