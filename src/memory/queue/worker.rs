//! Worker driver: claim one job, run its handler under the LLM gate, settle.
//!
//! The durable primitive is [`run_once`]: a
//! single claim→handle→settle step that a host drives in its own loop (and that
//! [`drain_until_idle`](crate::memory::queue::drain_until_idle) calls to settle
//! the queue deterministically in tests).
//!
//! Queue storage errors retain their typed categories. `Unavailable` and
//! `Conflict` can be retried by a host loop; a generic `Backend` error is
//! returned as fatal because the storage port has no corruption category and
//! the host cannot safely infer one from diagnostic text. [`is_host_io_error`]
//! recognizes persistent filesystem failures surfaced by file operations.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::Result;
#[cfg(any(feature = "tokio", test))]
use tinystoragedrivers_types::{ErrorKind, StorageError};

use crate::memory::config::MemoryConfig;
use crate::memory::queue::gate::{LlmGate, Permit};
use crate::memory::queue::handlers::{self, QueueDelegates};
use crate::memory::queue::ops::set_backfill_in_progress;
#[cfg(test)]
use crate::memory::queue::store::claim_next;
use crate::memory::queue::store::purge_retired_jobs;
#[cfg(test)]
use crate::memory::queue::store_settle::mark_failed_typed;
use crate::memory::queue::store_settle::recover_stale_locks;
use crate::memory::queue::types::{Job, JobFailure, JobKind, JobOutcome, JobStatus};
#[cfg(test)]
use crate::memory::queue::types::{NewJob, SealPayload};

/// Process-wide LLM concurrency gate. Single-slot by default (mirrors the
/// upstream single-permit semaphore); LLM-bound jobs hold a permit for the
/// duration of their handler.
static LLM_GATE: LazyLock<LlmGate> = LazyLock::new(LlmGate::default);
static CONFIGURED_LLM_GATES: LazyLock<parking_lot::Mutex<HashMap<usize, Arc<LlmGate>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// Short delay used when an LLM job is claimed while the process-wide gate is
/// full. Deferring releases the job lease and attempt immediately, avoiding a
/// blocking wait on an async executor thread.
const LLM_GATE_RETRY_MS: i64 = 50;

/// The global LLM gate (exposed so hosts/tests can inspect or share it).
pub fn llm_gate() -> &'static LlmGate {
    &LLM_GATE
}

/// Startup housekeeping: purge retired-kind rows and recover any leases left
/// `running` by a previous hard kill. Returns `(purged, recovered)` counts.
/// A host calls this once before entering its `run_once` loop.
pub fn bootstrap(config: &MemoryConfig) -> Result<(usize, usize)> {
    let purged = purge_retired_jobs(config)?;
    let recovered = recover_stale_locks(config)?;
    crate::memory::queue::store::repair_pending_hints(config)?;
    Ok((purged, recovered))
}

#[cfg(feature = "tokio")]
pub(crate) async fn bootstrap_async(config: &MemoryConfig) -> Result<(usize, usize)> {
    let purged = crate::memory::queue::store::purge_retired_jobs_async(config).await?;
    let recovered = crate::memory::queue::store_settle::recover_stale_locks_async(config).await?;
    crate::memory::queue::store::repair_pending_hints_async(config).await?;
    Ok((purged, recovered))
}

/// Claim and run a single job. Returns `true` when work was processed, `false`
/// when no eligible row was available.
///
/// LLM-bound jobs use [`LlmGate::try_acquire`] and defer when no permit is
/// available, so this async entry point does not block a Tokio executor while
/// waiting for capacity. All queue-state reads, writes, and settlements use
/// the async versioned-store path, including on current-thread runtimes.
pub async fn run_once(config: &MemoryConfig, delegates: &dyn QueueDelegates) -> Result<bool> {
    if config.storage().is_ok() {
        crate::memory::ingest::pipeline::dispatch_pending_extract_jobs_async(
            config,
            &crate::memory::ingest::QueueJobSink,
        )
        .await?;
    }
    let gate = {
        let mut gates = CONFIGURED_LLM_GATES.lock();
        Arc::clone(
            gates
                .entry(config.queue.llm_permits)
                .or_insert_with(|| Arc::new(LlmGate::new(config.queue.llm_permits))),
        )
    };
    run_once_with_gate(config, delegates, &gate).await
}

async fn run_once_with_gate(
    config: &MemoryConfig,
    delegates: &dyn QueueDelegates,
    gate: &LlmGate,
) -> Result<bool> {
    let Some(job) =
        crate::memory::queue::store::claim_next_async(config, config.queue.lock_duration_ms)
            .await?
    else {
        return Ok(false);
    };

    // LLM-bound jobs hold a permit from the global gate for the lifetime of the
    // handler; non-LLM jobs (AppendBuffer, FlushStale) run without one.
    let permit: Option<Permit> = if job.kind.is_llm_bound() {
        let Some(permit) = gate.try_acquire() else {
            crate::memory::queue::store_settle::mark_deferred_async(
                config,
                &job,
                chrono::Utc::now().timestamp_millis() + LLM_GATE_RETRY_MS,
                "llm concurrency gate busy",
            )
            .await?;
            if job.kind == JobKind::ReembedBackfill
                && matches!(
                    crate::memory::queue::store::get_job_async(config, &job.id).await?,
                    Some(current) if current.status == JobStatus::Ready
                )
            {
                set_backfill_in_progress(true);
            }
            return Ok(true);
        };
        Some(permit)
    } else {
        None
    };

    let result = handlers::plan_job(config, &job, delegates).await;
    drop(permit);

    settle_planned_job_async(config, &job, result, delegates).await?;
    Ok(true)
}

/// Translate a handler plan into the matching store settlement call. Successful
/// follow-ups and staged domain changes commit with `mark_done` under the same
/// owner-state head CAS.
/// Errors from the settlement call itself (e.g. a busy database) propagate to
/// the caller — the job's `running` row is then left for lease-expiry
/// recovery rather than being retried inline here.
#[cfg(test)]
fn settle_job(config: &MemoryConfig, job: &Job, result: Result<JobOutcome>) -> Result<()> {
    settle_planned_job(
        config,
        job,
        result.map(|outcome| handlers::JobPlan {
            outcome,
            follow_ups: Vec::new(),
            expected: Vec::new(),
            mutations: Vec::new(),
            document_projection: None,
        }),
    )
}

#[cfg(test)]
fn settle_planned_job(
    config: &MemoryConfig,
    job: &Job,
    result: Result<handlers::JobPlan>,
) -> Result<()> {
    match result {
        Ok(handlers::JobPlan {
            outcome: JobOutcome::Done,
            follow_ups,
            expected,
            mutations,
            ..
        }) => {
            let arms_reembed = follow_ups
                .iter()
                .any(|follow_up| follow_up.kind == JobKind::ReembedBackfill);
            let committed = crate::memory::queue::store_settle::mark_done_with_changes(
                config,
                job,
                &follow_ups,
                &expected,
                &mutations,
            )?;
            if committed && job.kind == JobKind::ReembedBackfill {
                set_backfill_in_progress(arms_reembed);
            } else if committed && arms_reembed {
                set_backfill_in_progress(true);
            }

            // A same-level seal enqueue can be suppressed while this row is
            // running. Once its dedupe key is released, re-check the live
            // buffer and restore the edge if content appended during the seal
            // crossed the gate again.
            if job.kind == JobKind::Seal
                && matches!(
                    crate::memory::queue::store::get_job(config, &job.id)?,
                    Some(current) if current.status == JobStatus::Done
                )
            {
                let payload: SealPayload = serde_json::from_str(&job.payload_json)?;
                let buffer = crate::memory::tree::store::get_buffer(
                    config,
                    &payload.tree_id,
                    payload.level,
                )?;
                if crate::memory::tree::should_seal(config, &buffer) {
                    crate::memory::queue::store::enqueue(config, &NewJob::seal(&payload)?)?;
                }
            }
            Ok(())
        }
        Ok(handlers::JobPlan {
            outcome: JobOutcome::Defer { until_ms, reason },
            expected,
            mutations,
            ..
        }) => {
            // Defer is normal operation (transient blocker) — does NOT burn the
            // failure budget; `mark_deferred` reverts the claim's attempts bump.
            let committed = crate::memory::queue::store_settle::mark_deferred_with_changes(
                config, job, until_ms, &reason, &expected, &mutations,
            )?;
            if committed && job.kind == JobKind::ReembedBackfill {
                let still_pending = matches!(
                    crate::memory::queue::store::get_job(config, &job.id)?,
                    Some(current) if current.status == JobStatus::Ready
                );
                set_backfill_in_progress(still_pending);
            }
            Ok(())
        }
        Err(err) => {
            // Preserve the full anyhow cause chain in `last_error` so a reader
            // can see the root cause. If the chain carries a typed failure,
            // pass it through so an unrecoverable cause fails fast instead of
            // burning the retry budget.
            let message = format!("{err:#}");
            // The pipeline's own failure taxonomy (`health::PipelineFailure`) is
            // the type `classify_embed_error` returns; handlers wrap it in the
            // anyhow chain with `.context(...)`. Map it onto the queue's
            // settlement type so unrecoverable codes (`budget_exhausted`,
            // `auth_invalid`, …) still fail fast.
            let converted_pipeline_failure = err
                .downcast_ref::<crate::memory::health::PipelineFailure>()
                .map(|f| JobFailure {
                    code: f.code.as_str(),
                    class: f.class.as_str(),
                });
            let typed = err
                .downcast_ref::<JobFailure>()
                .or(converted_pipeline_failure.as_ref());
            mark_failed_typed(config, job, &message, typed)?;

            // The handler clears this flag on every successful terminal
            // outcome. Mirror that cleanup when a re-embed row exhausts its
            // budget (or fails fast), otherwise retrieval remains in the
            // process-wide "backfill pending" mode indefinitely.
            if job.kind == JobKind::ReembedBackfill
                && matches!(
                    crate::memory::queue::store::get_job(config, &job.id)?,
                    Some(current) if current.status == JobStatus::Failed
                )
            {
                set_backfill_in_progress(false);
            }
            Ok(())
        }
    }
}

async fn settle_planned_job_async(
    config: &MemoryConfig,
    job: &Job,
    result: Result<handlers::JobPlan>,
    delegates: &dyn QueueDelegates,
) -> Result<()> {
    match result {
        Ok(handlers::JobPlan {
            outcome: JobOutcome::Done,
            follow_ups,
            expected,
            mutations,
            document_projection,
        }) => {
            let arms_reembed = follow_ups
                .iter()
                .any(|follow_up| follow_up.kind == JobKind::ReembedBackfill);
            let committed = crate::memory::queue::store_settle::mark_done_with_changes_async(
                config,
                job,
                &follow_ups,
                &expected,
                &mutations,
            )
            .await?;
            if committed && job.kind == JobKind::ReembedBackfill {
                set_backfill_in_progress(arms_reembed);
            } else if committed && arms_reembed {
                set_backfill_in_progress(true);
            }
            if committed {
                if let Some(projection) = document_projection {
                    if let Err(error) = delegates
                        .document_summaries_committed(
                            config,
                            &projection.tree_id,
                            &projection.summaries,
                        )
                        .await
                    {
                        log::error!(
                            "document summary projection failed after durable settlement for tree {}: {error:#}",
                            projection.tree_id
                        );
                    }
                }
            }
            // Post-seal rechecks are intentionally kept out of this async
            // path until the tree store exposes its owner-state async read.
            // The staged seal/append plan must carry any follow-up intent in
            // the same CAS; a synchronous storage read cannot run on a
            // current-thread executor.
            Ok(())
        }
        Ok(handlers::JobPlan {
            outcome: JobOutcome::Defer { until_ms, reason },
            expected,
            mutations,
            ..
        }) => {
            let committed = crate::memory::queue::store_settle::mark_deferred_with_changes_async(
                config, job, until_ms, &reason, &expected, &mutations,
            )
            .await?;
            if committed && job.kind == JobKind::ReembedBackfill {
                let still_pending = matches!(
                    crate::memory::queue::store::get_job_async(config, &job.id).await?,
                    Some(current) if current.status == JobStatus::Ready
                );
                set_backfill_in_progress(still_pending);
            }
            Ok(())
        }
        Err(err) => {
            let message = format!("{err:#}");
            let converted_pipeline_failure = err
                .downcast_ref::<crate::memory::health::PipelineFailure>()
                .map(|f| JobFailure {
                    code: f.code.as_str(),
                    class: f.class.as_str(),
                });
            let typed = err
                .downcast_ref::<JobFailure>()
                .or(converted_pipeline_failure.as_ref());
            crate::memory::queue::store_settle::mark_failed_typed_async(
                config, job, &message, typed,
            )
            .await?;
            if job.kind == JobKind::ReembedBackfill
                && matches!(
                    crate::memory::queue::store::get_job_async(config, &job.id).await?,
                    Some(current) if current.status == JobStatus::Failed
                )
            {
                set_backfill_in_progress(false);
            }
            Ok(())
        }
    }
}

/// Return the typed storage category, including through `anyhow::Context`.
#[cfg(any(feature = "tokio", test))]
pub(crate) fn storage_error_kind(err: &anyhow::Error) -> Option<ErrorKind> {
    err.downcast_ref::<StorageError>().map(StorageError::kind)
}

/// Whether a typed storage failure is retryable without changing the request.
#[cfg(test)]
pub(crate) fn is_retryable_storage_error(err: &anyhow::Error) -> bool {
    matches!(
        storage_error_kind(err),
        Some(ErrorKind::Unavailable | ErrorKind::Conflict)
    )
}

/// Whether the error is a typed generic backend failure that the host must
/// surface. The storage contract has no corruption category; diagnostic text
/// is not used to guess one or trigger filesystem recovery.
#[cfg(any(feature = "tokio", test))]
pub(crate) fn is_fatal_storage_error(err: &anyhow::Error) -> bool {
    matches!(storage_error_kind(err), Some(ErrorKind::Backend))
}

/// Classify a persistent **host-filesystem** I/O failure (EIO `5`, ENOSPC `28`,
/// EROFS `30`) surfaced as a `std::io::Error` — a dying SD card, a full disk, or
/// a kernel-remounted-read-only mount. These are user-only-fixable and never
/// clear on their own, so a host loop should back off long and page **once**
/// rather than re-poll and flood (Sentry CORE-RUST-19J).
///
/// This family is the raw OS error bubbling out of `create_dir_all` / `File`
/// operations, often through anyhow context layers,
/// so both the typed downcast and the flattened `(os error N)` text are checked.
/// EACCES (`13`) and ENOENT (`2`) are deliberately excluded — those are genuine
/// bugs that must keep reporting.
pub fn is_host_io_error(err: &anyhow::Error) -> bool {
    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        if matches!(io_err.raw_os_error(), Some(5) | Some(28) | Some(30)) {
            return true;
        }
    }
    let msg = format!("{err:#}").to_ascii_lowercase();
    msg.contains("(os error 5)") || msg.contains("(os error 28)") || msg.contains("(os error 30)")
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
