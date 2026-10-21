//! Lease-fenced state transitions for durable queue jobs.

use anyhow::Result;
use chrono::Utc;

use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::queue::store::{self, JobRecord};
use crate::memory::queue::types::{Job, JobFailure, JobStatus, NewJob};

/// Mark a claimed job complete. A stale claim is a harmless no-op.
pub fn mark_done(config: &MemoryConfig, job: &Job) -> Result<()> {
    mark_done_with_followups(config, job, &[]).map(|_| ())
}

/// Commit the completion and all follow-up jobs with delegate-produced domain
/// mutations in one versioned-head CAS. The queue receipt is acknowledged only
/// after that durable commit succeeds.
pub(crate) fn mark_done_with_changes(
    config: &MemoryConfig,
    job: &Job,
    follow_ups: &[NewJob],
    expected: &[StateChange],
    domain_changes: &[StateChange],
) -> Result<bool> {
    let mut next = JobRecord::from_public(job);
    next.status = JobStatus::Done.as_str().to_owned();
    next.completed_at_ms = Some(Utc::now().timestamp_millis());
    next.locked_until_ms = None;
    next.lease_token = None;
    next.last_error = None;
    store::commit_claimed(config, job, next, expected, domain_changes, follow_ups)
}

pub(crate) async fn mark_done_with_changes_async(
    config: &MemoryConfig,
    job: &Job,
    follow_ups: &[NewJob],
    expected: &[StateChange],
    domain_changes: &[StateChange],
) -> Result<bool> {
    let mut next = JobRecord::from_public(job);
    next.status = JobStatus::Done.as_str().to_owned();
    next.completed_at_ms = Some(Utc::now().timestamp_millis());
    next.locked_until_ms = None;
    next.lease_token = None;
    next.last_error = None;
    store::commit_claimed_async(config, job, next, expected, domain_changes, follow_ups).await
}

pub(crate) fn mark_done_with_followups(
    config: &MemoryConfig,
    job: &Job,
    follow_ups: &[NewJob],
) -> Result<bool> {
    mark_done_with_changes(config, job, follow_ups, &[], &[])
}

/// Settle a failed job. Retryable errors use capped exponential backoff;
/// unrecoverable errors and exhausted budgets become terminal failures.
pub fn mark_failed(config: &MemoryConfig, job: &Job, error: &str) -> Result<()> {
    mark_failed_typed(config, job, error, None)
}

pub fn mark_failed_typed(
    config: &MemoryConfig,
    job: &Job,
    error: &str,
    failure: Option<&JobFailure>,
) -> Result<()> {
    let mut next = JobRecord::from_public(job);
    next.locked_until_ms = None;
    next.last_error = Some(error.to_owned());
    next.lease_token = None;
    let unrecoverable = failure.is_some_and(JobFailure::is_unrecoverable);
    if job.attempts >= job.max_attempts || unrecoverable {
        next.status = JobStatus::Failed.as_str().to_owned();
        next.completed_at_ms = Some(Utc::now().timestamp_millis());
        next.failure_reason = failure.map(|failure| failure.code.to_owned());
        next.failure_class = failure.map(|failure| failure.class.to_owned());
    } else {
        next.status = JobStatus::Ready.as_str().to_owned();
        next.available_at_ms =
            Utc::now()
                .timestamp_millis()
                .saturating_add(store::backoff_ms_with_policy(
                    job.attempts,
                    config.queue.retry_base_ms,
                    config.queue.retry_cap_ms,
                ));
        next.started_at_ms = None;
    }
    let _ = store::commit_claimed(config, job, next, &[], &[], &[])?;
    Ok(())
}

pub(crate) async fn mark_failed_typed_async(
    config: &MemoryConfig,
    job: &Job,
    error: &str,
    failure: Option<&JobFailure>,
) -> Result<()> {
    let mut next = JobRecord::from_public(job);
    next.locked_until_ms = None;
    next.last_error = Some(error.to_owned());
    next.lease_token = None;
    let unrecoverable = failure.is_some_and(JobFailure::is_unrecoverable);
    if job.attempts >= job.max_attempts || unrecoverable {
        next.status = JobStatus::Failed.as_str().to_owned();
        next.completed_at_ms = Some(Utc::now().timestamp_millis());
        next.failure_reason = failure.map(|failure| failure.code.to_owned());
        next.failure_class = failure.map(|failure| failure.class.to_owned());
    } else {
        next.status = JobStatus::Ready.as_str().to_owned();
        next.available_at_ms =
            Utc::now()
                .timestamp_millis()
                .saturating_add(store::backoff_ms_with_policy(
                    job.attempts,
                    config.queue.retry_base_ms,
                    config.queue.retry_cap_ms,
                ));
        next.started_at_ms = None;
    }
    let _ = store::commit_claimed_async(config, job, next, &[], &[], &[]).await?;
    Ok(())
}

/// Defer without burning the failure budget. The updated due time is encoded
/// in a new queue hint after the current lease is acknowledged.
pub fn mark_deferred(config: &MemoryConfig, job: &Job, until_ms: i64, reason: &str) -> Result<()> {
    mark_deferred_with_changes(config, job, until_ms, reason, &[], &[]).map(|_| ())
}

pub(crate) async fn mark_deferred_async(
    config: &MemoryConfig,
    job: &Job,
    until_ms: i64,
    reason: &str,
) -> Result<()> {
    mark_deferred_with_changes_async(config, job, until_ms, reason, &[], &[])
        .await
        .map(|_| ())
}

pub(crate) fn mark_deferred_with_changes(
    config: &MemoryConfig,
    job: &Job,
    until_ms: i64,
    reason: &str,
    expected: &[StateChange],
    domain_changes: &[StateChange],
) -> Result<bool> {
    let now = Utc::now().timestamp_millis();
    let cutoff = now.saturating_sub(config.queue.max_defer_age_ms);
    let mut next = JobRecord::from_public(job);
    next.locked_until_ms = None;
    next.started_at_ms = None;
    next.lease_token = None;
    next.attempts = job.attempts.saturating_sub(1);
    next.available_at_ms = until_ms;
    next.last_error = Some(reason.to_owned());
    if job.created_at_ms <= cutoff {
        next.status = JobStatus::Failed.as_str().to_owned();
        next.completed_at_ms = Some(now);
        next.failure_reason = Some("defer_age_exceeded".to_owned());
        next.failure_class = Some("unrecoverable".to_owned());
    } else {
        next.status = JobStatus::Ready.as_str().to_owned();
        next.completed_at_ms = None;
        next.failure_reason = None;
        next.failure_class = None;
    }
    store::commit_claimed(config, job, next, expected, domain_changes, &[])
}

pub(crate) async fn mark_deferred_with_changes_async(
    config: &MemoryConfig,
    job: &Job,
    until_ms: i64,
    reason: &str,
    expected: &[StateChange],
    domain_changes: &[StateChange],
) -> Result<bool> {
    let now = Utc::now().timestamp_millis();
    let cutoff = now.saturating_sub(config.queue.max_defer_age_ms);
    let mut next = JobRecord::from_public(job);
    next.locked_until_ms = None;
    next.started_at_ms = None;
    next.lease_token = None;
    next.attempts = job.attempts.saturating_sub(1);
    next.available_at_ms = until_ms;
    next.last_error = Some(reason.to_owned());
    if job.created_at_ms <= cutoff {
        next.status = JobStatus::Failed.as_str().to_owned();
        next.completed_at_ms = Some(now);
        next.failure_reason = Some("defer_age_exceeded".to_owned());
        next.failure_class = Some("unrecoverable".to_owned());
    } else {
        next.status = JobStatus::Ready.as_str().to_owned();
        next.completed_at_ms = None;
        next.failure_reason = None;
        next.failure_class = None;
    }
    store::commit_claimed_async(config, job, next, expected, domain_changes, &[]).await
}

/// Expired QueueStore visibility leases make the message redeliverable. This
/// helper also clears stale durable lease markers when a host scheduler runs.
pub fn recover_stale_locks(config: &MemoryConfig) -> Result<usize> {
    let now = Utc::now().timestamp_millis();
    let jobs = store::all_job_records(config)?;
    let mut recovered = 0;
    for (old_value, mut record) in jobs {
        if record.status != JobStatus::Running.as_str()
            || record.locked_until_ms.is_none_or(|lease| lease >= now)
        {
            continue;
        }
        record.status = JobStatus::Ready.as_str().to_owned();
        record.hint_generation = record.hint_generation.saturating_add(1);
        record.locked_until_ms = None;
        record.lease_token = None;
        record
            .last_error
            .get_or_insert_with(|| "recovered_from_stale_lock".to_owned());
        if store::replace_record(config, &old_value, &record)? {
            store::publish_record(config, &record)?;
            recovered += 1;
        }
    }
    Ok(recovered)
}

#[cfg(feature = "tokio")]
pub(crate) async fn recover_stale_locks_async(config: &MemoryConfig) -> Result<usize> {
    let now = Utc::now().timestamp_millis();
    let jobs = store::all_job_records_async(config).await?;
    let mut recovered = 0;
    for (old_value, mut record) in jobs {
        if record.status != JobStatus::Running.as_str()
            || record.locked_until_ms.is_none_or(|lease| lease >= now)
        {
            continue;
        }
        record.status = JobStatus::Ready.as_str().to_owned();
        record.hint_generation = record.hint_generation.saturating_add(1);
        record.locked_until_ms = None;
        record.lease_token = None;
        record
            .last_error
            .get_or_insert_with(|| "recovered_from_stale_lock".to_owned());
        if versioned::commit_if_state_matches_async(
            config,
            &[StateChange {
                collection: store::JOBS.to_owned(),
                key: store::job_key(&record.id),
                value: Some(old_value),
            }],
            &[],
            &[store::state_change(&record)?],
        )
        .await?
        {
            store::publish_record_async(config, &record).await?;
            recovered += 1;
        }
    }
    Ok(recovered)
}

/// Release running jobs on graceful shutdown and restore the claim attempt
/// bump. Compare-and-swap clears each lease so an old worker cannot settle it.
pub fn release_running_locks(config: &MemoryConfig) -> Result<usize> {
    let jobs = store::all_job_records(config)?;
    let mut released = 0;
    for (old_value, mut record) in jobs {
        if record.status != JobStatus::Running.as_str() {
            continue;
        }
        record.status = JobStatus::Ready.as_str().to_owned();
        record.hint_generation = record.hint_generation.saturating_add(1);
        record.attempts = record.attempts.saturating_sub(1);
        record.locked_until_ms = None;
        record.started_at_ms = None;
        record.lease_token = None;
        if store::replace_record(config, &old_value, &record)? {
            store::publish_record(config, &record)?;
            released += 1;
        }
    }
    Ok(released)
}

/// Requeue every terminal failed row with a fresh retry budget.
pub fn requeue_failed(config: &MemoryConfig) -> Result<u64> {
    requeue(config, false)
}

/// Requeue only transient or legacy unclassified failures.
pub fn requeue_transient_failed(config: &MemoryConfig) -> Result<u64> {
    requeue(config, true)
}

pub fn retry_all_failed(config: &MemoryConfig) -> Result<u64> {
    requeue_failed(config)
}

#[cfg(any(feature = "tokio", test))]
pub(crate) async fn requeue_transient_failed_async(config: &MemoryConfig) -> Result<u64> {
    let jobs = store::all_job_records_async(config).await?;
    let active_keys: std::collections::HashSet<String> = jobs
        .iter()
        .filter_map(|(_, job)| {
            matches!(job.status.as_str(), "ready" | "running")
                .then(|| job.dedupe_key.clone())
                .flatten()
        })
        .collect();
    let mut candidates: Vec<_> = jobs
        .into_iter()
        .filter(|(_, job)| {
            job.status == JobStatus::Failed.as_str()
                && job.failure_class.as_deref() != Some("unrecoverable")
        })
        .collect();
    candidates.sort_by(|(_, a), (_, b)| {
        b.completed_at_ms
            .unwrap_or(b.created_at_ms)
            .cmp(&a.completed_at_ms.unwrap_or(a.created_at_ms))
            .then_with(|| b.created_at_ms.cmp(&a.created_at_ms))
            .then_with(|| b.id.cmp(&a.id))
    });
    let now = Utc::now().timestamp_millis();
    let mut seen = std::collections::HashSet::new();
    let mut expected = Vec::new();
    let mut changes = Vec::new();
    let mut publish = Vec::new();
    for (old_value, mut job) in candidates {
        let Some(dedupe) = job.dedupe_key.clone() else {
            reset_for_retry(&mut job, now);
            expected.push(StateChange {
                collection: store::JOBS.to_owned(),
                key: store::job_key(&job.id),
                value: Some(old_value),
            });
            changes.push(store::state_change(&job)?);
            publish.push(job);
            continue;
        };
        if active_keys.contains(&dedupe) {
            continue;
        }
        if !seen.insert(dedupe.clone()) {
            job.status = JobStatus::Cancelled.as_str().to_owned();
            job.completed_at_ms = Some(now);
            job.locked_until_ms = None;
            job.last_error = Some("superseded by a newer job with the same dedupe_key".to_owned());
            expected.push(StateChange {
                collection: store::JOBS.to_owned(),
                key: store::job_key(&job.id),
                value: Some(old_value),
            });
            changes.push(store::state_change(&job)?);
            continue;
        }
        reset_for_retry(&mut job, now);
        let dedupe_key = store::dedupe_state_key(&dedupe);
        let current = versioned::read_state_async(config, store::JOBS, &dedupe_key).await?;
        expected.push(StateChange {
            collection: store::JOBS.to_owned(),
            key: dedupe_key.clone(),
            value: current,
        });
        changes.push(StateChange {
            collection: store::JOBS.to_owned(),
            key: dedupe_key,
            value: Some(serde_json::Value::String(job.id.clone())),
        });
        expected.push(StateChange {
            collection: store::JOBS.to_owned(),
            key: store::job_key(&job.id),
            value: Some(old_value),
        });
        changes.push(store::state_change(&job)?);
        publish.push(job);
    }
    if changes.is_empty() {
        return Ok(0);
    }
    if !versioned::commit_if_state_matches_async(config, &expected, &[], &changes).await? {
        return Ok(0);
    }
    let count = publish.len() as u64;
    for job in publish {
        store::publish_record_async(config, &job).await?;
    }
    Ok(count)
}

fn requeue(config: &MemoryConfig, transient_only: bool) -> Result<u64> {
    let jobs = store::all_job_records(config)?;
    let active_keys: std::collections::HashSet<String> = jobs
        .iter()
        .filter_map(|(_, job)| {
            matches!(job.status.as_str(), "ready" | "running")
                .then(|| job.dedupe_key.clone())
                .flatten()
        })
        .collect();
    let mut candidates: Vec<_> = jobs
        .into_iter()
        .filter(|(_, job)| {
            job.status == JobStatus::Failed.as_str()
                && (!transient_only || job.failure_class.as_deref() != Some("unrecoverable"))
        })
        .collect();
    candidates.sort_by(|(_, a), (_, b)| {
        b.completed_at_ms
            .unwrap_or(b.created_at_ms)
            .cmp(&a.completed_at_ms.unwrap_or(a.created_at_ms))
            .then_with(|| b.created_at_ms.cmp(&a.created_at_ms))
            .then_with(|| b.id.cmp(&a.id))
    });
    let now = Utc::now().timestamp_millis();
    let mut seen = std::collections::HashSet::new();
    let mut expected = Vec::new();
    let mut changes = Vec::new();
    let mut publish = Vec::new();
    for (old_value, mut job) in candidates {
        let Some(dedupe) = job.dedupe_key.clone() else {
            reset_for_retry(&mut job, now);
            expected.push(StateChange {
                collection: super::store::JOBS.to_owned(),
                key: super::store::job_key(&job.id),
                value: Some(old_value),
            });
            changes.push(super::store::state_change(&job)?);
            publish.push(job);
            continue;
        };
        if active_keys.contains(&dedupe) {
            continue;
        }
        if !seen.insert(dedupe.clone()) {
            job.status = JobStatus::Cancelled.as_str().to_owned();
            job.completed_at_ms = Some(now);
            job.locked_until_ms = None;
            job.last_error = Some("superseded by a newer job with the same dedupe_key".to_owned());
            expected.push(StateChange {
                collection: super::store::JOBS.to_owned(),
                key: super::store::job_key(&job.id),
                value: Some(old_value),
            });
            changes.push(super::store::state_change(&job)?);
            continue;
        }
        reset_for_retry(&mut job, now);
        let dedupe_state_key = super::store::dedupe_state_key(&dedupe);
        let current = versioned::read_state(config, super::store::JOBS, &dedupe_state_key)?;
        expected.push(StateChange {
            collection: super::store::JOBS.to_owned(),
            key: dedupe_state_key.clone(),
            value: current,
        });
        changes.push(StateChange {
            collection: super::store::JOBS.to_owned(),
            key: dedupe_state_key,
            value: Some(serde_json::Value::String(job.id.clone())),
        });
        expected.push(StateChange {
            collection: super::store::JOBS.to_owned(),
            key: super::store::job_key(&job.id),
            value: Some(old_value),
        });
        changes.push(super::store::state_change(&job)?);
        publish.push(job);
    }
    if changes.is_empty() {
        return Ok(0);
    }
    if !versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
        return Ok(0);
    }
    let count = publish.len() as u64;
    for job in publish {
        store::publish_record(config, &job)?;
    }
    Ok(count)
}

fn reset_for_retry(job: &mut JobRecord, now: i64) {
    job.status = JobStatus::Ready.as_str().to_owned();
    job.hint_generation = job.hint_generation.saturating_add(1);
    job.attempts = 0;
    job.available_at_ms = now;
    job.locked_until_ms = None;
    job.started_at_ms = None;
    job.completed_at_ms = None;
    job.last_error = None;
    job.failure_reason = None;
    job.failure_class = None;
    job.lease_token = None;
}

#[cfg(test)]
#[path = "store_settle_tests.rs"]
mod tests;
