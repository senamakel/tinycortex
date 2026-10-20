//! Durable job metadata over the owner-scoped versioned state store.
//!
//! Queue messages are deliberately only wake-up hints. The versioned state
//! head is authoritative for lifecycle, dedupe, attempts and lease generation;
//! every transition is a head-CAS. This keeps correctness independent of a
//! backend's optional queue dedupe support and fences stale workers.

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tinystoragedrivers_types::{NewMessage, Retry};
use uuid::Uuid;

use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::queue::types::{Job, JobKind, JobStatus, NewJob, RETIRED_JOB_KINDS};

pub(crate) const JOBS: &str = "memory_tree_jobs";
pub(crate) const JOB_PREFIX: &str = "job:";
const DEDUPE_PREFIX: &str = "dedupe:";
const PUBLISH_RETRIES: usize = 8;
const MAX_STALE_HINTS: usize = 64;

/// Default visibility lock — a crashed worker is redelivered after this window.
pub const DEFAULT_LOCK_DURATION_MS: i64 = 5 * 60 * 1_000;
/// Backoff math for retry. Returns `now + min(base * 2^attempts, cap)`.
#[cfg(test)]
pub(crate) const RETRY_BASE_MS: i64 = 60 * 1_000;
#[cfg(test)]
pub(crate) const RETRY_CAP_MS: i64 = 60 * 60 * 1_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct JobRecord {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) payload_json: String,
    pub(crate) dedupe_key: Option<String>,
    pub(crate) status: String,
    pub(crate) attempts: u32,
    pub(crate) max_attempts: u32,
    pub(crate) available_at_ms: i64,
    pub(crate) locked_until_ms: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) failure_reason: Option<String>,
    pub(crate) failure_class: Option<String>,
    pub(crate) created_at_ms: i64,
    pub(crate) started_at_ms: Option<i64>,
    pub(crate) completed_at_ms: Option<i64>,
    /// Identity of the current Ready-state queue publication. This is
    /// separate from `lease_epoch`, which changes on every worker claim.
    #[serde(default)]
    pub(crate) hint_generation: u64,
    #[serde(default)]
    pub(crate) lease_epoch: u64,
    #[serde(default)]
    pub(crate) lease_token: Option<String>,
}

impl JobRecord {
    pub(crate) fn from_public(job: &Job) -> Self {
        Self {
            id: job.id.clone(),
            kind: job.kind.as_str().to_owned(),
            payload_json: job.payload_json.clone(),
            dedupe_key: job.dedupe_key.clone(),
            status: job.status.as_str().to_owned(),
            attempts: job.attempts,
            max_attempts: job.max_attempts,
            available_at_ms: job.available_at_ms,
            locked_until_ms: job.locked_until_ms,
            last_error: job.last_error.clone(),
            failure_reason: job.failure_reason.clone(),
            failure_class: job.failure_class.clone(),
            created_at_ms: job.created_at_ms,
            started_at_ms: job.started_at_ms,
            completed_at_ms: job.completed_at_ms,
            hint_generation: 0,
            lease_epoch: job.lease_epoch,
            lease_token: job
                .queue_receipt
                .as_ref()
                .map(|receipt| receipt.token.clone()),
        }
    }
    fn public(&self, receipt: Option<tinystoragedrivers_types::Receipt>) -> Result<Job> {
        Ok(Job {
            id: self.id.clone(),
            kind: JobKind::parse(&self.kind)?,
            payload_json: self.payload_json.clone(),
            dedupe_key: self.dedupe_key.clone(),
            status: JobStatus::parse(&self.status)?,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            available_at_ms: self.available_at_ms,
            locked_until_ms: self.locked_until_ms,
            last_error: self.last_error.clone(),
            failure_reason: self.failure_reason.clone(),
            failure_class: self.failure_class.clone(),
            created_at_ms: self.created_at_ms,
            started_at_ms: self.started_at_ms,
            completed_at_ms: self.completed_at_ms,
            lease_epoch: self.lease_epoch,
            queue_receipt: receipt,
        })
    }

    fn status(&self) -> Result<JobStatus> {
        JobStatus::parse(&self.status)
    }
}

pub(crate) fn job_key(id: &str) -> String {
    format!("{JOB_PREFIX}{id}")
}

pub(crate) fn dedupe_state_key(key: &str) -> String {
    let digest = sha256_hex(key.as_bytes());
    format!("{DEDUPE_PREFIX}{digest}")
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_job(value: &Value) -> Result<JobRecord> {
    serde_json::from_value(value.clone()).context("decode durable queue job")
}

fn encode_job(job: &JobRecord) -> Result<Value> {
    Ok(serde_json::to_value(job)?)
}

pub(crate) fn state_change(job: &JobRecord) -> Result<StateChange> {
    Ok(StateChange {
        collection: JOBS.to_owned(),
        key: job_key(&job.id),
        value: Some(encode_job(job)?),
    })
}

pub(crate) fn read_record(config: &MemoryConfig, id: &str) -> Result<Option<(Value, JobRecord)>> {
    let key = job_key(id);
    let Some(value) = versioned::read_state(config, JOBS, &key)? else {
        return Ok(None);
    };
    Ok(Some((value.clone(), decode_job(&value)?)))
}

pub(crate) async fn read_record_async(
    config: &MemoryConfig,
    id: &str,
) -> Result<Option<(Value, JobRecord)>> {
    let key = job_key(id);
    let Some(value) = versioned::read_state_async(config, JOBS, &key).await? else {
        return Ok(None);
    };
    Ok(Some((value.clone(), decode_job(&value)?)))
}

pub(crate) fn all_job_records(config: &MemoryConfig) -> Result<Vec<(Value, JobRecord)>> {
    versioned::list_state(config, JOBS)?
        .into_iter()
        .filter(|(key, _)| key.starts_with(JOB_PREFIX))
        .map(|(_, value)| Ok((value.clone(), decode_job(&value)?)))
        .collect()
}

pub(crate) async fn all_job_records_async(
    config: &MemoryConfig,
) -> Result<Vec<(Value, JobRecord)>> {
    versioned::list_state_async(config, JOBS)
        .await?
        .into_iter()
        .filter(|(key, _)| key.starts_with(JOB_PREFIX))
        .map(|(_, value)| Ok((value.clone(), decode_job(&value)?)))
        .collect()
}

pub(crate) fn replace_record(
    config: &MemoryConfig,
    old_value: &Value,
    next: &JobRecord,
) -> Result<bool> {
    versioned::commit_if_state_matches(
        config,
        &[StateChange {
            collection: JOBS.to_owned(),
            key: job_key(&next.id),
            value: Some(old_value.clone()),
        }],
        &[],
        &[state_change(next)?],
    )
}

pub(crate) fn publish_record(config: &MemoryConfig, record: &JobRecord) -> Result<()> {
    enqueue_hint(config, record)
}

pub(crate) async fn publish_record_async(config: &MemoryConfig, record: &JobRecord) -> Result<()> {
    enqueue_hint_async(config, record).await
}

fn hint_message(record: &JobRecord, now_ms: i64) -> NewMessage {
    let delay_ms = record.available_at_ms.saturating_sub(now_ms).max(0) as u64;
    NewMessage::new(json!({"job_id": record.id, "generation": record.hint_generation}))
        .priority(queue_priority(&record.kind))
        .delay_ms(delay_ms)
        // This is only a finite-window optimization. Duplicate hints are
        // harmless because the durable hint generation is authoritative.
        // Redelivery keeps the same key, while each new ready-state
        // generation gets a fresh key even when the backend retains finite-
        // window dedupe entries for acknowledged hints.
        .dedupe_key(format!("job:{}:{}", record.id, record.hint_generation))
}

fn queue_priority(kind: &str) -> u8 {
    match kind {
        "seal" => 9,
        "reembed_backfill" => 8,
        "flush_stale" => 7,
        "append_buffer" => 6,
        _ => 5,
    }
}

fn enqueue_hint(config: &MemoryConfig, record: &JobRecord) -> Result<()> {
    let queues = config.storage()?.queues().clone();
    let message = hint_message(record, Utc::now().timestamp_millis());
    config
        .storage_blocking()?
        .run(async move {
            queues
                .enqueue(crate::memory::queue::QUEUE_NAME, &[message])
                .await
        })?
        .map_err(anyhow::Error::from)?;
    Ok(())
}

async fn enqueue_hint_async(config: &MemoryConfig, record: &JobRecord) -> Result<()> {
    let queues = config.storage()?.queues().clone();
    let message = hint_message(record, Utc::now().timestamp_millis());
    config
        .storage_blocking()?
        .run_async(async move {
            queues
                .enqueue(crate::memory::queue::QUEUE_NAME, &[message])
                .await
        })
        .await?
        .map_err(anyhow::Error::from)?;
    Ok(())
}

fn record_from_new(config: &MemoryConfig, job: &NewJob, id: String) -> JobRecord {
    let now = Utc::now().timestamp_millis();
    JobRecord {
        id,
        kind: job.kind.as_str().to_owned(),
        payload_json: job.payload_json.clone(),
        dedupe_key: job.dedupe_key.clone(),
        status: JobStatus::Ready.as_str().to_owned(),
        attempts: 0,
        max_attempts: job.max_attempts.unwrap_or(config.queue.max_attempts.max(1)),
        available_at_ms: job.available_at_ms.unwrap_or(now),
        locked_until_ms: None,
        last_error: None,
        failure_reason: None,
        failure_class: None,
        created_at_ms: now,
        started_at_ms: None,
        completed_at_ms: None,
        hint_generation: 0,
        lease_epoch: 0,
        lease_token: None,
    }
}

/// Enqueue one durable job. An optional dedupe key suppresses another active
/// job with the same key. A message is published only after the owner-head CAS;
/// bootstrap repairs the crash window between that CAS and queue publication.
pub fn enqueue(config: &MemoryConfig, job: &NewJob) -> Result<Option<String>> {
    let id = job
        .id
        .clone()
        .unwrap_or_else(|| format!("job:{}", Uuid::new_v4()));
    let record = record_from_new(config, job, id.clone());
    let key = job_key(&id);

    for _ in 0..PUBLISH_RETRIES {
        let existing = versioned::read_state(config, JOBS, &key)?;
        if let Some(value) = &existing {
            let current = decode_job(value)?;
            if job.id.is_some()
                && (current.kind != record.kind
                    || current.payload_json != record.payload_json
                    || current.dedupe_key != record.dedupe_key
                    || current.max_attempts != record.max_attempts)
            {
                anyhow::bail!("stable queue job ID was reused with a different intent");
            }
            if current.status()? == JobStatus::Ready {
                enqueue_hint(config, &current)?;
            }
            // Stable outbox IDs suppress a retry even after completion.
            if job.id.is_some() {
                return Ok(None);
            }
        }

        let mut expected = vec![StateChange {
            collection: JOBS.to_owned(),
            key: key.clone(),
            value: existing,
        }];
        let mut changes = vec![state_change(&record)?];
        if let Some(dedupe) = &job.dedupe_key {
            let dkey = dedupe_state_key(dedupe);
            let current_id = versioned::read_state(config, JOBS, &dkey)?;
            if let Some(value) = &current_id {
                if let Some(active_id) = value.as_str() {
                    if let Some((_, active)) = read_record(config, active_id)? {
                        if matches!(active.status()?, JobStatus::Ready | JobStatus::Running) {
                            enqueue_hint(config, &active)?;
                            return Ok(None);
                        }
                    }
                }
            }
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey.clone(),
                value: current_id,
            });
            changes.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey,
                value: Some(Value::String(id.clone())),
            });
        }
        if versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
            enqueue_hint(config, &record)?;
            return Ok(Some(id));
        }
    }
    anyhow::bail!("queue enqueue compare-and-swap did not succeed within retry budget")
}

pub(crate) async fn enqueue_async(config: &MemoryConfig, job: &NewJob) -> Result<Option<String>> {
    let id = job
        .id
        .clone()
        .unwrap_or_else(|| format!("job:{}", Uuid::new_v4()));
    let record = record_from_new(config, job, id.clone());
    let key = job_key(&id);

    for _ in 0..PUBLISH_RETRIES {
        let existing = versioned::read_state_async(config, JOBS, &key).await?;
        if let Some(value) = &existing {
            let current = decode_job(value)?;
            if job.id.is_some()
                && (current.kind != record.kind
                    || current.payload_json != record.payload_json
                    || current.dedupe_key != record.dedupe_key
                    || current.max_attempts != record.max_attempts)
            {
                anyhow::bail!("stable queue job ID was reused with a different intent");
            }
            if current.status()? == JobStatus::Ready {
                enqueue_hint_async(config, &current).await?;
            }
            if job.id.is_some() {
                return Ok(None);
            }
        }

        let mut expected = vec![StateChange {
            collection: JOBS.to_owned(),
            key: key.clone(),
            value: existing,
        }];
        let mut changes = vec![state_change(&record)?];
        if let Some(dedupe) = &job.dedupe_key {
            let dkey = dedupe_state_key(dedupe);
            let current_id = versioned::read_state_async(config, JOBS, &dkey).await?;
            if let Some(active_id) = current_id.as_ref().and_then(Value::as_str) {
                if let Some((_, active)) = read_record_async(config, active_id).await? {
                    if matches!(active.status()?, JobStatus::Ready | JobStatus::Running) {
                        enqueue_hint_async(config, &active).await?;
                        return Ok(None);
                    }
                }
            }
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey.clone(),
                value: current_id,
            });
            changes.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey,
                value: Some(Value::String(id.clone())),
            });
        }
        if versioned::commit_if_state_matches_async(config, &expected, &[], &changes).await? {
            enqueue_hint_async(config, &record).await?;
            return Ok(Some(id));
        }
    }
    anyhow::bail!("queue enqueue compare-and-swap did not succeed within retry budget")
}

/// Claim the next due job by receiving one queue hint and CASing its durable
/// lease generation. Stale and duplicate hints are acknowledged and skipped.
pub fn claim_next(config: &MemoryConfig, lock_duration_ms: i64) -> Result<Option<Job>> {
    let queues = config.storage()?.queues().clone();
    let consumer = format!("tinycortex:{}", Uuid::new_v4());
    let visibility = lock_duration_ms.max(1) as u64;
    let mut repaired = false;
    for _ in 0..MAX_STALE_HINTS {
        let deliveries = config
            .storage_blocking()?
            .run({
                let queues = queues.clone();
                let consumer = consumer.clone();
                async move {
                    queues
                        .receive(crate::memory::queue::QUEUE_NAME, 1, visibility, &consumer)
                        .await
                }
            })?
            .map_err(anyhow::Error::from)?;
        let Some(delivery) = deliveries.into_iter().next() else {
            if !repaired {
                repaired = true;
                if repair_pending_hints(config)? > 0 {
                    continue;
                }
            }
            return Ok(None);
        };
        let Some(id) = delivery.payload.get("job_id").and_then(Value::as_str) else {
            ack(config, &delivery.receipt)?;
            continue;
        };
        let Some((old_value, mut record)) = read_record(config, id)? else {
            ack(config, &delivery.receipt)?;
            continue;
        };
        if is_retired_kind(&record.kind) {
            ack(config, &delivery.receipt)?;
            continue;
        }
        if delivery
            .payload
            .get("generation")
            .and_then(Value::as_u64)
            .is_some_and(|generation| generation != record.hint_generation)
        {
            ack(config, &delivery.receipt)?;
            continue;
        }
        let status = record.status()?;
        if !matches!(status, JobStatus::Ready | JobStatus::Running) {
            ack(config, &delivery.receipt)?;
            continue;
        }
        let now_ms = Utc::now().timestamp_millis();
        if status == JobStatus::Running
            && record
                .locked_until_ms
                .is_some_and(|lease_until| lease_until > now_ms)
        {
            // An optional-dedupe backend may deliver duplicate hints while a
            // separate valid receipt still owns this job. Do not steal it.
            ack(config, &delivery.receipt)?;
            continue;
        }
        if record.available_at_ms > now_ms {
            nack(
                config,
                &delivery.receipt,
                (record.available_at_ms - now_ms) as u64,
            )?;
            continue;
        }
        record.status = JobStatus::Running.as_str().to_owned();
        record.attempts = record.attempts.saturating_add(1);
        record.started_at_ms = Some(now_ms);
        record.locked_until_ms = Some(delivery.lease_until_ms);
        record.lease_epoch = record.lease_epoch.saturating_add(1);
        record.lease_token = Some(delivery.receipt.token.clone());
        let expected = [StateChange {
            collection: JOBS.to_owned(),
            key: job_key(id),
            value: Some(old_value),
        }];
        if versioned::commit_if_state_matches(config, &expected, &[], &[state_change(&record)?])? {
            return Ok(Some(record.public(Some(delivery.receipt))?));
        }
        // Another receiver changed the durable generation after this delivery
        // was issued. Release the stale hint and continue looking.
        ack(config, &delivery.receipt)?;
    }
    Ok(None)
}

pub(crate) async fn claim_next_async(
    config: &MemoryConfig,
    lock_duration_ms: i64,
) -> Result<Option<Job>> {
    let queues = config.storage()?.queues().clone();
    let consumer = format!("tinycortex:{}", Uuid::new_v4());
    let visibility = lock_duration_ms.max(1) as u64;
    let mut repaired = false;
    for _ in 0..MAX_STALE_HINTS {
        let deliveries = config
            .storage_blocking()?
            .run_async({
                let queues = queues.clone();
                let consumer = consumer.clone();
                async move {
                    queues
                        .receive(crate::memory::queue::QUEUE_NAME, 1, visibility, &consumer)
                        .await
                }
            })
            .await?
            .map_err(anyhow::Error::from)?;
        let Some(delivery) = deliveries.into_iter().next() else {
            if !repaired {
                repaired = true;
                if repair_pending_hints_async(config).await? > 0 {
                    continue;
                }
            }
            return Ok(None);
        };
        let Some(id) = delivery.payload.get("job_id").and_then(Value::as_str) else {
            ack_async(config, &delivery.receipt).await?;
            continue;
        };
        let Some((old_value, mut record)) = read_record_async(config, id).await? else {
            ack_async(config, &delivery.receipt).await?;
            continue;
        };
        if is_retired_kind(&record.kind) {
            ack_async(config, &delivery.receipt).await?;
            continue;
        }
        if delivery
            .payload
            .get("generation")
            .and_then(Value::as_u64)
            .is_some_and(|generation| generation != record.hint_generation)
        {
            ack_async(config, &delivery.receipt).await?;
            continue;
        }
        let status = record.status()?;
        if !matches!(status, JobStatus::Ready | JobStatus::Running) {
            ack_async(config, &delivery.receipt).await?;
            continue;
        }
        let now_ms = Utc::now().timestamp_millis();
        if status == JobStatus::Running
            && record
                .locked_until_ms
                .is_some_and(|lease_until| lease_until > now_ms)
        {
            ack_async(config, &delivery.receipt).await?;
            continue;
        }
        if record.available_at_ms > now_ms {
            nack_async(
                config,
                &delivery.receipt,
                (record.available_at_ms - now_ms) as u64,
            )
            .await?;
            continue;
        }
        record.status = JobStatus::Running.as_str().to_owned();
        record.attempts = record.attempts.saturating_add(1);
        record.started_at_ms = Some(now_ms);
        record.locked_until_ms = Some(delivery.lease_until_ms);
        record.lease_epoch = record.lease_epoch.saturating_add(1);
        record.lease_token = Some(delivery.receipt.token.clone());
        let expected = [StateChange {
            collection: JOBS.to_owned(),
            key: job_key(id),
            value: Some(old_value),
        }];
        if versioned::commit_if_state_matches_async(
            config,
            &expected,
            &[],
            &[state_change(&record)?],
        )
        .await?
        {
            return Ok(Some(record.public(Some(delivery.receipt))?));
        }
        ack_async(config, &delivery.receipt).await?;
    }
    Ok(None)
}

fn ack(config: &MemoryConfig, receipt: &tinystoragedrivers_types::Receipt) -> Result<bool> {
    let queues = config.storage()?.queues().clone();
    let receipt = receipt.clone();
    config
        .storage_blocking()?
        .run(async move { queues.ack(crate::memory::queue::QUEUE_NAME, &receipt).await })?
        .map_err(anyhow::Error::from)
}

async fn ack_async(
    config: &MemoryConfig,
    receipt: &tinystoragedrivers_types::Receipt,
) -> Result<bool> {
    let queues = config.storage()?.queues().clone();
    let receipt = receipt.clone();
    config
        .storage_blocking()?
        .run_async(async move { queues.ack(crate::memory::queue::QUEUE_NAME, &receipt).await })
        .await?
        .map_err(anyhow::Error::from)
}

fn nack(
    config: &MemoryConfig,
    receipt: &tinystoragedrivers_types::Receipt,
    delay_ms: u64,
) -> Result<bool> {
    let queues = config.storage()?.queues().clone();
    let receipt = receipt.clone();
    config
        .storage_blocking()?
        .run(async move {
            queues
                .nack(
                    crate::memory::queue::QUEUE_NAME,
                    &receipt,
                    Retry::After(delay_ms.max(1)),
                )
                .await
        })?
        .map_err(anyhow::Error::from)
}

async fn nack_async(
    config: &MemoryConfig,
    receipt: &tinystoragedrivers_types::Receipt,
    delay_ms: u64,
) -> Result<bool> {
    let queues = config.storage()?.queues().clone();
    let receipt = receipt.clone();
    config
        .storage_blocking()?
        .run_async(async move {
            queues
                .nack(
                    crate::memory::queue::QUEUE_NAME,
                    &receipt,
                    Retry::After(delay_ms.max(1)),
                )
                .await
        })
        .await?
        .map_err(anyhow::Error::from)
}

/// Commit a claimed job transition, domain mutations and follow-up rows under
/// one versioned-head CAS. Follow-up queue hints are published afterwards and
/// are repaired from durable ready rows on bootstrap if publication fails.
pub(crate) fn commit_claimed(
    config: &MemoryConfig,
    claimed: &Job,
    mut next: JobRecord,
    domain_expected: &[StateChange],
    domain_changes: &[StateChange],
    follow_ups: &[NewJob],
) -> Result<bool> {
    let Some((old_value, current)) = read_record(config, &claimed.id)? else {
        return Ok(false);
    };
    let now_ms = Utc::now().timestamp_millis();
    if current.status()? != JobStatus::Running
        || current.lease_epoch != claimed.lease_epoch
        || current.lease_token.as_deref()
            != claimed
                .queue_receipt
                .as_ref()
                .map(|receipt| receipt.token.as_str())
        || current
            .locked_until_ms
            .is_none_or(|lease_until| lease_until <= now_ms)
    {
        return Ok(false);
    }
    next.lease_epoch = current.lease_epoch;
    next.hint_generation = current.hint_generation;
    if next.status == JobStatus::Ready.as_str() {
        next.hint_generation = next.hint_generation.saturating_add(1);
    }
    next.lease_token = None;
    let mut expected = vec![StateChange {
        collection: JOBS.to_owned(),
        key: job_key(&claimed.id),
        value: Some(old_value),
    }];
    expected.extend_from_slice(domain_expected);
    let mut changes = vec![state_change(&next)?];
    let mut child_records = Vec::with_capacity(follow_ups.len());
    for (index, follow_up) in follow_ups.iter().enumerate() {
        let id = follow_up.id.clone().unwrap_or_else(|| {
            format!(
                "job:followup:{}:{}:{index}",
                claimed.id, claimed.lease_epoch
            )
        });
        let child = record_from_new(config, follow_up, id.clone());
        let key = job_key(&id);
        let existing = versioned::read_state(config, JOBS, &key)?;
        if let Some(existing_value) = existing {
            // Follow-up IDs are stable outbox identities; an earlier commit
            // already made this intent durable. Fence that observation so a
            // concurrent delete cannot make this parent commit lose its only
            // copy of the intent.
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key,
                value: Some(existing_value),
            });
            continue;
        }
        if let Some(dedupe) = &follow_up.dedupe_key {
            let dkey = dedupe_state_key(dedupe);
            let current_id = versioned::read_state(config, JOBS, &dkey)?;
            if let Some(value) = &current_id {
                if let Some(active_id) = value.as_str() {
                    if let Some((active_value, active)) = read_record(config, active_id)? {
                        if matches!(active.status()?, JobStatus::Ready | JobStatus::Running) {
                            // Preserve the queue's active-dedupe contract, but
                            // fence both observations so a concurrent terminal
                            // transition cannot erase the only follow-up intent.
                            expected.push(StateChange {
                                collection: JOBS.to_owned(),
                                key: dkey,
                                value: current_id.clone(),
                            });
                            expected.push(StateChange {
                                collection: JOBS.to_owned(),
                                key: job_key(active_id),
                                value: Some(active_value),
                            });
                            continue;
                        }
                    }
                }
            }
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey.clone(),
                value: current_id,
            });
            changes.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey,
                value: Some(Value::String(id)),
            });
        }
        expected.push(StateChange {
            collection: JOBS.to_owned(),
            key,
            value: None,
        });
        changes.push(state_change(&child)?);
        child_records.push(child);
    }
    if next.status != JobStatus::Ready.as_str() && next.status != JobStatus::Running.as_str() {
        if let Some(dedupe) = &current.dedupe_key {
            let key = dedupe_state_key(dedupe);
            let mapped = versioned::read_state(config, JOBS, &key)?;
            if mapped.as_ref().and_then(Value::as_str) == Some(claimed.id.as_str()) {
                expected.push(StateChange {
                    collection: JOBS.to_owned(),
                    key: key.clone(),
                    value: mapped,
                });
                changes.push(StateChange {
                    collection: JOBS.to_owned(),
                    key,
                    value: None,
                });
            }
        }
    }
    changes.extend_from_slice(domain_changes);
    if !versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
        return Ok(false);
    }
    if let Some(receipt) = &claimed.queue_receipt {
        ack(config, receipt)?;
    }
    if next.status == JobStatus::Ready.as_str() {
        enqueue_hint(config, &next)?;
    }
    for child in &child_records {
        enqueue_hint(config, child)?;
    }
    Ok(true)
}

pub(crate) async fn commit_claimed_async(
    config: &MemoryConfig,
    claimed: &Job,
    mut next: JobRecord,
    domain_expected: &[StateChange],
    domain_changes: &[StateChange],
    follow_ups: &[NewJob],
) -> Result<bool> {
    let Some((old_value, current)) = read_record_async(config, &claimed.id).await? else {
        return Ok(false);
    };
    let now_ms = Utc::now().timestamp_millis();
    if current.status()? != JobStatus::Running
        || current.lease_epoch != claimed.lease_epoch
        || current.lease_token.as_deref()
            != claimed
                .queue_receipt
                .as_ref()
                .map(|receipt| receipt.token.as_str())
        || current
            .locked_until_ms
            .is_none_or(|lease_until| lease_until <= now_ms)
    {
        return Ok(false);
    }
    next.lease_epoch = current.lease_epoch;
    next.hint_generation = current.hint_generation;
    if next.status == JobStatus::Ready.as_str() {
        next.hint_generation = next.hint_generation.saturating_add(1);
    }
    next.lease_token = None;
    let mut expected = vec![StateChange {
        collection: JOBS.to_owned(),
        key: job_key(&claimed.id),
        value: Some(old_value),
    }];
    expected.extend_from_slice(domain_expected);
    let mut changes = vec![state_change(&next)?];
    let mut child_records = Vec::with_capacity(follow_ups.len());
    for (index, follow_up) in follow_ups.iter().enumerate() {
        let id = follow_up.id.clone().unwrap_or_else(|| {
            format!(
                "job:followup:{}:{}:{index}",
                claimed.id, claimed.lease_epoch
            )
        });
        let child = record_from_new(config, follow_up, id.clone());
        let key = job_key(&id);
        let existing = versioned::read_state_async(config, JOBS, &key).await?;
        if let Some(existing_value) = existing {
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key,
                value: Some(existing_value),
            });
            continue;
        }
        if let Some(dedupe) = &follow_up.dedupe_key {
            let dkey = dedupe_state_key(dedupe);
            let current_id = versioned::read_state_async(config, JOBS, &dkey).await?;
            if let Some(active_id) = current_id.as_ref().and_then(Value::as_str) {
                if let Some((active_value, active)) = read_record_async(config, active_id).await? {
                    if matches!(active.status()?, JobStatus::Ready | JobStatus::Running) {
                        expected.push(StateChange {
                            collection: JOBS.to_owned(),
                            key: dkey,
                            value: current_id.clone(),
                        });
                        expected.push(StateChange {
                            collection: JOBS.to_owned(),
                            key: job_key(active_id),
                            value: Some(active_value),
                        });
                        continue;
                    }
                }
            }
            expected.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey.clone(),
                value: current_id,
            });
            changes.push(StateChange {
                collection: JOBS.to_owned(),
                key: dkey,
                value: Some(Value::String(id)),
            });
        }
        expected.push(StateChange {
            collection: JOBS.to_owned(),
            key,
            value: None,
        });
        changes.push(state_change(&child)?);
        child_records.push(child);
    }
    if next.status != JobStatus::Ready.as_str() && next.status != JobStatus::Running.as_str() {
        if let Some(dedupe) = &current.dedupe_key {
            let key = dedupe_state_key(dedupe);
            let mapped = versioned::read_state_async(config, JOBS, &key).await?;
            if mapped.as_ref().and_then(Value::as_str) == Some(claimed.id.as_str()) {
                expected.push(StateChange {
                    collection: JOBS.to_owned(),
                    key: key.clone(),
                    value: mapped,
                });
                changes.push(StateChange {
                    collection: JOBS.to_owned(),
                    key,
                    value: None,
                });
            }
        }
    }
    changes.extend_from_slice(domain_changes);
    if !versioned::commit_if_state_matches_async(config, &expected, &[], &changes).await? {
        return Ok(false);
    }
    if let Some(receipt) = &claimed.queue_receipt {
        ack_async(config, receipt).await?;
    }
    if next.status == JobStatus::Ready.as_str() {
        enqueue_hint_async(config, &next).await?;
    }
    for child in &child_records {
        enqueue_hint_async(config, child).await?;
    }
    Ok(true)
}

/// Remove legacy retired-kind records with a conditional owner-head commit.
/// Ready-hint repair is handled separately by [`repair_pending_hints`].
/// Returns the number of retired rows removed.
pub fn purge_retired_jobs(config: &MemoryConfig) -> Result<usize> {
    for _ in 0..PUBLISH_RETRIES {
        let records = versioned::list_state(config, JOBS)?;
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        for (key, value) in records {
            if !key.starts_with(JOB_PREFIX) {
                continue;
            }
            let record = decode_job(&value)?;
            if RETIRED_JOB_KINDS.contains(&record.kind.as_str()) {
                expected.push(StateChange {
                    collection: JOBS.to_owned(),
                    key: key.clone(),
                    value: Some(value),
                });
                changes.push(StateChange {
                    collection: JOBS.to_owned(),
                    key,
                    value: None,
                });
            }
        }
        if changes.is_empty() {
            return Ok(0);
        }
        let count = changes.len();
        if versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
            return Ok(count);
        }
    }
    anyhow::bail!("retired queue job cleanup conflicted within retry budget")
}

pub(crate) async fn purge_retired_jobs_async(config: &MemoryConfig) -> Result<usize> {
    for _ in 0..PUBLISH_RETRIES {
        let records = versioned::list_state_async(config, JOBS).await?;
        let mut expected = Vec::new();
        let mut changes = Vec::new();
        for (key, value) in records {
            if !key.starts_with(JOB_PREFIX) {
                continue;
            }
            let record = decode_job(&value)?;
            if RETIRED_JOB_KINDS.contains(&record.kind.as_str()) {
                expected.push(StateChange {
                    collection: JOBS.to_owned(),
                    key: key.clone(),
                    value: Some(value),
                });
                changes.push(StateChange {
                    collection: JOBS.to_owned(),
                    key,
                    value: None,
                });
            }
        }
        if changes.is_empty() {
            return Ok(0);
        }
        let count = changes.len();
        if versioned::commit_if_state_matches_async(config, &expected, &[], &changes).await? {
            return Ok(count);
        }
    }
    anyhow::bail!("retired queue job cleanup conflicted within retry budget")
}

pub(crate) fn repair_pending_hints(config: &MemoryConfig) -> Result<usize> {
    let records = versioned::list_state(config, JOBS)?;
    let mut count = 0;
    let now_ms = Utc::now().timestamp_millis();
    for (key, value) in records {
        if !key.starts_with(JOB_PREFIX) {
            continue;
        }
        let mut record = decode_job(&value)?;
        if !is_retired_kind(&record.kind)
            && record.status()? == JobStatus::Ready
            && record.available_at_ms <= now_ms
        {
            // Advance the durable publication generation before publishing so
            // repair can recover even when the backend still remembers the
            // previous hint's finite-window dedupe key (for example after a
            // hint expired or was dead-lettered).
            record.hint_generation = record.hint_generation.saturating_add(1);
            if replace_record(config, &value, &record)? {
                enqueue_hint(config, &record)?;
                count += 1;
            }
        }
    }
    Ok(count)
}

pub(crate) async fn repair_pending_hints_async(config: &MemoryConfig) -> Result<usize> {
    let records = versioned::list_state_async(config, JOBS).await?;
    let mut count = 0;
    let now_ms = Utc::now().timestamp_millis();
    for (key, value) in records {
        if !key.starts_with(JOB_PREFIX) {
            continue;
        }
        let mut record = decode_job(&value)?;
        if !is_retired_kind(&record.kind)
            && record.status()? == JobStatus::Ready
            && record.available_at_ms <= now_ms
        {
            record.hint_generation = record.hint_generation.saturating_add(1);
            if versioned::commit_if_state_matches_async(
                config,
                &[StateChange {
                    collection: JOBS.to_owned(),
                    key: key.clone(),
                    value: Some(value),
                }],
                &[],
                &[state_change(&record)?],
            )
            .await?
            {
                enqueue_hint_async(config, &record).await?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Count rows with a given durable lifecycle status.
pub fn count_by_status(config: &MemoryConfig, status: JobStatus) -> Result<u64> {
    Ok(all_jobs(config)?
        .into_iter()
        .filter(|job| job.status == status)
        .count() as u64)
}

#[cfg(test)]
pub(crate) async fn count_by_status_async(config: &MemoryConfig, status: JobStatus) -> Result<u64> {
    Ok(all_jobs_async(config)
        .await?
        .into_iter()
        .filter(|job| job.status == status)
        .count() as u64)
}

#[cfg(test)]
pub(crate) async fn count_total_async(config: &MemoryConfig) -> Result<u64> {
    Ok(all_jobs_async(config).await?.len() as u64)
}

pub fn count_failed_unrecoverable(config: &MemoryConfig) -> Result<u64> {
    Ok(all_jobs(config)?
        .into_iter()
        .filter(|job| {
            job.status == JobStatus::Failed && job.failure_class.as_deref() == Some("unrecoverable")
        })
        .count() as u64)
}

pub fn count_total(config: &MemoryConfig) -> Result<u64> {
    Ok(all_jobs(config)?.len() as u64)
}

fn all_jobs(config: &MemoryConfig) -> Result<Vec<Job>> {
    versioned::list_state(config, JOBS)?
        .into_iter()
        .filter(|(key, _)| key.starts_with(JOB_PREFIX))
        .map(|(_, value)| decode_job(&value)?.public(None))
        .collect()
}

#[cfg(test)]
async fn all_jobs_async(config: &MemoryConfig) -> Result<Vec<Job>> {
    versioned::list_state_async(config, JOBS)
        .await?
        .into_iter()
        .filter(|(key, _)| key.starts_with(JOB_PREFIX))
        .map(|(_, value)| decode_job(&value)?.public(None))
        .collect()
}

pub fn get_job(config: &MemoryConfig, id: &str) -> Result<Option<Job>> {
    read_record(config, id)?
        .map(|(_, record)| record.public(None))
        .transpose()
}

pub(crate) async fn get_job_async(config: &MemoryConfig, id: &str) -> Result<Option<Job>> {
    read_record_async(config, id)
        .await?
        .map(|(_, record)| record.public(None))
        .transpose()
}

pub fn is_retired_kind(kind: &str) -> bool {
    RETIRED_JOB_KINDS.contains(&kind)
}

#[cfg(test)]
pub(crate) fn insert_test_record(config: &MemoryConfig, record: &JobRecord) -> Result<()> {
    versioned::write_state_changes(config, &[state_change(record)?])
}

#[cfg(test)]
pub(crate) async fn insert_test_record_async(
    config: &MemoryConfig,
    record: &JobRecord,
) -> Result<()> {
    versioned::commit_if_state_matches_async(config, &[], &[], &[state_change(record)?]).await?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn update_test_record(
    config: &MemoryConfig,
    id: &str,
    update: impl FnOnce(&mut JobRecord),
) -> Result<()> {
    let Some((old_value, mut record)) = read_record(config, id)? else {
        anyhow::bail!("test queue record {id} does not exist")
    };
    update(&mut record);
    if !replace_record(config, &old_value, &record)? {
        anyhow::bail!("test queue record changed during update")
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn expire_test_lease(config: &MemoryConfig, id: &str) -> Result<()> {
    update_test_record(config, id, |record| {
        record.locked_until_ms = Some(0);
    })
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) async fn update_test_record_async(
    config: &MemoryConfig,
    id: &str,
    update: impl FnOnce(&mut JobRecord),
) -> Result<()> {
    let Some((old_value, mut record)) = read_record_async(config, id).await? else {
        anyhow::bail!("test queue record {id} does not exist")
    };
    update(&mut record);
    if !versioned::commit_if_state_matches_async(
        config,
        &[StateChange {
            collection: JOBS.to_owned(),
            key: job_key(id),
            value: Some(old_value),
        }],
        &[],
        &[state_change(&record)?],
    )
    .await?
    {
        anyhow::bail!("test queue record changed during update")
    }
    Ok(())
}

/// Backoff math shared by settlement and its deterministic unit tests.
pub(crate) fn backoff_ms(attempts: u32) -> i64 {
    backoff_ms_with_policy(attempts, RETRY_BASE_MS, RETRY_CAP_MS)
}

pub(crate) fn backoff_ms_with_policy(attempts: u32, base_ms: i64, cap_ms: i64) -> i64 {
    let exponent = attempts.saturating_sub(1).min(62);
    let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
    base_ms.saturating_mul(multiplier).min(cap_ms)
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
