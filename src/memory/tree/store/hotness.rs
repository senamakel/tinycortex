//! Versioned entity-hotness counters.
//!
//! Hotness is a read-only subconscious signal used to gate topic-tree
//! materialisation. Rows are stored in the host-owned memory state collection
//! so they share the same scoped commit history as the entity index.

use anyhow::{Context, Result};
use chrono::Utc;

use super::types::HotnessCounters;
use crate::memory::chunks::collections::{ENTITY_HOTNESS, ENTITY_INDEX};
use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;

const CAS_RETRIES: usize = 8;

/// Fetch the hotness row for `entity_id`, or `None` if never seen.
pub fn get(config: &MemoryConfig, entity_id: &str) -> Result<Option<HotnessCounters>> {
    versioned::read_state(config, ENTITY_HOTNESS, entity_id)?
        .map(decode_counters)
        .transpose()
}

/// Async counterpart to [`get`] for current-thread Tokio callers.
pub async fn get_async(config: &MemoryConfig, entity_id: &str) -> Result<Option<HotnessCounters>> {
    versioned::read_state_async(config, ENTITY_HOTNESS, entity_id)
        .await?
        .map(decode_counters)
        .transpose()
}

/// Fetch the hotness row, or a fresh (all-zero) row if the entity has never
/// been seen. The fresh row is NOT persisted.
pub fn get_or_fresh(config: &MemoryConfig, entity_id: &str) -> Result<HotnessCounters> {
    match get(config, entity_id)? {
        Some(c) => Ok(c),
        None => Ok(HotnessCounters::fresh(
            entity_id,
            Utc::now().timestamp_millis(),
        )),
    }
}

/// Async counterpart to [`get_or_fresh`].
pub async fn get_or_fresh_async(config: &MemoryConfig, entity_id: &str) -> Result<HotnessCounters> {
    match get_async(config, entity_id).await? {
        Some(c) => Ok(c),
        None => Ok(HotnessCounters::fresh(
            entity_id,
            Utc::now().timestamp_millis(),
        )),
    }
}

/// Upsert the full counter row. Idempotent on `entity_id`.
pub fn upsert(config: &MemoryConfig, counters: &HotnessCounters) -> Result<()> {
    upsert_inner(config, counters)
}

/// Async counterpart to [`upsert`] for current-thread Tokio callers.
pub async fn upsert_async(config: &MemoryConfig, counters: &HotnessCounters) -> Result<()> {
    upsert_inner_async(config, counters).await
}

/// Count distinct non-null tree ids in the entity index for `entity_id`.
pub fn distinct_sources_for(config: &MemoryConfig, entity_id: &str) -> Result<u32> {
    let entries = versioned::list_state(config, ENTITY_INDEX)?;
    distinct_sources(entries, entity_id)
}

/// Async counterpart to [`distinct_sources_for`].
pub async fn distinct_sources_for_async(config: &MemoryConfig, entity_id: &str) -> Result<u32> {
    let entries = versioned::list_state_async(config, ENTITY_INDEX).await?;
    distinct_sources(entries, entity_id)
}

/// Test / diagnostic helper.
pub fn count(config: &MemoryConfig) -> Result<u64> {
    Ok(versioned::list_state(config, ENTITY_HOTNESS)?.len() as u64)
}

fn upsert_inner(config: &MemoryConfig, counters: &HotnessCounters) -> Result<()> {
    let value = serde_json::to_value(counters)?;
    for _ in 0..CAS_RETRIES {
        let current = versioned::read_state(config, ENTITY_HOTNESS, &counters.entity_id)?;
        let expected = StateChange {
            collection: ENTITY_HOTNESS.to_owned(),
            key: counters.entity_id.clone(),
            value: current,
        };
        let change = StateChange {
            collection: ENTITY_HOTNESS.to_owned(),
            key: counters.entity_id.clone(),
            value: Some(value.clone()),
        };
        if versioned::commit_if_state_matches(config, &[expected], &[], &[change])? {
            return Ok(());
        }
    }
    anyhow::bail!("entity hotness changed too frequently to upsert")
}

async fn upsert_inner_async(config: &MemoryConfig, counters: &HotnessCounters) -> Result<()> {
    let value = serde_json::to_value(counters)?;
    for _ in 0..CAS_RETRIES {
        let current =
            versioned::read_state_async(config, ENTITY_HOTNESS, &counters.entity_id).await?;
        let expected = StateChange {
            collection: ENTITY_HOTNESS.to_owned(),
            key: counters.entity_id.clone(),
            value: current,
        };
        let change = StateChange {
            collection: ENTITY_HOTNESS.to_owned(),
            key: counters.entity_id.clone(),
            value: Some(value.clone()),
        };
        if versioned::commit_if_state_matches_async(config, &[expected], &[], &[change]).await? {
            return Ok(());
        }
    }
    anyhow::bail!("entity hotness changed too frequently to upsert")
}

fn distinct_sources(entries: Vec<(String, serde_json::Value)>, entity_id: &str) -> Result<u32> {
    let mut trees = std::collections::HashSet::new();
    for (key, value) in entries
        .into_iter()
        .filter(|(key, _)| key.starts_with("occ:"))
    {
        let occurrence: crate::memory::store::entity_index::transaction::EntityOccurrence =
            serde_json::from_value(value).context("decode entity-index occurrence")?;
        anyhow::ensure!(
            crate::memory::store::entity_index::transaction::occurrence_key(
                &occurrence.entity_id,
                &occurrence.node_id
            ) == key,
            "entity-index occurrence key does not match its record"
        );
        if occurrence.entity_id == entity_id {
            if let Some(tree_id) = occurrence.tree_id {
                trees.insert(tree_id);
            }
        }
    }
    Ok(trees.len().min(u32::MAX as usize) as u32)
}

fn decode_counters(value: serde_json::Value) -> Result<HotnessCounters> {
    let counters: HotnessCounters =
        serde_json::from_value(value).context("decode entity hotness counters")?;
    anyhow::ensure!(!counters.entity_id.is_empty(), "empty entity hotness key");
    Ok(counters)
}

#[cfg(test)]
#[path = "hotness_tests.rs"]
mod tests;
