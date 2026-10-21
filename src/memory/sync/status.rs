//! Pull-based synchronization status derived from the authoritative chunk store.

use serde::{Deserialize, Serialize};

use crate::memory::chunks::{collections, versioned};
use crate::memory::config::MemoryConfig;

const WAVE_WINDOW_MS: i64 = 10 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessLabel {
    Active,
    Recent,
    Idle,
}

impl FreshnessLabel {
    pub fn from_age_ms(last_chunk_at_ms: Option<i64>, now_ms: i64) -> Self {
        match last_chunk_at_ms {
            None => Self::Idle,
            Some(timestamp) => match now_ms.saturating_sub(timestamp) {
                age if age <= 30_000 => Self::Active,
                age if age <= 5 * 60_000 => Self::Recent,
                _ => Self::Idle,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemorySyncStatus {
    pub provider: String,
    pub chunks_synced: u64,
    pub chunks_pending: u64,
    pub batch_total: u64,
    pub batch_processed: u64,
    pub last_chunk_at_ms: Option<i64>,
    pub freshness: FreshnessLabel,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusListResponse {
    pub statuses: Vec<MemorySyncStatus>,
}

pub fn list_sync_statuses(config: &MemoryConfig) -> anyhow::Result<Vec<MemorySyncStatus>> {
    list_sync_statuses_at(config, chrono::Utc::now().timestamp_millis())
}

fn list_sync_statuses_at(
    config: &MemoryConfig,
    now_ms: i64,
) -> anyhow::Result<Vec<MemorySyncStatus>> {
    let chunks = versioned::list_chunks(config)?;
    let embedded = state_chunk_ids(config, collections::CHUNK_EMBEDDINGS)?;
    let skipped = state_chunk_ids(config, collections::CHUNK_REEMBED_SKIPS)?;
    let lifecycle = versioned::list_state(config, collections::CHUNK_LIFECYCLE)?
        .into_iter()
        .filter_map(|(key, value)| {
            (value.get("status").and_then(serde_json::Value::as_str) == Some("dropped"))
                .then_some(key)
        })
        .collect::<std::collections::HashSet<_>>();

    let mut by_provider = std::collections::HashMap::<String, Vec<(i64, i64, bool)>>::new();
    for chunk in chunks {
        let source_id = &chunk.metadata.source_id;
        let provider = source_id
            .split_once(':')
            .map_or_else(|| chunk.metadata.source_kind.as_str().to_owned(), |(name, _)| name.to_owned());
        let created_at_ms = chunk.created_at.timestamp_millis();
        let timestamp_ms = chunk.metadata.timestamp.timestamp_millis();
        let resolved = embedded.contains(&chunk.id)
            || skipped.contains(&chunk.id)
            || lifecycle.contains(&chunk.id);
        by_provider
            .entry(provider)
            .or_default()
            .push((created_at_ms, timestamp_ms, resolved));
    }

    let mut statuses = Vec::with_capacity(by_provider.len());
    for (provider, rows) in by_provider {
        let max_created = rows.iter().map(|row| row.0).max().unwrap_or_default();
        let wave_rows = rows
            .iter()
            .filter(|row| row.0 >= max_created.saturating_sub(WAVE_WINDOW_MS))
            .collect::<Vec<_>>();
        let pending_in_wave = wave_rows.iter().filter(|row| !row.2).count();
        let anchor = (pending_in_wave > 0)
            .then(|| wave_rows.iter().map(|row| row.0).min())
            .flatten();
        let batch_rows = anchor
            .map(|anchor| rows.iter().filter(|row| row.0 >= anchor).collect::<Vec<_>>())
            .unwrap_or_default();
        let last_chunk_at_ms = rows.iter().map(|row| row.1).max();
        statuses.push(MemorySyncStatus {
            provider,
            chunks_synced: rows.len() as u64,
            chunks_pending: rows.iter().filter(|row| !row.2).count() as u64,
            batch_total: batch_rows.len() as u64,
            batch_processed: batch_rows.iter().filter(|row| row.2).count() as u64,
            last_chunk_at_ms,
            freshness: FreshnessLabel::from_age_ms(last_chunk_at_ms, now_ms),
        });
    }
    statuses.sort_by(|left, right| right.last_chunk_at_ms.cmp(&left.last_chunk_at_ms));
    Ok(statuses)
}

fn state_chunk_ids(
    config: &MemoryConfig,
    collection: &str,
) -> anyhow::Result<std::collections::HashSet<String>> {
    versioned::list_state(config, collection)?
        .into_iter()
        .map(|(key, value)| {
            value
                .get("chunk_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("malformed {collection} record at {key:?}"))
        })
        .collect()
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
