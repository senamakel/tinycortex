//! Append-only synchronization cost and outcome audit log.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tinystoragedrivers_api::StreamStore;

use crate::memory::config::MemoryConfig;

const AUDIT_STREAM: &str = "tinycortex:sync-audit:v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncAuditEntry {
    pub timestamp: DateTime<Utc>,
    pub source_id: String,
    pub source_kind: String,
    pub scope: String,
    pub items_fetched: u32,
    pub batches: u32,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated_cost_usd: f64,
    #[serde(default)]
    pub composio_actions_called: u32,
    #[serde(default)]
    pub composio_cost_usd: f64,
    #[serde(default)]
    pub actual_charged_usd: Option<f64>,
    pub duration_ms: u64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl SyncAuditEntry {
    pub fn combined_cost_usd(&self) -> f64 {
        self.effective_cost_usd()
    }

    pub fn effective_cost_usd(&self) -> f64 {
        self.actual_charged_usd.unwrap_or(self.estimated_cost_usd) + self.composio_cost_usd
    }
}

pub fn append_audit_entry(config: &MemoryConfig, entry: &SyncAuditEntry) -> anyhow::Result<()> {
    let blocking = config.storage_blocking()?.clone();
    let streams = Arc::clone(config.storage()?.streams());
    let value = serde_json::to_value(entry)?;
    blocking
        .run(async move { streams.append(AUDIT_STREAM, value).await })
        .map_err(anyhow::Error::from)?;
    tracing::debug!(source_id = %entry.source_id, success = entry.success, "[memory_sync:audit] entry appended");
    Ok(())
}

pub async fn append_audit_entry_async(
    config: &MemoryConfig,
    entry: &SyncAuditEntry,
) -> anyhow::Result<()> {
    let streams = Arc::clone(config.storage()?.streams());
    let value = serde_json::to_value(entry)?;
    streams
        .append(AUDIT_STREAM, value)
        .await
        .map_err(anyhow::Error::from)?;
    tracing::debug!(source_id = %entry.source_id, success = entry.success, "[memory_sync:audit] entry appended");
    Ok(())
}

pub fn read_audit_log(config: &MemoryConfig) -> anyhow::Result<Vec<SyncAuditEntry>> {
    let blocking = config.storage_blocking()?.clone();
    let streams = Arc::clone(config.storage()?.streams());
    blocking
        .run(async move { read_entries(&*streams).await })
        .map_err(anyhow::Error::from)?
}

pub async fn read_audit_log_async(config: &MemoryConfig) -> anyhow::Result<Vec<SyncAuditEntry>> {
    read_entries(&**config.storage()?.streams()).await
}

async fn read_entries(streams: &dyn tinystoragedrivers_api::StreamStore) -> anyhow::Result<Vec<SyncAuditEntry>> {
    let len = streams.len(AUDIT_STREAM).await?;
    let mut offset = 0_u64;
    let mut entries = Vec::new();
    while offset < len {
        let page = streams.read_window(AUDIT_STREAM, offset, 512).await?;
        anyhow::ensure!(!page.is_empty(), "audit stream ended before its reported length");
        for record in page {
            offset = record.offset.saturating_add(1);
            match serde_json::from_value::<SyncAuditEntry>(record.value) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    tracing::warn!(%error, "[memory_sync:audit] malformed audit record skipped");
                }
            }
        }
    }
    entries.reverse();
    Ok(entries)
}

pub fn estimate_cost_usd(input_tokens: u64, output_tokens: u64) -> f64 {
    input_tokens as f64 * 0.07 / 1_000_000.0 + output_tokens as f64 * 0.28 / 1_000_000.0
}

#[derive(Debug, Default, Clone)]
pub struct RealCostAccumulator {
    total_batches: u32,
    batches_with_usage: u32,
    batches_with_charge: u32,
    est_input_tokens: u64,
    est_output_tokens: u64,
    real_input_tokens: u64,
    real_output_tokens: u64,
    real_charged_usd: f64,
}

impl RealCostAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_batch(
        &mut self,
        est_input: u64,
        est_output: u64,
        real_input: u64,
        real_output: u64,
        charge: Option<f64>,
    ) {
        self.total_batches = self.total_batches.saturating_add(1);
        self.est_input_tokens = self.est_input_tokens.saturating_add(est_input);
        self.est_output_tokens = self.est_output_tokens.saturating_add(est_output);
        if real_input > 0 || real_output > 0 {
            self.batches_with_usage = self.batches_with_usage.saturating_add(1);
            self.real_input_tokens = self.real_input_tokens.saturating_add(real_input);
            self.real_output_tokens = self.real_output_tokens.saturating_add(real_output);
        }
        if let Some(charge) = charge {
            self.batches_with_charge = self.batches_with_charge.saturating_add(1);
            self.real_charged_usd += charge;
        }
    }

    fn usage_is_complete(&self) -> bool {
        self.total_batches > 0 && self.batches_with_usage == self.total_batches
    }

    fn charge_is_complete(&self) -> bool {
        self.total_batches > 0 && self.batches_with_charge == self.total_batches
    }

    pub fn audit_input_tokens(&self) -> u64 {
        if self.usage_is_complete() {
            self.real_input_tokens
        } else {
            self.est_input_tokens
        }
    }

    pub fn audit_output_tokens(&self) -> u64 {
        if self.usage_is_complete() {
            self.real_output_tokens
        } else {
            self.est_output_tokens
        }
    }

    pub fn estimated_cost(&self) -> f64 {
        estimate_cost_usd(self.est_input_tokens, self.est_output_tokens)
    }

    pub fn actual_charged_usd(&self) -> Option<f64> {
        self.charge_is_complete().then_some(self.real_charged_usd)
    }

    pub fn usage_is_real(&self) -> bool {
        self.usage_is_complete()
    }

    pub fn effective_cost_usd(&self) -> f64 {
        self.actual_charged_usd()
            .unwrap_or_else(|| self.estimated_cost())
    }

    pub fn cost_is_actual(&self) -> bool {
        self.charge_is_complete()
    }
}

#[cfg(test)]
#[path = "audit_tests.rs"]
mod tests;
