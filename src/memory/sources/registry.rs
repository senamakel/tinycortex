//! Host-config-backed memory source registry.
//!
//! TinyCortex owns source validation and mutation rules. The host owns the
//! durable configuration and supplies a revisioned compare-and-set adapter so
//! concurrent source updates cannot overwrite one another. Legacy TOML import
//! is a one-time host operation; this module never reads or writes config
//! files.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;

use super::types::{MemorySourceEntry, MemorySourcePatch, SourceKind};

const UPDATE_RETRIES: usize = 16;

/// A host snapshot used to compare source configuration updates.
#[derive(Debug, Clone)]
pub struct SourceConfigSnapshot {
    /// Host-defined opaque revision for the exact source list returned.
    pub revision: String,
    /// Sources decoded from the host's active configuration.
    pub entries: Vec<MemorySourceEntry>,
}

/// Host-owned configuration access for memory source entries.
///
/// `compare_and_set` must replace only the source list when `expected_revision`
/// still identifies the snapshot. Hosts preserve unrelated config settings and
/// serialize the operation with their other config writers.
#[async_trait]
pub trait SourceConfigPort: Send + Sync {
    /// Read the current revision and validated/raw source list.
    async fn read(&self) -> Result<SourceConfigSnapshot>;

    /// Replace the source list if its snapshot is still current.
    ///
    /// Returns `false` on a revision conflict and must leave the config intact.
    async fn compare_and_set(
        &self,
        expected_revision: &str,
        entries: Vec<MemorySourceEntry>,
    ) -> Result<bool>;
}

/// Registry operations over a host-provided source-configuration adapter.
#[derive(Clone)]
pub struct SourceRegistry {
    port: Arc<dyn SourceConfigPort>,
}

impl std::fmt::Debug for SourceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceRegistry").finish_non_exhaustive()
    }
}

impl SourceRegistry {
    /// Attach to the host's memory source configuration adapter.
    pub fn from_port(port: Arc<dyn SourceConfigPort>) -> Self {
        Self { port }
    }

    /// List every configured source.
    pub async fn list(&self) -> Result<Vec<MemorySourceEntry>> {
        Ok(self.port.read().await?.entries)
    }

    /// List enabled sources of a given [`SourceKind`].
    pub async fn list_enabled_by_kind(&self, kind: SourceKind) -> Result<Vec<MemorySourceEntry>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .filter(|source| source.kind == kind && source.enabled)
            .collect())
    }

    /// Get a single source by id, if present.
    pub async fn get(&self, id: &str) -> Result<Option<MemorySourceEntry>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .find(|source| source.id == id))
    }

    async fn mutate<T>(
        &self,
        mut apply: impl FnMut(&mut Vec<MemorySourceEntry>) -> Result<T>,
    ) -> Result<T> {
        for _ in 0..UPDATE_RETRIES {
            let snapshot = self.port.read().await?;
            let mut entries = snapshot.entries.clone();
            let result = apply(&mut entries)?;
            if serde_json::to_value(&entries)? == serde_json::to_value(&snapshot.entries)? {
                return Ok(result);
            }
            if self
                .port
                .compare_and_set(&snapshot.revision, entries)
                .await?
            {
                return Ok(result);
            }
        }
        bail!("source configuration changed too often to update safely")
    }

    /// Validate and add a new source. Fails if the id already exists.
    pub async fn add(&self, entry: MemorySourceEntry) -> Result<MemorySourceEntry> {
        entry.validate().map_err(|error| anyhow!(error))?;
        self.mutate(|sources| {
            if sources.iter().any(|source| source.id == entry.id) {
                bail!("source with id '{}' already exists", entry.id);
            }
            sources.push(entry.clone());
            Ok(entry.clone())
        })
        .await
    }

    /// Apply a patch to an existing source, then revalidate it.
    pub async fn update(&self, id: &str, patch: MemorySourcePatch) -> Result<MemorySourceEntry> {
        self.mutate(|sources| {
            let entry = sources
                .iter_mut()
                .find(|source| source.id == id)
                .ok_or_else(|| anyhow!("source '{id}' not found"))?;
            patch.validate_for_kind(entry.kind.clone())?;
            patch.apply_to(entry);
            entry.validate().map_err(|error| anyhow!(error))?;
            Ok(entry.clone())
        })
        .await
    }

    /// Remove a source by id. Returns `true` if an entry was removed.
    pub async fn remove(&self, id: &str) -> Result<bool> {
        self.mutate(|sources| {
            let before = sources.len();
            sources.retain(|source| source.id != id);
            Ok(sources.len() < before)
        })
        .await
    }

    /// Remove all Composio sources bound to a connection id.
    pub async fn remove_composio_source_by_connection_id(
        &self,
        connection_id: &str,
    ) -> Result<usize> {
        self.mutate(|sources| {
            let before = sources.len();
            sources.retain(|source| {
                !(source.kind == SourceKind::Composio
                    && source.connection_id.as_deref() == Some(connection_id))
            });
            Ok(before - sources.len())
        })
        .await
    }

    /// Upsert a Composio source keyed by its connected-account id.
    pub async fn upsert_composio_source(
        &self,
        toolkit: &str,
        connection_id: &str,
        label: &str,
    ) -> Result<MemorySourceEntry> {
        self.mutate(|sources| {
            Ok(upsert_composio_entry_in_place(sources, toolkit, connection_id, label).0)
        })
        .await
    }

    /// Batch-upsert Composio sources with one host-config compare-and-set.
    pub async fn upsert_composio_sources_batch(
        &self,
        targets: &[ComposioUpsertTarget],
    ) -> Result<u32> {
        if targets.is_empty() {
            return Ok(0);
        }
        self.mutate(|sources| {
            for (toolkit, connection_id, label) in targets {
                upsert_composio_entry_in_place(sources, toolkit, connection_id, label);
            }
            Ok(targets.len().min(u32::MAX as usize) as u32)
        })
        .await
    }

    /// Enable all sources and clear their per-source sync caps.
    pub async fn apply_all_in(&self) -> Result<Vec<MemorySourceEntry>> {
        self.mutate(|sources| {
            for source in sources.iter_mut() {
                source.enabled = true;
                source.max_items = None;
                source.since_days = None;
                source.sync_depth_days = None;
                source.max_commits = None;
                source.max_issues = None;
                source.max_prs = None;
                source.max_tokens_per_sync = None;
                source.max_cost_per_sync_usd = None;
            }
            Ok(sources.clone())
        })
        .await
    }
}

pub type ComposioUpsertTarget = (String, String, String);

/// Conservative default sync caps for a Composio toolkit.
pub fn memory_sync_defaults_for_toolkit(toolkit: &str) -> (Option<u32>, Option<u32>) {
    match toolkit {
        "gmail" => (Some(100), Some(30)),
        "slack" => (Some(50), Some(14)),
        "notion" => (Some(30), Some(30)),
        "linear" | "clickup" | "github" => (Some(50), Some(30)),
        _ => (Some(30), Some(14)),
    }
}

/// Apply a single Composio upsert to an in-memory source list.
pub(crate) fn upsert_composio_entry_in_place(
    sources: &mut Vec<MemorySourceEntry>,
    toolkit: &str,
    connection_id: &str,
    label: &str,
) -> (MemorySourceEntry, bool) {
    if let Some(existing) = sources.iter_mut().find(|source| {
        source.kind == SourceKind::Composio
            && source.connection_id.as_deref() == Some(connection_id)
    }) {
        existing.label = label.to_string();
        return (existing.clone(), false);
    }

    let (max_items, sync_depth_days) = memory_sync_defaults_for_toolkit(toolkit);
    let entry = MemorySourceEntry {
        id: format!("src_{}", uuid::Uuid::new_v4().as_simple()),
        kind: SourceKind::Composio,
        label: label.to_string(),
        enabled: true,
        toolkit: Some(toolkit.to_string()),
        connection_id: Some(connection_id.to_string()),
        path: None,
        glob: None,
        url: None,
        branch: None,
        paths: Vec::new(),
        max_commits: None,
        max_issues: None,
        max_prs: None,
        query: None,
        since_days: None,
        max_items,
        selector: None,
        max_tokens_per_sync: None,
        max_cost_per_sync_usd: None,
        sync_depth_days,
    };
    sources.push(entry.clone());
    (entry, true)
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
