//! Incremental-run state for the persona pipeline (doc 06 §6.7).
//!
//! Mirrors the `sync::state::SyncStateStore` pattern with a persona-local
//! [`PersonaStateStore`] trait (the `sync` module is gated behind the heavy
//! `sync`/reqwest feature, and persona stays dependency-light). Cursors are
//! deliberately coarse and cheap:
//!
//! - JSONL / transcript sources: `(mtime_ms, len)` per file — append-only in
//!   practice, so an unchanged `(mtime, len)` skips the file.
//! - Git: last-distilled HEAD sha per repo (+ the author-set hash, so changing
//!   the email list forces a re-scan).
//! - Instruction files: content sha per path — re-digest only on change.
//!
//! [`KvStateStore`] persists cursors through the host's scoped KV port, so they
//! survive across process runs. Evidence ids are content-addressed (§6.3), so
//! overlapping cursors are harmless — this state is a *fast-skip* optimisation,
//! not a correctness gate.

use std::path::Path;

use crate::memory::store::state_kv::StateKvStore;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Persona-scoped state namespace.
pub const NAMESPACE: &str = "persona-sync-state";

/// A minimal key/value state store, scoped by namespace. Mirrors
/// `sync::state::SyncStateStore` so the same persistence pattern is reused
/// without depending on the feature-gated `sync` module.
#[async_trait]
pub trait PersonaStateStore: Send + Sync {
    /// Fetch a stored value, if any.
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<serde_json::Value>>;
    /// Store a value, overwriting any prior value.
    async fn set(&self, namespace: &str, key: &str, value: &serde_json::Value) -> Result<()>;
}

/// A file's fast-skip cursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileCursor {
    /// File modification time in milliseconds since the epoch.
    pub mtime_ms: i64,
    /// File length in bytes.
    pub len: u64,
}

impl FileCursor {
    /// Read the current cursor for `path`.
    pub fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        let mtime_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)?;
        Some(Self {
            mtime_ms,
            len: meta.len(),
        })
    }
}

/// Cursor key for a transcript/instruction file of a given source kind.
pub fn file_key(source_kind: &str, path: &Path) -> String {
    format!("{source_kind}:{}", path.display())
}

/// Cursor key for a git repo.
pub fn git_key(repo: &Path) -> String {
    format!("git_history:{}", repo.display())
}

/// True when `path` is unchanged since its recorded cursor (skip it).
pub async fn file_unchanged(store: &dyn PersonaStateStore, key: &str, path: &Path) -> Result<bool> {
    let current = match FileCursor::of(path) {
        Some(c) => c,
        None => return Ok(false),
    };
    match store.get(NAMESPACE, key).await? {
        Some(v) => {
            let stored: FileCursor = match serde_json::from_value(v) {
                Ok(c) => c,
                Err(_) => return Ok(false),
            };
            Ok(stored == current)
        }
        None => Ok(false),
    }
}

/// Record `path`'s current cursor under `key`.
pub async fn record_file(store: &dyn PersonaStateStore, key: &str, path: &Path) -> Result<()> {
    if let Some(cursor) = FileCursor::of(path) {
        store
            .set(NAMESPACE, key, &serde_json::to_value(cursor)?)
            .await?;
    }
    Ok(())
}

/// True when the recorded string watermark under `key` equals `value` (skip).
pub async fn watermark_unchanged(
    store: &dyn PersonaStateStore,
    key: &str,
    value: &str,
) -> Result<bool> {
    match store.get(NAMESPACE, key).await? {
        Some(serde_json::Value::String(s)) => Ok(s == value),
        _ => Ok(false),
    }
}

/// Record a string watermark (git sha, content sha) under `key`.
pub async fn record_watermark(store: &dyn PersonaStateStore, key: &str, value: &str) -> Result<()> {
    store
        .set(
            NAMESPACE,
            key,
            &serde_json::Value::String(value.to_string()),
        )
        .await
}

/// Host-KV-backed [`PersonaStateStore`]. The host owns its scope and backend;
/// cursor values remain ordinary JSON application data.
#[derive(Clone)]
pub struct KvStateStore {
    kv: StateKvStore,
}

/// One opaque cursor value read by a host from the legacy persona state file.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyPersonaStateEntry {
    /// Original state key, without the legacy file's namespace prefix.
    pub key: String,
    /// Original JSON value, retained without normalization.
    pub value: serde_json::Value,
}

impl KvStateStore {
    /// Attach to the host's prepared KV port.
    pub fn from_config(config: &crate::memory::config::MemoryConfig) -> Result<Self> {
        Ok(Self {
            kv: StateKvStore::from_config(config)?,
        })
    }

    fn key(namespace: &str, key: &str) -> String {
        format!("{}:{namespace}{key}", namespace.len())
    }

    /// Import legacy cursor data without overwriting values already advanced
    /// by the active pipeline. Replaying the same import is idempotent.
    pub async fn import_legacy_entries(&self, entries: &[LegacyPersonaStateEntry]) -> Result<()> {
        let owner_entries = entries
            .iter()
            .map(|entry| (Self::key(NAMESPACE, &entry.key), entry.value.clone()))
            .collect::<Vec<_>>();
        self.kv
            .import_entries(NAMESPACE, &owner_entries)
            .await
    }
}

#[async_trait]
impl PersonaStateStore for KvStateStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<serde_json::Value>> {
        self.kv.get(NAMESPACE, &Self::key(namespace, key)).await
    }

    async fn set(&self, namespace: &str, key: &str, value: &serde_json::Value) -> Result<()> {
        self.kv
            .set(NAMESPACE, &Self::key(namespace, key), value.clone())
            .await
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
