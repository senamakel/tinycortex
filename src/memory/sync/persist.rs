//! Durable host sinks for live synchronization.
//!
//! Sync pipelines hand [`SkillDocument`]s to a host-owned [`SkillDocSink`] (see
//! [`crate::memory::sync::traits`]); the crate otherwise ships only in-memory
//! sinks for tests, so nothing a sync run ingests survives the process. That is
//! the right default for a library, but it makes a sync run impossible to
//! observe after the fact.
//!
//! [`KvSkillDocSink`] closes that gap with a durable, inspectable reference
//! sink backed by the host's [`KvStore`] port. Every document becomes one
//! namespaced owner record, so a host — or a debug viewer — can browse exactly
//! what a sync run ingested without re-running it (and without a live Composio
//! key).
//!
//! ## Layout
//!
//! - namespace: `skilldoc:<namespace_skill_id>` (the toolkit slug every
//!   provider sets, e.g. `skilldoc:gmail`)
//! - key: `document_id` (e.g. `gmail:18f2…`)
//! - value: the full JSON-serialised [`SkillDocument`]
//!
//! One record per document keeps deletes direct and lets a reader enumerate a
//! toolkit's documents with a namespace listing.
//!
//! ## Safety
//!
//! Writes go through [`KvStore::set_namespace`], which **sanitizes the stored
//! value** (redacting secrets/PII) before it is persisted. The persisted
//! documents therefore match the scrubbed content the rest of the memory system
//! would hold — a debug viewer over this store never surfaces raw credentials.
//! The namespace/key are derived from safe slugs and ids; if a provider ever
//! emits a `document_id` that itself looks like a secret or personal identifier
//! the underlying store rejects the write, and the error surfaces here rather
//! than being silently dropped.

use std::sync::Arc;

use async_trait::async_trait;

use crate::memory::config::MemoryConfig;
use crate::memory::store::KvStore;
use crate::memory::sync::traits::{SkillDocSink, SkillDocument};

/// KV namespace prefix under which skill documents are stored.
pub const SKILLDOC_NS_PREFIX: &str = "skilldoc:";

/// Durable [`SkillDocSink`] that persists every [`SkillDocument`] as one row in
/// a [`KvStore`], so a host or debug viewer can inspect ingested memory.
///
/// The sink is cheap to clone-share (`Arc<KvStore>` inside); the store
/// serializes its own writes behind a mutex, so concurrent `store`/`delete`
/// calls are safe.
pub struct KvSkillDocSink {
    kv: Arc<KvStore>,
}

impl KvSkillDocSink {
    /// Wrap an already-open [`KvStore`].
    pub fn new(kv: Arc<KvStore>) -> Self {
        Self { kv }
    }

    /// Attach to the prepared owner KV port selected by the host.
    pub fn from_config(config: &MemoryConfig) -> anyhow::Result<Self> {
        Ok(Self::new(Arc::new(KvStore::from_config(config)?)))
    }

    /// Borrow the backing store (e.g. to read documents back for inspection).
    pub fn store_handle(&self) -> &Arc<KvStore> {
        &self.kv
    }

    /// KV namespace a given skill id is stored under.
    pub fn namespace_for(namespace_skill_id: &str) -> String {
        format!("{SKILLDOC_NS_PREFIX}{namespace_skill_id}")
    }
}

#[async_trait]
impl SkillDocSink for KvSkillDocSink {
    async fn store(&self, document: SkillDocument) -> anyhow::Result<()> {
        let namespace = Self::namespace_for(&document.namespace_skill_id);
        let key = document.document_id.clone();
        let value = serde_json::to_value(&document)
            .map_err(|error| anyhow::anyhow!("serialize skill doc {key}: {error}"))?;
        self.kv
            .set_namespace_async(&namespace, &key, &value)
            .await
            .map_err(|error| anyhow::anyhow!("persist skill doc {key}: {error}"))?;
        Ok(())
    }

    async fn delete(&self, namespace_skill_id: &str, document_id: &str) -> anyhow::Result<()> {
        let namespace = Self::namespace_for(namespace_skill_id);
        self.kv
            .delete_namespace_async(&namespace, document_id)
            .await
            .map_err(|error| anyhow::anyhow!("delete skill doc {document_id}: {error}"))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "persist_tests.rs"]
mod tests;
