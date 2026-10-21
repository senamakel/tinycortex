//! Host-storage facade for the entity occurrence index.

use std::sync::Arc;

use anyhow::{Context, Result};

use super::transaction::{
    stage_node_occurrences, stage_upsert_node_entities, stage_upsert_node_entities_async,
    EntityOccurrence,
};
use super::types::{CanonicalEntity, EntityHit, EntityKind};
use crate::memory::chunks::versioned;
use crate::memory::config::MemoryConfig;

/// Resolves whether a canonical entity refers to the local user / self.
pub trait SelfIdentity: Send + Sync {
    /// Returns true when `(kind, surface)` matches a self-identity.
    fn is_self(&self, kind: EntityKind, surface: &str) -> bool;
}

/// A host identity resolver that never marks an entity as the user.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSelfIdentity;

impl SelfIdentity for NoSelfIdentity {
    fn is_self(&self, _kind: EntityKind, _surface: &str) -> bool {
        false
    }
}

/// Entity-index operations over already-prepared host storage.
pub struct EntityIndex {
    config: MemoryConfig,
    identity: Arc<dyn SelfIdentity>,
}

impl EntityIndex {
    /// Bind to the prepared host storage bundle with the default no-op identity.
    pub fn for_memory_config(config: &MemoryConfig) -> Result<Self> {
        Self::for_memory_config_with_identity(config, Arc::new(NoSelfIdentity))
    }

    /// Bind to prepared host storage with a host-provided self-identity resolver.
    pub fn for_memory_config_with_identity(
        config: &MemoryConfig,
        identity: Arc<dyn SelfIdentity>,
    ) -> Result<Self> {
        config.storage()?;
        Ok(Self {
            config: config.clone(),
            identity,
        })
    }

    fn is_user(&self, entity: &CanonicalEntity) -> bool {
        self.identity.is_self(entity.kind, &entity.surface)
    }

    /// Index one `(entity, node)` association.
    pub fn index_entity(
        &self,
        entity: &CanonicalEntity,
        node_id: &str,
        node_kind: &str,
        timestamp_ms: i64,
        tree_id: Option<&str>,
    ) -> Result<()> {
        self.index_entities(
            std::slice::from_ref(entity),
            node_id,
            node_kind,
            timestamp_ms,
            tree_id,
        )
        .map(|_| ())
    }

    /// Add or update occurrences for one node and increment its co-occurrence edges.
    pub fn index_entities(
        &self,
        entities: &[CanonicalEntity],
        node_id: &str,
        node_kind: &str,
        timestamp_ms: i64,
        tree_id: Option<&str>,
    ) -> Result<usize> {
        let occurrences = entities
            .iter()
            .map(|entity| EntityOccurrence {
                entity_id: entity.canonical_id.clone(),
                node_id: node_id.to_owned(),
                node_kind: node_kind.to_owned(),
                entity_kind: entity.kind.as_str().to_owned(),
                surface: entity.surface.clone(),
                score: entity.score,
                timestamp_ms,
                tree_id: tree_id.map(str::to_owned),
                is_user: self.is_user(entity),
            })
            .collect::<Vec<_>>();
        for _ in 0..8 {
            let staged = stage_upsert_node_entities(
                &self.config,
                node_id,
                occurrences.clone(),
                timestamp_ms,
            )?;
            if versioned::commit_if_state_matches(
                &self.config,
                &staged.expected,
                &[],
                &staged.changes,
            )? {
                return Ok(entities.len());
            }
        }
        anyhow::bail!("entity-index update conflicted repeatedly for node {node_id}")
    }

    /// Async counterpart to [`Self::index_entity`] for current-thread callers.
    pub async fn index_entity_async(
        &self,
        entity: &CanonicalEntity,
        node_id: &str,
        node_kind: &str,
        timestamp_ms: i64,
        tree_id: Option<&str>,
    ) -> Result<()> {
        self.index_entities_async(
            std::slice::from_ref(entity),
            node_id,
            node_kind,
            timestamp_ms,
            tree_id,
        )
        .await
        .map(|_| ())
    }

    /// Async counterpart to [`Self::index_entities`] for current-thread callers.
    pub async fn index_entities_async(
        &self,
        entities: &[CanonicalEntity],
        node_id: &str,
        node_kind: &str,
        timestamp_ms: i64,
        tree_id: Option<&str>,
    ) -> Result<usize> {
        let occurrences = entities
            .iter()
            .map(|entity| EntityOccurrence {
                entity_id: entity.canonical_id.clone(),
                node_id: node_id.to_owned(),
                node_kind: node_kind.to_owned(),
                entity_kind: entity.kind.as_str().to_owned(),
                surface: entity.surface.clone(),
                score: entity.score,
                timestamp_ms,
                tree_id: tree_id.map(str::to_owned),
                is_user: self.is_user(entity),
            })
            .collect::<Vec<_>>();
        for _ in 0..8 {
            let staged = stage_upsert_node_entities_async(
                &self.config,
                node_id,
                occurrences.clone(),
                timestamp_ms,
            )
            .await?;
            if versioned::commit_if_state_matches_async(
                &self.config,
                &staged.expected,
                &[],
                &staged.changes,
            )
            .await?
            {
                return Ok(entities.len());
            }
        }
        anyhow::bail!("entity-index update conflicted repeatedly for node {node_id}")
    }

    /// Replace summary occurrences, storing each canonical id as its surface.
    pub fn index_summary_entity_ids(
        &self,
        entity_ids: &[String],
        node_id: &str,
        score: f32,
        timestamp_ms: i64,
        tree_id: Option<&str>,
    ) -> Result<usize> {
        let occurrences = entity_ids
            .iter()
            .map(|entity_id| {
                let entity_kind = entity_id
                    .split_once(':')
                    .map_or(entity_id.as_str(), |(kind, _)| kind)
                    .to_owned();
                let is_user = canonical_id_is_user(self.identity.as_ref(), entity_id);
                EntityOccurrence {
                    entity_id: entity_id.clone(),
                    node_id: node_id.to_owned(),
                    node_kind: "summary".to_owned(),
                    entity_kind,
                    surface: entity_id.clone(),
                    score,
                    timestamp_ms,
                    tree_id: tree_id.map(str::to_owned),
                    is_user,
                }
            })
            .collect::<Vec<_>>();
        for _ in 0..8 {
            let staged = stage_upsert_node_entities(
                &self.config,
                node_id,
                occurrences.clone(),
                timestamp_ms,
            )?;
            if versioned::commit_if_state_matches(
                &self.config,
                &staged.expected,
                &[],
                &staged.changes,
            )? {
                return Ok(entity_ids.len());
            }
        }
        anyhow::bail!("summary entity-index update conflicted repeatedly for node {node_id}")
    }

    /// Remove all indexed occurrences for one node.
    pub fn clear_entity_index_for_node(&self, node_id: &str) -> Result<usize> {
        for _ in 0..8 {
            let staged = stage_node_occurrences(&self.config, node_id, std::iter::empty())?;
            let removed = staged
                .changes
                .iter()
                .filter(|change| {
                    change.collection == crate::memory::chunks::collections::ENTITY_INDEX
                        && change.value.is_none()
                })
                .count();
            if versioned::commit_if_state_matches(
                &self.config,
                &staged.expected,
                &[],
                &staged.changes,
            )? {
                return Ok(removed);
            }
        }
        anyhow::bail!("entity-index clear conflicted repeatedly for node {node_id}")
    }

    /// Find all nodes indexed against `entity_id`, newest first.
    pub fn lookup_entity(&self, entity_id: &str, limit: Option<usize>) -> Result<Vec<EntityHit>> {
        crate::memory::score::store::lookup_entity(&self.config, entity_id, limit)?
            .into_iter()
            .map(|hit| {
                Ok(EntityHit {
                    entity_id: hit.entity_id,
                    node_id: hit.node_id,
                    node_kind: hit.node_kind,
                    entity_kind: EntityKind::parse(hit.entity_kind.as_str())
                        .map_err(anyhow::Error::msg)
                        .context("decode entity kind")?,
                    surface: hit.surface,
                    score: hit.score,
                    timestamp_ms: hit.timestamp_ms,
                    tree_id: hit.tree_id,
                    is_user: hit.is_user,
                })
            })
            .collect()
    }

    /// List the distinct entity ids associated with a node.
    pub fn list_entity_ids_for_node(&self, node_id: &str) -> Result<Vec<String>> {
        crate::memory::score::store::list_entity_ids_for_node(&self.config, node_id)
    }

    /// Count indexed occurrence rows (manifests are excluded).
    pub fn count_entity_index(&self) -> Result<u64> {
        crate::memory::score::store::count_entity_index(&self.config)
    }
}

fn canonical_id_is_user(identity: &dyn SelfIdentity, canonical_id: &str) -> bool {
    let Some((kind_str, value)) = canonical_id.split_once(':') else {
        return false;
    };
    let Ok(kind) = EntityKind::parse(kind_str) else {
        return false;
    };
    identity.is_self(kind, value)
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
