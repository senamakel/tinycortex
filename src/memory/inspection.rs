//! Read-only, owner-shaped snapshots for server-side inspection tools.
//!
//! The caller supplies an already opened and scope-bound storage bundle. This
//! API performs reads only and intentionally skips `prepare_storage`, schema
//! declaration, and queue creation. It never selects a backend or fallback.

use serde::Serialize;
use tinystoragedrivers_api::Storage;

use crate::memory::chunks::{self, Chunk, ListChunksQuery};
use crate::memory::config::MemoryConfig;
use crate::memory::store::KvStore;
use crate::memory::tree::store::{self, SummaryNode, Tree, TreeKind};
use crate::memory::types::MemoryKvRecord;

/// Typed data needed by the standalone viewer, collected through owner APIs.
#[derive(Debug, Clone, Serialize)]
pub struct InspectionSnapshot {
    /// Skill-document KV rows, including their logical namespace and timestamp.
    pub skill_documents: Vec<MemoryKvRecord>,
    /// Bounded newest-first chunk page.
    pub chunks: Vec<Chunk>,
    /// Chunks referenced by summaries, fetched independently of the page.
    pub graph_chunks: Vec<Chunk>,
    /// Number of chunks visible to the same owner scope.
    pub chunk_total: u64,
    /// Trees in the scope, in owner-defined order.
    pub trees: Vec<Tree>,
    /// Non-deleted summaries for the listed trees.
    pub summaries: Vec<SummaryNode>,
}

/// Read one inspection snapshot from an explicitly supplied storage bundle.
///
/// `limit` is clamped to the chunk store's supported maximum. The caller must
/// open and scope `storage`; this function neither prepares collections nor
/// falls back to a local backend.
pub async fn read_snapshot(
    workspace: impl Into<std::path::PathBuf>,
    storage: Storage,
    limit: usize,
) -> anyhow::Result<InspectionSnapshot> {
    let config = MemoryConfig::for_inspection(workspace, storage)?;
    let kv = KvStore::from_config(&config)?;
    let skill_documents = kv
        .records_namespaces_with_prefix_async("skilldoc")
        .await
        .map_err(anyhow::Error::msg)?;

    let chunk_total = chunks::count_chunks_async(&config).await?;
    let chunks = chunks::list_chunks_async(
        &config,
        &ListChunksQuery {
            limit: Some(limit.clamp(1, 10_000)),
            ..Default::default()
        },
    )
    .await?;

    let mut trees = Vec::new();
    for kind in [
        TreeKind::Source,
        TreeKind::Topic,
        TreeKind::Global,
        TreeKind::Flavoured,
    ] {
        trees.extend(store::list_trees_by_kind_async(&config, kind).await?);
    }
    let mut summaries = Vec::new();
    for tree in &trees {
        // A corrupt or hostile row must not turn inspection into an unbounded
        // query loop. Normal tree levels are small; cap the viewer projection.
        for level in 1..=tree.max_level.min(64) {
            summaries.extend(store::list_summaries_at_level_async(&config, &tree.id, level).await?);
        }
    }

    let summary_ids = summaries
        .iter()
        .map(|summary| summary.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let referenced_chunk_ids = summaries
        .iter()
        .flat_map(|summary| summary.child_ids.iter())
        .filter(|id| !summary_ids.contains(id.as_str()))
        .take(10_000)
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut referenced_chunk_ids = referenced_chunk_ids.into_iter().collect::<Vec<_>>();
    referenced_chunk_ids.sort();
    let graph_chunks = if referenced_chunk_ids.is_empty() {
        Vec::new()
    } else {
        chunks::list_chunks_async(
            &config,
            &ListChunksQuery {
                ids: referenced_chunk_ids,
                limit: Some(10_000),
                ..Default::default()
            },
        )
        .await?
    };

    Ok(InspectionSnapshot {
        skill_documents,
        chunks,
        graph_chunks,
        chunk_total,
        trees,
        summaries,
    })
}

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod tests;
