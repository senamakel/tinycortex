//! Pure storage declarations owned by the chunk tier.
//!
//! Hosts call [`collections`] while preparing their scoped document store,
//! before exposing a TinyCortex instance. This module deliberately performs no
//! I/O so schema ownership stays with the domain and preparation stays with
//! the host.

use tinystoragedrivers_types::{CollectionSpec, IndexSpec};

/// Collections and query indexes required by chunk persistence.
///
/// The returned declarations cover chunk records, per-model embedding
/// metadata, re-embedding skips, raw archive references, and source-ingest
/// gates. Indexes mirror the existing query shapes: source and source-order
/// retrieval, timestamp and owner filtering, embedding lookup by model, and
/// unique source/chunk keys.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(CHUNKS)
            .index(IndexSpec::new(
                "by_source",
                ["metadata.source_kind", "metadata.source_id"],
            ))
            .index(IndexSpec::new("by_timestamp", ["metadata.timestamp"]))
            .index(IndexSpec::new("by_owner", ["metadata.owner"]))
            .index(IndexSpec::new(
                "by_source_sequence",
                [
                    "metadata.source_kind",
                    "metadata.source_id",
                    "seq_in_source",
                ],
            )),
        CollectionSpec::new(CHUNK_VERSIONS).index(IndexSpec::new(
            "by_logical_id_revision",
            ["logical_id", "revision"],
        )),
        CollectionSpec::new(STATE_VERSIONS).index(IndexSpec::new(
            "by_collection_key_revision",
            ["collection", "key", "revision"],
        )),
        CollectionSpec::new(OPERATIONS).index(IndexSpec::new("by_parent", ["parent_op"])),
        CollectionSpec::new(COMMIT_HEADS),
        CollectionSpec::new(CHUNK_EMBEDDINGS)
            .index(IndexSpec::new("by_chunk_model", ["chunk_id", "model_signature"]).unique())
            .index(IndexSpec::new("by_model", ["model_signature"])),
        CollectionSpec::new(CHUNK_REEMBED_SKIPS)
            .index(IndexSpec::new("by_chunk_model", ["chunk_id", "model_signature"]).unique())
            .index(IndexSpec::new("by_model", ["model_signature"])),
        CollectionSpec::new(RAW_REFS).index(IndexSpec::new("by_chunk", ["chunk_id"]).unique()),
        CollectionSpec::new(CHUNK_CONTENT_POINTERS)
            .index(IndexSpec::new("by_chunk", ["chunk_id"]).unique()),
        CollectionSpec::new(INGESTED_SOURCES)
            .index(IndexSpec::new("by_source", ["source_kind", "source_id"]).unique()),
        CollectionSpec::new(SCORES)
            .index(IndexSpec::new("by_chunk", ["chunk_id"]).unique())
            .index(IndexSpec::new("by_total", ["total"]))
            .index(IndexSpec::new("by_dropped", ["dropped"])),
        CollectionSpec::new(ENTITY_INDEX)
            .index(IndexSpec::new("by_entity_node", ["entity_id", "node_id"]).unique())
            .index(IndexSpec::new("by_entity", ["entity_id"]))
            .index(IndexSpec::new("by_node", ["node_id"]))
            .index(IndexSpec::new("by_timestamp", ["timestamp_ms"])),
        CollectionSpec::new(ENTITY_EDGES)
            .index(IndexSpec::new("by_pair", ["entity_a", "entity_b"]).unique())
            .index(IndexSpec::new("by_entity_a", ["entity_a"]))
            .index(IndexSpec::new("by_entity_b", ["entity_b"])),
        CollectionSpec::new(TREES)
            .index(IndexSpec::new("by_kind_scope", ["kind", "scope"]).unique())
            .index(IndexSpec::new("by_status", ["status"])),
        CollectionSpec::new(SUMMARIES)
            .index(IndexSpec::new("by_tree_level", ["tree_id", "level"]))
            .index(IndexSpec::new("by_parent", ["parent_id"]))
            .index(IndexSpec::new("by_sealed", ["sealed_at_ms"]))
            .index(IndexSpec::new("by_deleted", ["deleted"])),
        CollectionSpec::new(BUFFERS)
            .index(IndexSpec::new("by_tree_level", ["tree_id", "level"]).unique())
            .index(IndexSpec::new("by_oldest", ["oldest_at_ms"])),
        CollectionSpec::new(ENTITY_HOTNESS)
            .index(IndexSpec::new("by_entity", ["entity_id"]).unique())
            .index(IndexSpec::new("by_score", ["last_hotness"])),
        CollectionSpec::new(INGEST_OUTBOX).index(IndexSpec::new("by_pending", ["published"])),
    ]
}

/// Stable collection name for persisted chunk records.
pub const CHUNKS: &str = "memory_chunks";
/// Stable collection name for immutable chunk versions.
pub const CHUNK_VERSIONS: &str = "memory_chunk_versions";
/// Stable collection name for immutable non-chunk memory record versions.
pub const STATE_VERSIONS: &str = "memory_state_versions";
/// Stable collection name for immutable memory commit manifests.
pub const OPERATIONS: &str = "memory_commit_operations";
/// Stable collection name for the per-scope commit-chain head.
pub const COMMIT_HEADS: &str = "memory_commit_heads";
/// Stable collection name for per-model embedding metadata.
pub const CHUNK_EMBEDDINGS: &str = "memory_chunk_embeddings";
/// Stable collection name for embedding retry-skip records.
pub const CHUNK_REEMBED_SKIPS: &str = "memory_chunk_reembed_skipped";
/// Stable collection name for staged chunk content pointers.
pub const CHUNK_CONTENT_POINTERS: &str = "memory_chunk_content_pointers";
/// Stable collection name for raw archive pointers associated with chunks.
pub const RAW_REFS: &str = "memory_chunk_raw_refs";
/// Stable collection name for source-ingest idempotency gates.
pub const INGESTED_SOURCES: &str = "memory_ingested_sources";
/// Stable collection name for current chunk lifecycle values.
pub const CHUNK_LIFECYCLE: &str = "memory_chunk_lifecycle";
/// Stable collection name for chunk admission scores.
pub const SCORES: &str = "memory_chunk_scores";
/// Stable collection name for entity-to-node occurrences.
pub const ENTITY_INDEX: &str = "memory_entity_index";
/// Stable collection name for co-occurrence edges.
pub const ENTITY_EDGES: &str = "memory_entity_edges";
/// Stable collection name for summary-tree roots.
pub const TREES: &str = "memory_trees";
/// Stable collection name for summary nodes.
pub const SUMMARIES: &str = "memory_summaries";
/// Stable collection name for unsealed tree buffers.
pub const BUFFERS: &str = "memory_tree_buffers";
/// Stable collection name for graph entity hotness records.
pub const ENTITY_HOTNESS: &str = "memory_entity_hotness";
/// Stable collection name for committed ingest work waiting for queue hints.
pub const INGEST_OUTBOX: &str = "memory_ingest_outbox";

#[cfg(test)]
#[path = "collections_tests.rs"]
mod tests;
