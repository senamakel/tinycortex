//! Chunks — the unit of memory-store persistence.
//!
//! One module for the full chunk lifecycle, ported faithfully from
//! OpenHuman's `memory_store/chunks`:
//!
//! - `types` — [`Chunk`], [`Metadata`], [`SourceKind`], [`DataSource`],
//!   [`SourceRef`], [`StagedChunk`] and the deterministic
//!   [`chunk_id`] / token-estimate functions. The persisted shape.
//! - `produce` — source-kind-dispatch chunker (chat / email / document).
//!   Splits canonical Markdown into bounded chunks with stable per-source
//!   sequence numbers.
//! - `semantic` — heading- and paragraph-aware chunker used to split large
//!   documents into LLM-context-sized pieces while preserving heading context.
//!   Exported as [`chunk_semantic`].
//! - `store` / `versioned` / `collections` — chunks and associated state in
//!   the host document store, exposed through an immutable operation chain
//!   and one compare-and-swap commit head.
//! - `connection` / `migrations` / `recovery` and several transaction helpers
//!   still support transaction-coupled chunks, tree, score, and queue paths
//!   that are being migrated together.
//! - `embeddings` / `embeddings_query` — versioned chunk vectors and
//!   re-embed skips; summary embedding adjuncts remain with the tree store.
//!
//! ## Differences from OpenHuman
//!
//! This module owns chunk records and their source-ingest, embedding, and
//! raw-content adjuncts in the host-supplied document store. Tree, queue, and
//! entity relationships use the same versioned owner head where atomicity is
//! required.

use std::path::PathBuf;

use crate::memory::config::MemoryConfig;

pub(crate) mod collections;

#[path = "embeddings.rs"]
mod embeddings;
#[path = "embeddings_query.rs"]
mod embeddings_query;
mod legacy_import;
#[path = "produce.rs"]
mod produce;
#[path = "produce_split.rs"]
mod produce_split;
#[path = "raw_refs.rs"]
mod raw_refs;
#[path = "semantic.rs"]
mod semantic;
#[path = "signature.rs"]
mod signature;
#[path = "store.rs"]
mod store;
#[path = "store_delete.rs"]
mod store_delete;
mod store_list;
#[path = "store_sources.rs"]
pub(crate) mod store_sources;
pub(crate) mod versioned;

/// The persisted chunk value types now live in the dependency-light
/// `tinycortex-api` crate. Aliasing the whole module (rather than importing
/// the individual items) keeps every `super::types::…` path inside this module
/// tree resolving exactly as before, at the same private visibility the local
/// `mod types;` had.
use tinycortex_api::chunks as types;

#[cfg(test)]
#[path = "store_delete_tests.rs"]
mod store_delete_tests;
#[cfg(test)]
#[path = "store_embed_tests.rs"]
mod store_embed_tests;
#[cfg(test)]
#[path = "store_list_tests.rs"]
mod store_list_tests;
#[cfg(test)]
#[path = "store_tests.rs"]
mod store_tests;
#[cfg(test)]
pub(crate) mod test_support;

// ── Public API surface ───────────────────────────────────────────────────────

pub use produce::{chunk_markdown, ChunkerInput, ChunkerOptions, DEFAULT_CHUNK_MAX_TOKENS};
pub use semantic::{chunk_markdown as chunk_semantic, Chunk as SemanticChunk};
pub use types::{
    approx_token_count, chunk_id, conservative_token_estimate, truncate_to_conservative_tokens,
    Chunk, DataSource, Metadata, SourceKind, SourceRef, StagedChunk,
};

pub use collections::collections;
pub(crate) use embeddings::summary_reembed_skip_key;
pub use embeddings::{
    clear_chunk_reembed_skipped, clear_reembed_skipped_for_signature,
    clear_summary_reembed_skipped, clear_summary_reembed_skipped_async, embedding_to_blob,
    mark_chunk_reembed_skipped, mark_summary_reembed_skipped, mark_summary_reembed_skipped_async,
    plan_chunk_embedding_for_queue, set_chunk_embedding, set_chunk_embedding_for_signature,
    tree_active_signature, REEMBED_SKIP_KEY_MAX_LEN,
};
pub use embeddings_query::{
    get_chunk_embedding, get_chunk_embedding_async, get_chunk_embedding_for_signature,
    get_chunk_embedding_for_signature_async, get_chunk_embeddings_batch,
    get_chunk_embeddings_batch_async, get_chunk_embeddings_for_signature_batch,
    get_chunk_embeddings_for_signature_batch_async, has_uncovered_reembed_work,
    has_uncovered_reembed_work_async,
};
pub use legacy_import::{
    import_legacy_batch, LegacyChunkRecord, LegacyContentPointer, LegacyEmbedding,
    LegacyEntityEdge, LegacyEntityOccurrence, LegacyImportBatch, LegacyImportCounts,
    LegacySourceGate, LegacySummaryContent, LegacySummaryEmbedding,
};
pub use raw_refs::{
    get_chunk_content_path, get_chunk_content_pointers, get_chunk_raw_refs,
    get_chunk_raw_refs_async, get_summary_content_pointers, get_summary_content_pointers_async,
    list_chunk_raw_ref_paths_with_prefix, list_chunk_raw_ref_paths_with_prefix_async,
    list_summaries_with_content_path, set_chunk_raw_refs, RawRef,
};
pub use signature::{
    format_signature, parse_signature, signature_variants, signatures_equivalent, SignatureParts,
};
pub use store::{
    claim_source_ingest, count_chunks, count_chunks_by_lifecycle_status,
    count_raw_paths_ingested_with_prefix, delete_source_ingest, extraction_coverage,
    extraction_coverage_async, filter_raw_paths_not_ingested, get_chunk, get_chunk_async,
    get_chunk_lifecycle_status, get_chunks_batch, get_chunks_batch_async, is_source_ingested,
    list_source_ids_with_prefix, mark_raw_paths_ingested, set_chunk_lifecycle_status,
    update_chunk_content_sha256, update_summary_content_sha256,
    update_summary_content_sha256_async, upsert_chunks, upsert_chunks_async,
    upsert_staged_chunks, CHUNK_STATUS_ADMITTED, CHUNK_STATUS_BUFFERED,
    CHUNK_STATUS_DROPPED, CHUNK_STATUS_PENDING_EXTRACTION, CHUNK_STATUS_SEALED, RAW_FILE_GATE_KIND,
};
pub use store_delete::{
    delete_chunk_by_id, delete_chunk_by_id_async, delete_chunks_by_owner,
    delete_chunks_by_owner_async, delete_chunks_by_source, delete_chunks_by_source_async,
    delete_chunks_by_source_prefix, delete_chunks_by_source_prefix_async,
    delete_orphaned_source_tree, delete_orphaned_source_tree_async, purge_all, purge_all_async,
};
pub(crate) use store_delete::remove_unreferenced_content_files_async;
pub use store_list::{
    count_chunks_matching, list_chunk_details, list_chunks, source_totals, ChunkDetailRow,
    ListChunksQuery, SourceTotal,
};

#[cfg(test)]
#[path = "embedding_plan_tests.rs"]
mod embedding_plan_tests;

// ── Shared internal constants / helpers ─────────────────────────────────────

/// Default subdirectory used for memory content files.
pub(crate) const DB_DIR: &str = "memory_tree";
/// Root directory for on-disk chunk and summary content files.
pub(crate) fn content_root(config: &MemoryConfig) -> PathBuf {
    config
        .content_root
        .clone()
        .unwrap_or_else(|| config.workspace.join(DB_DIR).join("content"))
}

/// Redact a PII-bearing string for log output by hashing it to 8 hex chars.
/// Stable across runs for the same input (so it stays greppable) but never
/// reveals the original value (source ids and content paths can embed emails).
#[allow(dead_code)]
pub(crate) fn redact(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let d = h.finalize();
    format!("{:08x}", u32::from_be_bytes([d[0], d[1], d[2], d[3]]))
}
