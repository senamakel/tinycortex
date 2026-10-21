//! Memory contracts and storage primitives.
//!
//! The starter in-memory store (`store` / `types`) provides the simple
//! reference backend used by the smoke test. The richer storage primitives
//! ported from OpenHuman live in the submodules below:
//!
//! - [`content`]       — markdown content store (YAML front-matter, atomic
//!   writes, `content_path`/`content_sha256` pointers, Obsidian-vault paths).
//! - [`vectors`]       — host-document-backed packed-`f32` cosine vector store.
//! - [`kv`]            — host-port global + namespace JSON key-value store.
//! - [`entity_index`] — entity occurrence index over tree nodes.
//! - [`safety`]       — secret / PII detection guard used by KV writes.
//!
//! ## Concurrency contract
//!
//! Persistent collections are supplied by the host through the storage ports;
//! these domain stores do not open their own database connections. KV writes
//! use optimistic compare-and-swap.

/// Markdown content store: source-of-truth files with YAML front matter,
/// atomic writes, and `content_path`/`content_sha256` provenance pointers.
pub mod content;
/// Entity occurrence index over tree nodes, backing co-occurrence graph queries.
pub mod entity_index;
/// Global + namespace-scoped JSON key-value store (writes pass the [`safety`] guard).
pub mod kv;
mod memory_trait;
/// Secret / PII detection guard applied before KV writes are persisted.
pub mod safety;
/// Opaque engine-owned values stored without user-memory PII rewriting.
#[cfg(any(feature = "persona", feature = "sync"))]
pub(crate) mod state_kv;
/// Starter in-memory reference backend used by the smoke test.
#[allow(clippy::module_inception)]
pub mod store;
/// Core memory contract types ([`StoreError`], [`MemoryRecord`], etc.).
pub mod types;
/// Host-document-backed packed-`f32` cosine vector store and embedding backends.
pub mod vectors;

pub use store::{InMemoryMemoryStore, MemoryStore};
pub use types::{
    MemoryId, MemoryInput, MemoryQuery, MemoryRecord, MemoryResult, SearchHit, StoreError,
};

// ── Ported storage-primitive re-exports ─────────────────────────────────────
pub use entity_index::{CanonicalEntity, EntityHit, EntityIndex, EntityKind, SelfIdentity};
pub use kv::KvStore;
pub use vectors::{
    bytes_to_vec, cosine_similarity, vec_to_bytes, EmbeddingBackend, InertEmbedding, SearchResult,
    VectorStore,
};

/// Score a case-insensitive whitespace-term query against content.
///
/// Empty queries match fully; content with no matching term is excluded.
pub(super) fn query_match_score(content: &str, query: &str) -> Option<f32> {
    let terms = query
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return Some(1.0);
    }
    let content = content.to_lowercase();
    let matched = terms
        .iter()
        .filter(|term| content.contains(term.as_str()))
        .count();
    (matched != 0).then_some(matched as f32 / terms.len() as f32)
}
