//! Tree, summary, buffer, embedding and hotness persistence over the
//! host-prepared, scope-bound versioned document owner. Multi-record tree
//! operations stage changes and publish them through one owner-head CAS so
//! readers observe a coherent commit.

mod buffers;
mod common;
pub mod hotness;
mod summaries;
mod trees;
pub mod types;

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;

// ── Tree rows ───────────────────────────────────────────────────────────────
pub(crate) use trees::stage_tree_cascade_deletion_async;
pub use trees::{
    archive_tree, archive_tree_async, delete_tree_cascade_async, delete_tree_cascade_tx, get_tree,
    get_tree_async, get_tree_by_scope, get_tree_by_scope_async, get_tree_by_scope_conn,
    get_trees_batch, get_trees_batch_async, insert_tree, insert_tree_async, insert_tree_conn,
    list_trees_by_kind, list_trees_by_kind_async, refresh_last_sealed_tx,
    update_tree_after_seal_tx, TreeCascadeDeletion,
};

// ── Summary rows + embeddings ───────────────────────────────────────────────
pub(crate) use summaries::stage_summary_changes;
pub use summaries::{
    count_summaries, count_summaries_async, get_summaries_batch, get_summaries_batch_async,
    get_summary, get_summary_async, get_summary_embedding, get_summary_embedding_async,
    get_summary_embedding_for_signature, get_summary_embedding_for_signature_async,
    get_summary_embeddings_batch, get_summary_embeddings_batch_async,
    get_summary_embeddings_for_signature_batch, get_summary_embeddings_for_signature_batch_async,
    insert_staged_summary_tx, insert_summary_tx, list_children_of_summary,
    list_children_of_summary_async, list_summaries_at_level, list_summaries_at_level_async,
    list_summaries_in_window, list_summaries_in_window_async, list_summaries_overlapping_window,
    list_summaries_overlapping_window_async, set_summary_embedding,
    set_summary_embedding_for_signature,
};

// ── Buffers ─────────────────────────────────────────────────────────────────
pub(crate) use buffers::{
    append_buffer_item, append_buffer_item_async, consume_snapshot_tx, get_buffer_async,
    stage_buffer_item_async,
};
pub use buffers::{
    clear_buffer_tx, get_buffer, get_buffer_conn, list_stale_buffers, list_stale_buffers_async,
    upsert_buffer_tx,
};

// ── Type + constant re-exports ──────────────────────────────────────────────
pub use types::{
    Buffer, EntityIndexStats, HotnessCounters, SummaryNode, Tree, TreeKind, TreeStatus,
    DEFAULT_FLUSH_AGE_SECS, INPUT_TOKEN_BUDGET, OUTPUT_TOKEN_BUDGET, SUMMARY_FANOUT,
    TOPIC_ARCHIVE_THRESHOLD, TOPIC_CREATION_THRESHOLD, TOPIC_RECHECK_EVERY,
};
pub(crate) use types::TreeInsertConflict;
