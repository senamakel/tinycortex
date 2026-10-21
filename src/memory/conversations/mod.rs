//! Conversation transcripts backed by host-provided storage ports.
//!
//! Each thread has an append-only [`StreamStore`](tinystoragedrivers_api::StreamStore)
//! containing the authoritative thread and message history. A declared
//! [`DocumentStore`](tinystoragedrivers_api::DocumentStore) collection holds a
//! rebuildable thread-list projection. These ports are supplied and prepared by
//! the host through [`MemoryConfig`](crate::memory::config::MemoryConfig);
//! this module has no filesystem or in-memory persistence fallback.
//!
//! This is transcript persistence, not semantic indexing: it owns raw thread
//! and message records and builds the local multilingual inverted index used
//! for cross-thread substring search. Transcript archival into summary trees
//! lives in `super::archivist`.
//!
//! The event bus remains abstracted behind [`ConversationEventBus`] and
//! [`ChannelEventHandler`], so this crate has no dependency on a host's
//! concrete channel event type.

mod bus;
pub mod collections;
mod inverted_index;
mod store;
mod tokenize;
mod types;

pub use bus::{
    register_conversation_persistence_subscriber, ChannelEvent, ChannelEventHandler,
    ConversationEventBus, ConversationPersistenceSubscriber,
};
pub use collections::collections;
pub use store::{
    append_message, append_message_idempotent_async, delete_messages_from, delete_thread,
    ensure_thread, get_messages, list_threads, purge_threads, update_message, update_thread_labels,
    update_thread_title, ConversationPurgeStats, ConversationStore,
};
pub use types::{
    is_deterministic_message_id, reply_run_id, run_reply_message_id, ConversationMessage,
    ConversationMessagePatch, ConversationThread, CreateConversationThread, CrossThreadHit,
    LegacyConversationTranscript,
};
