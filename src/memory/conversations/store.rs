//! Stream-backed conversation persistence.
//!
//! Per-thread streams are authoritative. The document collection is only a
//! rebuildable metadata projection; stream appends and projection updates are
//! intentionally not described as one cross-port transaction.

use std::path::PathBuf;
use std::sync::LazyLock;

mod async_ops;
mod common;
use common::{decode_stream_id, hex, infer_labels, normalize_labels};
mod list_ops;
mod search_ops;
mod sync_ops;
pub use sync_ops::*;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_types::{Capability, ErrorKind, Precondition, WriteOptions};

use super::collections::THREAD_INDEX;
use super::inverted_index::InvertedIndex;
use super::types::{
    is_deterministic_message_id, ConversationMessage, ConversationMessagePatch, ConversationThread,
    CreateConversationThread, CrossThreadHit, LegacyConversationTranscript,
};

const STREAM_PREFIX: &str = "tinycortex:conversation:";
const STREAM_PAGE: usize = 512;
const MAX_WRITE_ATTEMPTS: usize = 128;

// The old store serialized thread creation and purge process-wide. Keep that
// lifecycle exclusion across independently opened handles while stream and
// document ports remain separate operations.
static LIFECYCLE_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Counts returned by [`ConversationStore::purge_threads`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConversationPurgeStats {
    /// Number of live threads tombstoned.
    pub thread_count: usize,
    /// Number of messages in those threads at the time they were read.
    pub message_count: usize,
}

/// A conversation store bound to prepared host storage.
#[derive(Debug, Clone)]
pub struct ConversationStore {
    storage: Storage,
    blocking: crate::memory::storage::Blocking,
    workspace: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    ThreadUpsert {
        thread: ConversationThread,
        updated_at: String,
    },
    ThreadDelete {
        deleted_at: String,
    },
    MessageAppend {
        message: ConversationMessage,
    },
    MessageImport {
        message: ConversationMessage,
    },
    TranscriptCheckpoint {
        thread: ConversationThread,
        updated_at: String,
        messages: Vec<ConversationMessage>,
    },
}

enum AppendFailure {
    Conflict,
    Other(String),
}

impl AppendFailure {
    fn message(self) -> String {
        match self {
            Self::Conflict => "conversation stream changed too often".into(),
            Self::Other(message) => message,
        }
    }
}

#[derive(Debug, Clone)]
struct ThreadSnapshot {
    thread: Option<ConversationThread>,
    updated_at: String,
    messages: Vec<ConversationMessage>,
    offset: u64,
    tombstone_offset: Option<u64>,
    checkpoint_offset: Option<u64>,
}

impl ConversationStore {
    /// Bind the store to an opened configuration. The host must have prepared
    /// its collections before this handle is created.
    pub fn open(config: &crate::memory::config::MemoryConfig) -> anyhow::Result<Self> {
        Ok(Self {
            storage: config.storage()?.clone(),
            blocking: config.storage_blocking()?.clone(),
            workspace: config.workspace.clone(),
        })
    }

    /// Workspace associated with the host's storage scope.
    pub fn workspace(&self) -> &std::path::Path {
        &self.workspace
    }

    fn stream_name(thread_id: &str) -> String {
        format!("{STREAM_PREFIX}{}", hex(thread_id.as_bytes()))
    }

    fn document_id(thread_id: &str) -> String {
        hex(thread_id.as_bytes())
    }

    async fn read_stream(&self, thread_id: &str) -> Result<(Vec<(u64, Event)>, u64), String> {
        let streams = self.storage.streams();
        let name = Self::stream_name(thread_id);
        // Streams are append-only, so fixing the end before paging gives this
        // read one coherent prefix even while another writer appends.
        let length = streams
            .len(&name)
            .await
            .map_err(|error| error.to_string())?;
        let mut cursor = 0_u64;
        let mut events = Vec::new();
        while cursor < length {
            let remaining = usize::try_from(length - cursor).unwrap_or(usize::MAX);
            let page = streams
                .read_window(&name, cursor, remaining.min(STREAM_PAGE))
                .await
                .map_err(|error| error.to_string())?;
            if page.is_empty() {
                return Err(format!(
                    "conversation stream {thread_id} ended before captured offset {length}"
                ));
            }
            for entry in page {
                let event = serde_json::from_value(entry.value)
                    .map_err(|error| format!("decode conversation stream: {error}"))?;
                if events.is_empty() && cursor == 0 && entry.offset > 0 {
                    if !matches!(
                        event,
                        Event::ThreadDelete { .. } | Event::TranscriptCheckpoint { .. }
                    ) {
                        return Err(format!(
                            "conversation stream {thread_id} has an unmarked truncated prefix"
                        ));
                    }
                    cursor = entry.offset;
                }
                if entry.offset != cursor {
                    return Err(format!(
                        "conversation stream {thread_id} returned offset {}, expected {cursor}",
                        entry.offset
                    ));
                }
                events.push((entry.offset, event));
                cursor = entry.offset.saturating_add(1);
            }
        }
        Ok((events, length))
    }

    fn fold(thread_id: &str, events: &[(u64, Event)], offset: u64) -> ThreadSnapshot {
        let mut thread: Option<ConversationThread> = None;
        let mut updated_at = String::new();
        let mut messages: Vec<ConversationMessage> = Vec::new();
        let mut tombstone_offset = None;
        let mut checkpoint_offset = None;
        for (event_offset, event) in events {
            match event {
                Event::ThreadUpsert {
                    thread: next,
                    updated_at: at,
                } => {
                    let mut next = next.clone();
                    if let Some(previous) = &thread {
                        next.created_at = previous.created_at.clone();
                        if next.labels.is_empty() {
                            next.labels = previous.labels.clone();
                        }
                        if next.parent_thread_id.is_none() {
                            next.parent_thread_id = previous.parent_thread_id.clone();
                        }
                        if next.personality_id.is_none() {
                            next.personality_id = previous.personality_id.clone();
                        }
                    } else if next.labels.is_empty() {
                        next.labels = super::store::infer_labels(thread_id);
                    }
                    next.message_count = messages.len();
                    next.last_message_at = messages
                        .last()
                        .map(|message| message.created_at.clone())
                        .unwrap_or_else(|| next.created_at.clone());
                    next.is_active = true;
                    thread = Some(next);
                    updated_at = at.clone();
                    tombstone_offset = None;
                }
                Event::ThreadDelete { .. } => {
                    thread = None;
                    messages.clear();
                    tombstone_offset = Some(*event_offset);
                }
                Event::MessageAppend { message } => {
                    if !is_deterministic_message_id(&message.id)
                        || !messages.iter().any(|existing| existing.id == message.id)
                    {
                        messages.push(message.clone());
                    }
                }
                Event::MessageImport { message } => messages.push(message.clone()),
                Event::TranscriptCheckpoint {
                    thread: next,
                    updated_at: at,
                    messages: retained,
                } => {
                    thread = Some(next.clone());
                    updated_at = at.clone();
                    messages = retained.clone();
                    tombstone_offset = None;
                    checkpoint_offset = Some(*event_offset);
                }
            }
        }
        if let Some(current) = &mut thread {
            current.message_count = messages.len();
            current.last_message_at = messages
                .last()
                .map(|message| message.created_at.clone())
                .unwrap_or_else(|| current.created_at.clone());
        }
        ThreadSnapshot {
            thread,
            updated_at,
            messages,
            offset,
            tombstone_offset,
            checkpoint_offset,
        }
    }

    async fn snapshot(&self, thread_id: &str) -> Result<ThreadSnapshot, String> {
        let (events, length) = self.read_stream(thread_id).await?;
        Ok(Self::fold(thread_id, &events, length))
    }

    async fn append_event_at(
        &self,
        thread_id: &str,
        expected_offset: u64,
        event: Event,
    ) -> Result<u64, AppendFailure> {
        self.append_events_at(thread_id, expected_offset, vec![event])
            .await
    }

    async fn append_events_at(
        &self,
        thread_id: &str,
        expected_offset: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendFailure> {
        let streams = self.storage.streams();
        if !streams
            .capabilities()
            .contains(Capability::ConditionalStreamAppend)
        {
            return Err(AppendFailure::Other(
                "conversation writes require ConditionalStreamAppend storage capability".into(),
            ));
        }
        let values = events
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| AppendFailure::Other(error.to_string()))?;
        streams
            .append_batch_if_offset(&Self::stream_name(thread_id), expected_offset, values)
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::Conflict {
                    AppendFailure::Conflict
                } else {
                    AppendFailure::Other(error.to_string())
                }
            })
    }

    /// Import one legacy thread and its ordered messages in a single
    /// conditional stream append. Retrying the same logical transcript is
    /// idempotent; a different existing transcript is rejected. Hosts own
    /// legacy file reading and call this destination API after quiescing their
    /// source files. Separate threads are separate streams, so a multi-thread
    /// import is not a cross-stream transaction.
    pub async fn import_legacy_transcript_async(
        &self,
        transcript: LegacyConversationTranscript,
    ) -> Result<(), String> {
        let _lifecycle = LIFECYCLE_LOCK.lock().await;
        let LegacyConversationTranscript {
            thread,
            updated_at,
            messages,
        } = transcript;
        if thread.id.trim().is_empty() {
            return Err("legacy conversation thread ID is empty".into());
        }
        let mut expected_thread = thread.clone();
        expected_thread.is_active = true;
        expected_thread.message_count = messages.len();
        expected_thread.last_message_at = messages
            .last()
            .map(|message| message.created_at.clone())
            .unwrap_or_else(|| thread.created_at.clone());
        expected_thread.labels = if thread.labels.is_empty() {
            infer_labels(&thread.id)
        } else {
            normalize_labels(thread.labels.clone())
        };

        for _ in 0..MAX_WRITE_ATTEMPTS {
            let current = self.snapshot(&thread.id).await?;
            if current.thread.is_some() {
                if current.thread.as_ref() == Some(&expected_thread)
                    && current.updated_at == updated_at
                    && current.messages == messages
                {
                    self.project(&thread.id, &current).await?;
                    return Ok(());
                }
                return Err(format!(
                    "legacy conversation {} conflicts with existing data",
                    thread.id
                ));
            }
            if current.tombstone_offset.is_some() {
                return Err(format!(
                    "legacy conversation {} was deleted previously",
                    thread.id
                ));
            }
            if !current.messages.is_empty() {
                return Err(format!(
                    "legacy conversation {} conflicts with existing transcript data",
                    thread.id
                ));
            }
            let mut events = Vec::with_capacity(messages.len() + 1);
            events.push(Event::ThreadUpsert {
                thread: expected_thread.clone(),
                updated_at: updated_at.clone(),
            });
            events.extend(
                messages
                    .iter()
                    .cloned()
                    .map(|message| Event::MessageImport { message }),
            );
            match self
                .append_events_at(&thread.id, current.offset, events)
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let committed = self.snapshot(&thread.id).await?;
                    self.project(&thread.id, &committed).await?;
                    return Ok(());
                }
            }
        }
        Err("conversation stream changed too often during legacy import".into())
    }

    async fn project(&self, thread_id: &str, snapshot: &ThreadSnapshot) -> Result<(), String> {
        let documents = self.storage.documents();
        let id = Self::document_id(thread_id);
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let current = documents
                .get(THREAD_INDEX, &id)
                .await
                .map_err(|error| error.to_string())?;
            if current.as_ref().is_some_and(|record| {
                let current_offset = record.doc["stream_offset"].as_u64().unwrap_or(0);
                if snapshot.thread.is_some() {
                    current_offset >= snapshot.offset
                } else {
                    // A tombstone at the same stream offset must still remove
                    // the projection. A strictly newer projection belongs to
                    // a later recreation and must be preserved.
                    current_offset > snapshot.offset
                }
            }) {
                return Ok(());
            }
            let Some(thread) = &snapshot.thread else {
                if let Some(current) = current {
                    match documents
                        .delete(THREAD_INDEX, &id, Precondition::Version(current.version))
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(error) if error.kind() == ErrorKind::Conflict => continue,
                        Err(error) => return Err(error.to_string()),
                    }
                }
                return Ok(());
            };
            let body = json!({
                "thread_id": thread_id,
                "title": thread.title,
                "created_at": thread.created_at,
                "updated_at": snapshot.updated_at,
                "parent_thread_id": thread.parent_thread_id,
                "labels": thread.labels,
                "personality_id": thread.personality_id,
                "message_count": thread.message_count,
                "last_message_at": thread.last_message_at,
                "stream_offset": snapshot.offset,
            });
            let precondition = current
                .as_ref()
                .map(|record| Precondition::Version(record.version))
                .unwrap_or(Precondition::Absent);
            match documents
                .put_with(
                    THREAD_INDEX,
                    &id,
                    body,
                    precondition,
                    WriteOptions::default(),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("conversation projection changed too often".into())
    }
}

#[cfg(test)]
#[path = "store_import_tests.rs"]
mod import_tests;
#[cfg(test)]
#[path = "store_lifecycle_tests.rs"]
mod lifecycle_tests;
#[cfg(test)]
#[path = "store_port_tests.rs"]
mod port_tests;
