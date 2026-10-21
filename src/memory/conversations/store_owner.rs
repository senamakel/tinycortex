//! Stream-backed conversation persistence.
//!
//! Per-thread streams are authoritative. The document collection is only a
//! rebuildable metadata projection; stream appends and projection updates are
//! intentionally not described as one cross-port transaction.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_types::{Capability, ErrorKind, Precondition, WriteOptions};

use super::collections::THREAD_INDEX;
use super::inverted_index::InvertedIndex;
use super::types::{
    is_deterministic_message_id, ConversationMessage, ConversationMessagePatch, ConversationThread,
    CreateConversationThread, CrossThreadHit,
};

const STREAM_PREFIX: &str = "tinycortex:conversation:";
const STREAM_PAGE: usize = 512;
const MAX_WRITE_ATTEMPTS: usize = 128;

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
    MessageReplace {
        message: ConversationMessage,
    },
    MessagesTruncate {
        from_message_id: String,
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

    async fn read_stream(&self, thread_id: &str) -> Result<(Vec<Event>, u64), String> {
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
                if entry.offset != cursor {
                    return Err(format!(
                        "conversation stream {thread_id} returned offset {}, expected {cursor}",
                        entry.offset
                    ));
                }
                events.push(
                    serde_json::from_value(entry.value)
                        .map_err(|error| format!("decode conversation stream: {error}"))?,
                );
                cursor = entry.offset.saturating_add(1);
            }
        }
        Ok((events, length))
    }

    fn fold(thread_id: &str, events: &[Event], offset: u64) -> ThreadSnapshot {
        let mut thread: Option<ConversationThread> = None;
        let mut updated_at = String::new();
        let mut messages: Vec<ConversationMessage> = Vec::new();
        for event in events {
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
                }
                Event::ThreadDelete { .. } => {
                    thread = None;
                    messages.clear();
                }
                Event::MessageAppend { message } => {
                    if !messages.iter().any(|existing| existing.id == message.id) {
                        messages.push(message.clone());
                    }
                }
                Event::MessageReplace { message } => {
                    if let Some(existing) = messages.iter_mut().find(|item| item.id == message.id) {
                        *existing = message.clone();
                    }
                }
                Event::MessagesTruncate { from_message_id } => {
                    if let Some(index) =
                        messages.iter().position(|item| item.id == *from_message_id)
                    {
                        messages.truncate(index);
                    }
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
        let streams = self.storage.streams();
        if !streams
            .capabilities()
            .contains(Capability::ConditionalStreamAppend)
        {
            return Err(AppendFailure::Other(
                "conversation writes require ConditionalStreamAppend storage capability".into(),
            ));
        }
        let value =
            serde_json::to_value(event).map_err(|error| AppendFailure::Other(error.to_string()))?;
        streams
            .append_batch_if_offset(&Self::stream_name(thread_id), expected_offset, vec![value])
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::Conflict {
                    AppendFailure::Conflict
                } else {
                    AppendFailure::Other(error.to_string())
                }
            })
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
                record.doc["stream_offset"].as_u64().unwrap_or(0) >= snapshot.offset
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

    /// Create or touch a thread. The event stream is durable before the
    /// derived document projection is updated; a failed projection is repaired
    /// by the next operation that projects the thread.
    pub async fn ensure_thread_async(
        &self,
        request: CreateConversationThread,
    ) -> Result<ConversationThread, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(&request.id).await?;
            let mut thread = before.thread.clone().unwrap_or_else(|| ConversationThread {
                id: request.id.clone(),
                title: request.title.clone(),
                chat_id: None,
                is_active: true,
                message_count: before.messages.len(),
                last_message_at: before
                    .messages
                    .last()
                    .map(|message| message.created_at.clone())
                    .unwrap_or_else(|| request.created_at.clone()),
                created_at: request.created_at.clone(),
                parent_thread_id: request.parent_thread_id.clone(),
                labels: request
                    .labels
                    .clone()
                    .map(super::store::normalize_labels)
                    .unwrap_or_else(|| super::store::infer_labels(&request.id)),
                personality_id: request.personality_id.clone(),
            });
            thread.title = request.title.clone();
            if let Some(parent) = &request.parent_thread_id {
                thread.parent_thread_id = Some(parent.clone());
            }
            if let Some(labels) = &request.labels {
                thread.labels = super::store::normalize_labels(labels.clone());
            }
            if let Some(personality) = &request.personality_id {
                thread.personality_id = Some(personality.clone());
            }
            thread.is_active = true;
            match self
                .append_event_at(
                    &request.id,
                    before.offset,
                    Event::ThreadUpsert {
                        thread: thread.clone(),
                        updated_at: request.created_at.clone(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let snapshot = self.snapshot(&request.id).await?;
                    self.project(&request.id, &snapshot).await?;
                    return Ok(snapshot.thread.unwrap_or(thread));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// List live threads in descending update time, matching the legacy
    /// conversation list ordering.
    pub async fn list_threads_async(&self) -> Result<Vec<ConversationThread>, String> {
        let names = self
            .storage
            .streams()
            .streams(STREAM_PREFIX)
            .await
            .map_err(|error| error.to_string())?;
        let mut threads = Vec::new();
        for name in names {
            let Some(thread_id) = decode_stream_id(&name) else {
                continue;
            };
            let snapshot = self.snapshot(&thread_id).await?;
            if let Some(thread) = snapshot.thread {
                threads.push((snapshot.updated_at, thread));
            }
        }
        threads.sort_by(|left, right| {
            right
                .1
                .last_message_at
                .cmp(&left.1.last_message_at)
                .then_with(|| left.1.id.cmp(&right.1.id))
                .then_with(|| right.0.cmp(&left.0))
        });
        Ok(threads.into_iter().map(|(_, thread)| thread).collect())
    }

    /// Read messages in their append order.
    pub async fn get_messages_async(
        &self,
        thread_id: &str,
    ) -> Result<Vec<ConversationMessage>, String> {
        Ok(self.snapshot(thread_id).await?.messages)
    }

    /// Append one message. Deterministic message ids are deduplicated against
    /// the stream's current contents; arbitrary client ids retain append
    /// semantics.
    pub async fn append_message_async(
        &self,
        thread_id: &str,
        message: ConversationMessage,
    ) -> Result<ConversationMessage, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(thread_id).await?;
            if before.thread.is_none() {
                return Err(format!("thread {thread_id} not found"));
            }
            if is_deterministic_message_id(&message.id) {
                if let Some(existing) = before.messages.iter().find(|item| item.id == message.id) {
                    return Ok(existing.clone());
                }
            }
            match self
                .append_event_at(
                    thread_id,
                    before.offset,
                    Event::MessageAppend {
                        message: message.clone(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let snapshot = self.snapshot(thread_id).await?;
                    self.project(thread_id, &snapshot).await?;
                    return Ok(snapshot
                        .messages
                        .iter()
                        .find(|item| item.id == message.id)
                        .cloned()
                        .unwrap_or(message));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Substring search over live conversation messages.
    pub async fn search_cross_thread_messages_async(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> Result<Vec<CrossThreadHit>, String> {
        let names = self
            .storage
            .streams()
            .streams(STREAM_PREFIX)
            .await
            .map_err(|error| error.to_string())?;
        let mut index = InvertedIndex::new();
        for name in names {
            if let Some(thread_id) = decode_stream_id(&name) {
                let snapshot = self.snapshot(&thread_id).await?;
                if snapshot.thread.is_some() {
                    for message in snapshot.messages {
                        index.insert(&thread_id, message);
                    }
                }
            }
        }
        Ok(index.search(query, limit, exclude_thread_id))
    }

    /// Update a thread title while preserving its other metadata.
    pub async fn update_thread_title_async(
        &self,
        thread_id: &str,
        title: &str,
        updated_at: &str,
    ) -> Result<ConversationThread, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(thread_id).await?;
            let mut thread = before
                .thread
                .clone()
                .ok_or_else(|| format!("thread {thread_id} not found"))?;
            thread.title = title.to_string();
            match self
                .append_event_at(
                    thread_id,
                    before.offset,
                    Event::ThreadUpsert {
                        thread,
                        updated_at: updated_at.to_string(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return latest
                        .thread
                        .ok_or_else(|| format!("thread {thread_id} missing after update"));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Replace a thread's label set.
    pub async fn update_thread_labels_async(
        &self,
        thread_id: &str,
        labels: Vec<String>,
        updated_at: &str,
    ) -> Result<ConversationThread, String> {
        let labels = super::store::normalize_labels(labels);
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(thread_id).await?;
            let mut thread = before
                .thread
                .clone()
                .ok_or_else(|| format!("thread {thread_id} not found"))?;
            thread.labels = labels.clone();
            match self
                .append_event_at(
                    thread_id,
                    before.offset,
                    Event::ThreadUpsert {
                        thread,
                        updated_at: updated_at.to_string(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return latest
                        .thread
                        .ok_or_else(|| format!("thread {thread_id} missing after update"));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Patch one message's metadata.
    pub async fn update_message_async(
        &self,
        thread_id: &str,
        message_id: &str,
        patch: ConversationMessagePatch,
    ) -> Result<ConversationMessage, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let snapshot = self.snapshot(thread_id).await?;
            let mut message = snapshot
                .messages
                .iter()
                .find(|item| item.id == message_id)
                .cloned()
                .ok_or_else(|| format!("message {message_id} not found in thread {thread_id}"))?;
            if let Some(extra_metadata) = &patch.extra_metadata {
                message.extra_metadata = extra_metadata.clone();
            }
            match self
                .append_event_at(
                    thread_id,
                    snapshot.offset,
                    Event::MessageReplace {
                        message: message.clone(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return Ok(message);
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Remove `message_id` and every later message from the logical transcript.
    pub async fn delete_messages_from_async(
        &self,
        thread_id: &str,
        message_id: &str,
    ) -> Result<Option<usize>, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let snapshot = self.snapshot(thread_id).await?;
            let Some(index) = snapshot
                .messages
                .iter()
                .position(|item| item.id == message_id)
            else {
                return Ok(None);
            };
            let removed = snapshot.messages.len() - index;
            match self
                .append_event_at(
                    thread_id,
                    snapshot.offset,
                    Event::MessagesTruncate {
                        from_message_id: message_id.to_string(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return Ok(Some(removed));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Tombstone a thread without deleting its stream history.
    pub async fn delete_thread_async(
        &self,
        thread_id: &str,
        deleted_at: &str,
    ) -> Result<bool, String> {
        for _ in 0..8 {
            let before = self.snapshot(thread_id).await?;
            if before.thread.is_none() {
                return Ok(false);
            }
            match self
                .append_event_at(
                    thread_id,
                    before.offset,
                    Event::ThreadDelete {
                        deleted_at: deleted_at.to_string(),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return Ok(true);
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Tombstone every live thread. Stream history remains the recovery source.
    pub async fn purge_threads_async(&self) -> Result<ConversationPurgeStats, String> {
        let threads = self.list_threads_async().await?;
        let mut stats = ConversationPurgeStats::default();
        for thread in threads {
            stats.thread_count += 1;
            stats.message_count += thread.message_count;
            self.delete_thread_async(&thread.id, &thread.last_message_at)
                .await?;
        }
        Ok(stats)
    }

    pub fn ensure_thread(
        &self,
        request: CreateConversationThread,
    ) -> Result<ConversationThread, String> {
        let store = self.clone();
        self.blocking
            .run(async move { store.ensure_thread_async(request).await })
            .map_err(|error| error.to_string())?
    }

    pub fn list_threads(&self) -> Result<Vec<ConversationThread>, String> {
        let store = self.clone();
        self.blocking
            .run(async move { store.list_threads_async().await })
            .map_err(|error| error.to_string())?
    }

    pub fn get_messages(&self, thread_id: &str) -> Result<Vec<ConversationMessage>, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        self.blocking
            .run(async move { store.get_messages_async(&thread_id).await })
            .map_err(|error| error.to_string())?
    }

    pub fn append_message(
        &self,
        thread_id: &str,
        message: ConversationMessage,
    ) -> Result<ConversationMessage, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        self.blocking
            .run(async move { store.append_message_async(&thread_id, message).await })
            .map_err(|error| error.to_string())?
    }

    pub fn search_cross_thread_messages(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> Result<Vec<CrossThreadHit>, String> {
        let store = self.clone();
        let query = query.to_string();
        let exclude_thread_id = exclude_thread_id.map(str::to_string);
        self.blocking
            .run(async move {
                store
                    .search_cross_thread_messages_async(&query, limit, exclude_thread_id.as_deref())
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    pub fn update_thread_title(
        &self,
        thread_id: &str,
        title: &str,
        updated_at: &str,
    ) -> Result<ConversationThread, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        let title = title.to_string();
        let updated_at = updated_at.to_string();
        self.blocking
            .run(async move {
                store
                    .update_thread_title_async(&thread_id, &title, &updated_at)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    pub fn update_thread_labels(
        &self,
        thread_id: &str,
        labels: Vec<String>,
        updated_at: &str,
    ) -> Result<ConversationThread, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        let updated_at = updated_at.to_string();
        self.blocking
            .run(async move {
                store
                    .update_thread_labels_async(&thread_id, labels, &updated_at)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    pub fn update_message(
        &self,
        thread_id: &str,
        message_id: &str,
        patch: ConversationMessagePatch,
    ) -> Result<ConversationMessage, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        let message_id = message_id.to_string();
        self.blocking
            .run(async move {
                store
                    .update_message_async(&thread_id, &message_id, patch)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    pub fn delete_messages_from(
        &self,
        thread_id: &str,
        message_id: &str,
    ) -> Result<Option<usize>, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        let message_id = message_id.to_string();
        self.blocking
            .run(async move {
                store
                    .delete_messages_from_async(&thread_id, &message_id)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    pub fn delete_thread(&self, thread_id: &str, deleted_at: &str) -> Result<bool, String> {
        let store = self.clone();
        let thread_id = thread_id.to_string();
        let deleted_at = deleted_at.to_string();
        self.blocking
            .run(async move { store.delete_thread_async(&thread_id, &deleted_at).await })
            .map_err(|error| error.to_string())?
    }

    pub fn purge_threads(&self) -> Result<ConversationPurgeStats, String> {
        let store = self.clone();
        self.blocking
            .run(async move { store.purge_threads_async().await })
            .map_err(|error| error.to_string())?
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_stream_id(stream: &str) -> Option<String> {
    let encoded = stream.strip_prefix(STREAM_PREFIX)?;
    if encoded.len() % 2 != 0 {
        return None;
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(bytes).ok()
}

fn infer_labels(thread_id: &str) -> Vec<String> {
    if thread_id == "proactive:morning_briefing" {
        vec!["briefing".to_string()]
    } else if thread_id.starts_with("proactive:") {
        vec!["notification".to_string()]
    } else {
        vec!["general".to_string()]
    }
}

fn normalize_labels(labels: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::with_capacity(labels.len());
    for label in labels {
        let label = match label.as_str() {
            "work" => "general".to_string(),
            "from_reflection" | "subconscious_tick" => "subconscious".to_string(),
            "agent-task" | "worker" => "tasks".to_string(),
            _ => label,
        };
        if !normalized.contains(&label) {
            normalized.push(label);
        }
    }
    normalized
}

fn open_error(config: &crate::memory::config::MemoryConfig) -> Result<ConversationStore, String> {
    ConversationStore::open(config).map_err(|error| error.to_string())
}

pub fn ensure_thread(
    config: &crate::memory::config::MemoryConfig,
    request: CreateConversationThread,
) -> Result<ConversationThread, String> {
    open_error(config)?.ensure_thread(request)
}

pub fn list_threads(
    config: &crate::memory::config::MemoryConfig,
) -> Result<Vec<ConversationThread>, String> {
    open_error(config)?.list_threads()
}

pub fn get_messages(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
) -> Result<Vec<ConversationMessage>, String> {
    open_error(config)?.get_messages(thread_id)
}

pub fn append_message(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    message: ConversationMessage,
) -> Result<ConversationMessage, String> {
    open_error(config)?.append_message(thread_id, message)
}

pub fn update_thread_title(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    title: &str,
    updated_at: &str,
) -> Result<ConversationThread, String> {
    open_error(config)?.update_thread_title(thread_id, title, updated_at)
}

pub fn update_thread_labels(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    labels: Vec<String>,
    updated_at: &str,
) -> Result<ConversationThread, String> {
    open_error(config)?.update_thread_labels(thread_id, labels, updated_at)
}

pub fn update_message(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    message_id: &str,
    patch: ConversationMessagePatch,
) -> Result<ConversationMessage, String> {
    open_error(config)?.update_message(thread_id, message_id, patch)
}

pub fn delete_messages_from(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    message_id: &str,
) -> Result<Option<usize>, String> {
    open_error(config)?.delete_messages_from(thread_id, message_id)
}

pub fn delete_thread(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    deleted_at: &str,
) -> Result<bool, String> {
    open_error(config)?.delete_thread(thread_id, deleted_at)
}

pub fn purge_threads(
    config: &crate::memory::config::MemoryConfig,
) -> Result<ConversationPurgeStats, String> {
    open_error(config)?.purge_threads()
}

#[cfg(test)]
#[path = "store_port_tests.rs"]
mod port_tests;
