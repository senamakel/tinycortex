//! Public asynchronous conversation operations.

use super::*;

impl ConversationStore {
    /// Create or touch a thread. The event stream is durable before the
    /// derived document projection is updated; a failed projection is repaired
    /// by the next operation that projects the thread.
    pub async fn ensure_thread_async(
        &self,
        request: CreateConversationThread,
    ) -> Result<ConversationThread, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(&request.id).await?;
            if let Some(tombstone_offset) = before.tombstone_offset {
                self.storage
                    .streams()
                    .truncate_before(&Self::stream_name(&request.id), tombstone_offset)
                    .await
                    .map_err(|error| format!("finalize deleted conversation cleanup: {error}"))?;
            }
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
                    .map(super::normalize_labels)
                    .unwrap_or_else(|| super::infer_labels(&request.id)),
                personality_id: request.personality_id.clone(),
            });
            thread.title = request.title.clone();
            if let Some(parent) = &request.parent_thread_id {
                thread.parent_thread_id = Some(parent.clone());
            }
            if let Some(labels) = &request.labels {
                thread.labels = super::normalize_labels(labels.clone());
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

    /// Append one message. Deterministic message ids are deduplicated against
    /// the stream's current contents; arbitrary client ids retain append
    /// semantics.
    pub async fn append_message_async(
        &self,
        thread_id: &str,
        message: ConversationMessage,
    ) -> Result<ConversationMessage, String> {
        self.append_message_with_idempotency(thread_id, message, false)
            .await
    }

    /// Append a message once per ID, for event sources that redeliver stable
    /// external message IDs such as channel provider IDs.
    pub async fn append_message_idempotent_async(
        &self,
        thread_id: &str,
        message: ConversationMessage,
    ) -> Result<ConversationMessage, String> {
        self.append_message_with_idempotency(thread_id, message, true)
            .await
    }

    async fn append_message_with_idempotency(
        &self,
        thread_id: &str,
        message: ConversationMessage,
        dedupe_any_id: bool,
    ) -> Result<ConversationMessage, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(thread_id).await?;
            if before.thread.is_none() {
                return Err(format!("thread {thread_id} not found"));
            }
            if dedupe_any_id || is_deterministic_message_id(&message.id) {
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

    /// Update a thread title while preserving its other metadata.
    pub async fn update_thread_title_async(
        &self,
        thread_id: &str,
        title: &str,
        updated_at: &str,
    ) -> Result<ConversationThread, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
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
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        let labels = super::normalize_labels(labels);
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
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
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
            let mut updated_messages = snapshot.messages.clone();
            if let Some(existing) = updated_messages
                .iter_mut()
                .find(|item| item.id == message.id)
            {
                *existing = message.clone();
            }
            let mut thread = snapshot
                .thread
                .clone()
                .ok_or_else(|| format!("thread {thread_id} not found"))?;
            thread.message_count = updated_messages.len();
            thread.last_message_at = updated_messages
                .last()
                .map(|item| item.created_at.clone())
                .unwrap_or_else(|| thread.created_at.clone());
            match self
                .append_event_at(
                    thread_id,
                    snapshot.offset,
                    Event::TranscriptCheckpoint {
                        thread,
                        updated_at: snapshot.updated_at.clone(),
                        messages: updated_messages,
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    self.storage
                        .streams()
                        .truncate_before(&Self::stream_name(thread_id), snapshot.offset)
                        .await
                        .map_err(|error| {
                            format!("truncate replaced conversation message: {error}")
                        })?;
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
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let snapshot = self.snapshot(thread_id).await?;
            let Some(index) = snapshot
                .messages
                .iter()
                .position(|item| item.id == message_id)
            else {
                if let Some(checkpoint_offset) = snapshot.checkpoint_offset {
                    self.storage
                        .streams()
                        .truncate_before(&Self::stream_name(thread_id), checkpoint_offset)
                        .await
                        .map_err(|error| {
                            format!("finalize conversation cleanup {thread_id}: {error}")
                        })?;
                }
                return Ok(None);
            };
            let removed = snapshot.messages.len() - index;
            let mut retained_messages = snapshot.messages[..index].to_vec();
            let mut thread = snapshot
                .thread
                .clone()
                .ok_or_else(|| format!("thread {thread_id} not found"))?;
            thread.message_count = retained_messages.len();
            thread.last_message_at = retained_messages
                .last()
                .map(|item| item.created_at.clone())
                .unwrap_or_else(|| thread.created_at.clone());
            match self
                .append_event_at(
                    thread_id,
                    snapshot.offset,
                    Event::TranscriptCheckpoint {
                        thread,
                        updated_at: snapshot.updated_at.clone(),
                        messages: std::mem::take(&mut retained_messages),
                    },
                )
                .await
            {
                Err(AppendFailure::Conflict) => continue,
                Err(error) => return Err(error.message()),
                Ok(_) => {
                    self.storage
                        .streams()
                        .truncate_before(&Self::stream_name(thread_id), snapshot.offset)
                        .await
                        .map_err(|error| {
                            format!("truncate deleted conversation messages: {error}")
                        })?;
                    let latest = self.snapshot(thread_id).await?;
                    self.project(thread_id, &latest).await?;
                    return Ok(Some(removed));
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Delete a thread's transcript, retaining only a tombstone at the
    /// monotonic stream offset so stale writers cannot recreate old data.
    pub async fn delete_thread_async(
        &self,
        thread_id: &str,
        deleted_at: &str,
    ) -> Result<bool, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        self.delete_thread_locked_async(thread_id, deleted_at).await
    }

    async fn delete_thread_locked_async(
        &self,
        thread_id: &str,
        deleted_at: &str,
    ) -> Result<bool, String> {
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let before = self.snapshot(thread_id).await?;
            if before.thread.is_none() {
                if let Some(tombstone_offset) = before.tombstone_offset {
                    self.storage
                        .streams()
                        .truncate_before(&Self::stream_name(thread_id), tombstone_offset)
                        .await
                        .map_err(|error| {
                            format!("truncate deleted conversation {thread_id}: {error}")
                        })?;
                    self.project(thread_id, &before).await?;
                }
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
                    let tombstone_offset = latest.tombstone_offset.ok_or_else(|| {
                        format!("conversation {thread_id} missing delete marker after append")
                    })?;
                    self.storage
                        .streams()
                        .truncate_before(&Self::stream_name(thread_id), tombstone_offset)
                        .await
                        .map_err(|error| {
                            format!("truncate deleted conversation {thread_id}: {error}")
                        })?;
                    self.project(thread_id, &latest).await?;
                    return Ok(true);
                }
            }
        }
        Err("conversation stream changed too often".into())
    }

    /// Delete every live transcript and retry cleanup for prior tombstones.
    pub async fn purge_threads_async(&self) -> Result<ConversationPurgeStats, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        let mut stats = ConversationPurgeStats::default();
        let streams = self.storage.streams();
        for _ in 0..MAX_WRITE_ATTEMPTS {
            let names = streams
                .streams(STREAM_PREFIX)
                .await
                .map_err(|error| error.to_string())?;
            let mut found_live = false;
            for name in names {
                let Some(thread_id) = decode_stream_id(&name) else {
                    continue;
                };
                let before = self.snapshot(&thread_id).await?;
                if let Some(thread) = before.thread {
                    found_live = true;
                    if self
                        .delete_thread_locked_async(&thread_id, &thread.last_message_at)
                        .await?
                    {
                        stats.thread_count += 1;
                        stats.message_count += before.messages.len();
                    }
                } else if before.tombstone_offset.is_some() {
                    self.delete_thread_locked_async(&thread_id, &before.updated_at)
                        .await?;
                }
            }
            if !found_live {
                // Re-scan once after the last live deletion so a thread whose
                // creation linearized during the previous scan is included.
                let remaining = streams
                    .streams(STREAM_PREFIX)
                    .await
                    .map_err(|error| error.to_string())?;
                let mut has_live = false;
                for name in remaining {
                    if let Some(thread_id) = decode_stream_id(&name) {
                        if self.snapshot(&thread_id).await?.thread.is_some() {
                            has_live = true;
                            break;
                        }
                    }
                }
                if !has_live {
                    return Ok(stats);
                }
            }
        }
        Err("conversation store changed too often during purge".into())
    }
}
