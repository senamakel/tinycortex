//! Read-only conversation listing operations.

use super::*;

impl ConversationStore {
    /// List live threads in descending update time, matching the legacy
    /// conversation list ordering.
    pub async fn list_threads_async(&self) -> Result<Vec<ConversationThread>, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
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
}
