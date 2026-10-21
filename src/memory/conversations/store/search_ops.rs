//! Snapshot-backed conversation reads and cross-thread search.

use super::*;

impl ConversationStore {
    /// Read messages in their append order.
    pub async fn get_messages_async(
        &self,
        thread_id: &str,
    ) -> Result<Vec<ConversationMessage>, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
        Ok(self.snapshot(thread_id).await?.messages)
    }

    /// Substring search over live conversation messages.
    pub async fn search_cross_thread_messages_async(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> Result<Vec<CrossThreadHit>, String> {
        let _lifecycle = super::LIFECYCLE_LOCK.lock().await;
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
}
