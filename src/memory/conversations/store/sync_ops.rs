//! Synchronous wrappers for callers outside an async runtime.

use super::*;

impl ConversationStore {
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

/// Append one externally identified message at most once per thread.
pub async fn append_message_idempotent_async(
    config: &crate::memory::config::MemoryConfig,
    thread_id: &str,
    message: ConversationMessage,
) -> Result<ConversationMessage, String> {
    let store = open_error(config)?;
    store
        .append_message_idempotent_async(thread_id, message)
        .await
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
