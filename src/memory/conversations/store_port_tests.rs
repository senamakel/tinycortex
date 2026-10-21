use super::*;
use crate::memory::conversations::run_reply_message_id;

#[tokio::test]
async fn conversation_store_uses_prepared_streams_and_rebuildable_document_index() {
    let workspace = tempfile::tempdir().unwrap();
    let unprepared = crate::memory::config::MemoryConfig::new(workspace.path());
    assert!(ConversationStore::open(&unprepared).is_err());

    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let request = CreateConversationThread {
        id: "thread:alpha".into(),
        title: "Alpha".into(),
        created_at: "2025-02-03T04:05:06Z".into(),
        parent_thread_id: Some("parent".into()),
        labels: Some(vec!["general".into()]),
        personality_id: Some("persona-a".into()),
    };
    let thread = store.ensure_thread_async(request).await.unwrap();
    assert_eq!(thread.id, "thread:alpha");
    assert_eq!(thread.parent_thread_id.as_deref(), Some("parent"));
    assert_eq!(thread.personality_id.as_deref(), Some("persona-a"));

    let message = ConversationMessage {
        id: run_reply_message_id("reply-1"),
        content: "hello from the stream".into(),
        message_type: "text".into(),
        extra_metadata: serde_json::json!({"attempt": 1}),
        sender: "assistant".into(),
        created_at: "2025-02-03T04:06:00Z".into(),
    };
    store
        .append_message_async("thread:alpha", message.clone())
        .await
        .unwrap();
    store
        .append_message_async("thread:alpha", message.clone())
        .await
        .unwrap();
    let messages = store.get_messages_async("thread:alpha").await.unwrap();
    assert_eq!(messages, vec![message]);

    let listed = store.list_threads_async().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].message_count, 1);
    assert_eq!(listed[0].last_message_at, "2025-02-03T04:06:00Z");
    assert_eq!(
        store
            .search_cross_thread_messages_async("stream", 10, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn thread_stream_remains_authoritative_when_document_projection_write_fails() {
    use std::sync::Arc;
    use tinystoragedrivers_api::Storage;
    use tinystoragedrivers_testkit::Failing;
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let workspace = tempfile::tempdir().unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(
        &Scope::new("test:conversation-projection-repair").unwrap(),
    );
    let failing = Failing::new(raw.documents().clone());
    let faults = failing.faults.clone();
    let storage = Storage::new(
        Arc::new(failing),
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    let config = crate::memory::config::MemoryConfig::open_with_storage(workspace.path(), storage)
        .await
        .unwrap();
    let store = ConversationStore::open(&config).unwrap();
    let request = CreateConversationThread {
        id: "recoverable".into(),
        title: "Recoverable".into(),
        created_at: "2025-02-03T04:05:06Z".into(),
        parent_thread_id: None,
        labels: None,
        personality_id: None,
    };
    faults.fail_operation_nth("documents.put_with", 1, ErrorKind::Unavailable);
    assert!(store.ensure_thread_async(request.clone()).await.is_err());
    faults.clear();

    let repaired = store.ensure_thread_async(request).await.unwrap();
    assert_eq!(repaired.id, "recoverable");
    assert_eq!(store.list_threads_async().await.unwrap().len(), 1);
}

#[tokio::test]
async fn current_thread_async_conversation_operations_do_not_block_on_storage_bridge() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let request = CreateConversationThread {
        id: "current-thread".into(),
        title: "Current thread".into(),
        created_at: "2025-02-03T04:05:06Z".into(),
        parent_thread_id: None,
        labels: None,
        personality_id: None,
    };
    store.ensure_thread_async(request).await.unwrap();
    assert_eq!(store.list_threads_async().await.unwrap().len(), 1);
}
