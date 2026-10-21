use super::*;
use crate::memory::conversations::ConversationStore;
async fn setup() -> (tempfile::TempDir, crate::memory::config::MemoryConfig) {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let storage = config.storage().unwrap().clone();
    for spec in super::super::collections() {
        storage.documents().ensure_collection(&spec).await.unwrap();
    }
    (workspace, config)
}

fn event(workspace_dir: std::path::PathBuf, content: &str) -> ChannelEvent {
    ChannelEvent::Received {
        channel: "slack".into(),
        message_id: "m-1".into(),
        sender: "alice".into(),
        reply_target: "general".into(),
        content: content.into(),
        thread_ts: None,
        workspace_dir,
    }
}

#[tokio::test]
async fn received_and_processed_channel_events_use_the_host_store() {
    let (workspace, config) = setup().await;
    let store = ConversationStore::open(&config).unwrap();
    let subscriber = ConversationPersistenceSubscriber::new(&config).unwrap();
    subscriber
        .handle(&event(workspace.path().to_path_buf(), "hello"))
        .await;
    subscriber
        .handle(&ChannelEvent::Processed {
            channel: "slack".into(),
            message_id: "m-1".into(),
            sender: "alice".into(),
            reply_target: "general".into(),
            thread_ts: None,
            response: "hi".into(),
            elapsed_ms: 5,
            success: true,
            workspace_dir: workspace.path().to_path_buf(),
        })
        .await;
    let threads = store.list_threads_async().await.unwrap();
    assert_eq!(threads.len(), 1);
    let messages = store.get_messages_async(&threads[0].id).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].content, "hello");
    assert_eq!(messages[1].content, "hi");
}

#[tokio::test]
async fn duplicate_channel_event_is_idempotent() {
    let (workspace, config) = setup().await;
    let store = ConversationStore::open(&config).unwrap();
    let subscriber = ConversationPersistenceSubscriber::new(&config).unwrap();
    let received = event(workspace.path().to_path_buf(), "hello");
    tokio::join!(subscriber.handle(&received), subscriber.handle(&received));
    let threads = store.list_threads_async().await.unwrap();
    let messages = store.get_messages_async(&threads[0].id).await.unwrap();
    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn stale_workspace_channel_event_is_dropped() {
    let (workspace, config) = setup().await;
    let store = ConversationStore::open(&config).unwrap();
    let subscriber = ConversationPersistenceSubscriber::new(&config).unwrap();
    let other = tempfile::tempdir().unwrap();
    subscriber
        .handle(&event(other.path().to_path_buf(), "ignored"))
        .await;
    assert!(store.list_threads_async().await.unwrap().is_empty());
    assert!(!workspace.path().as_os_str().is_empty());
}

#[test]
fn channel_thread_ids_do_not_alias_underscore_components() {
    assert_ne!(
        persisted_channel_thread_id("a_b", "c", "d", None),
        persisted_channel_thread_id("a", "b_c", "d", None)
    );
}
