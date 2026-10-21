use super::*;
#[tokio::test]
async fn deleted_transcripts_erase_payloads_and_stale_writes_cannot_resurrect_them() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    store
        .ensure_thread_async(CreateConversationThread {
            id: "erase-me".into(),
            title: "Erase me".into(),
            created_at: "2025-02-03T04:05:00Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    let message = ConversationMessage {
        id: "erase-message".into(),
        content: "private transcript payload".into(),
        message_type: "text".into(),
        extra_metadata: serde_json::json!({}),
        sender: "user".into(),
        created_at: "2025-02-03T04:06:00Z".into(),
    };
    store
        .append_message_async("erase-me", message.clone())
        .await
        .unwrap();
    let stale = store.snapshot("erase-me").await.unwrap();
    let stream = ConversationStore::stream_name("erase-me");

    assert!(store
        .delete_thread_async("erase-me", "2025-02-03T04:07:00Z")
        .await
        .unwrap());
    let retained = config
        .storage()
        .unwrap()
        .streams()
        .read_window(&stream, 0, 20)
        .await
        .unwrap();
    assert_eq!(retained.len(), 1, "only the tombstone may remain");
    assert_eq!(retained[0].offset, stale.offset);
    assert!(matches!(
        serde_json::from_value::<Event>(retained[0].value.clone()).unwrap(),
        Event::ThreadDelete { .. }
    ));
    assert!(!retained[0]
        .value
        .to_string()
        .contains("private transcript payload"));
    assert!(matches!(
        store
            .append_event_at(
                "erase-me",
                stale.offset,
                Event::MessageAppend {
                    message: message.clone()
                }
            )
            .await,
        Err(AppendFailure::Conflict)
    ));
    assert!(!store
        .get_messages_async("erase-me")
        .await
        .unwrap()
        .iter()
        .any(|item| item.content == message.content));

    store
        .ensure_thread_async(CreateConversationThread {
            id: "erase-me".into(),
            title: "New incarnation".into(),
            created_at: "2025-02-03T04:08:00Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    store
        .append_message_async(
            "erase-me",
            ConversationMessage {
                id: "fresh-message".into(),
                content: "fresh transcript".into(),
                message_type: "text".into(),
                extra_metadata: serde_json::json!({}),
                sender: "user".into(),
                created_at: "2025-02-03T04:09:00Z".into(),
            },
        )
        .await
        .unwrap();
    let recreated = store.snapshot("erase-me").await.unwrap();
    assert_eq!(recreated.messages.len(), 1);
    assert_eq!(recreated.messages[0].id, "fresh-message");

    let purged = store.purge_threads_async().await.unwrap();
    assert_eq!(purged.thread_count, 1);
    assert_eq!(purged.message_count, 1);
    let after_purge = config
        .storage()
        .unwrap()
        .streams()
        .read_window(&stream, 0, 20)
        .await
        .unwrap();
    assert_eq!(after_purge.len(), 1, "purge retains only a delete marker");
    assert!(matches!(
        serde_json::from_value::<Event>(after_purge[0].value.clone()).unwrap(),
        Event::ThreadDelete { .. }
    ));
    assert!(!after_purge[0]
        .value
        .to_string()
        .contains("fresh transcript"));

    let stale_import = LegacyConversationTranscript {
        thread: recreated.thread.unwrap(),
        updated_at: recreated.updated_at,
        messages: vec![message],
    };
    let before_import = store.snapshot("erase-me").await.unwrap();
    assert!(store
        .import_legacy_transcript_async(stale_import)
        .await
        .unwrap_err()
        .contains("was deleted previously"));
    let after_import = store.snapshot("erase-me").await.unwrap();
    assert_eq!(after_import.offset, before_import.offset);
    assert_eq!(after_import.messages, before_import.messages);
}

#[tokio::test]
async fn purge_serializes_new_thread_creation_across_store_handles() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let purger = ConversationStore::open(&config).unwrap();
    let creator = ConversationStore::open(&config).unwrap();
    purger
        .ensure_thread_async(CreateConversationThread {
            id: "before-purge".into(),
            title: "Before".into(),
            created_at: "2025-02-03T04:05:06Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();

    // Hold the same lifecycle gate while starting both real public
    // operations. FIFO mutex acquisition makes purge linearize first; a
    // create from another handle must wait until the whole purge completes.
    let gate = super::LIFECYCLE_LOCK.lock().await;
    let purge = tokio::spawn(async move { purger.purge_threads_async().await });
    tokio::task::yield_now().await;
    let create = tokio::spawn(async move {
        creator
            .ensure_thread_async(CreateConversationThread {
                id: "after-purge".into(),
                title: "After".into(),
                created_at: "2025-02-03T04:07:06Z".into(),
                parent_thread_id: None,
                labels: None,
                personality_id: None,
            })
            .await
    });
    tokio::task::yield_now().await;
    assert!(!purge.is_finished());
    assert!(!create.is_finished());
    drop(gate);

    let stats = purge.await.unwrap().unwrap();
    assert_eq!(stats.thread_count, 1);
    let thread = create.await.unwrap().unwrap();
    assert_eq!(thread.id, "after-purge");
    let remaining = ConversationStore::open(&config)
        .unwrap()
        .list_threads_async()
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "after-purge");
}
