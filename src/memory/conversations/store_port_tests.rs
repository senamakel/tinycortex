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
    let (first, retry) = tokio::join!(
        store.append_message_async("thread:alpha", message.clone()),
        store.append_message_async("thread:alpha", message.clone()),
    );
    assert_eq!(first.unwrap(), message);
    assert_eq!(retry.unwrap(), message);
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
async fn arbitrary_message_ids_keep_legacy_append_order_and_duplicates() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    store
        .ensure_thread_async(CreateConversationThread {
            id: "duplicate-id".into(),
            title: "Duplicates".into(),
            created_at: "2025-02-03T04:05:06Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    let message = ConversationMessage {
        id: "provider-message-id".into(),
        content: "first delivery".into(),
        message_type: "text".into(),
        extra_metadata: serde_json::Value::Null,
        sender: "user".into(),
        created_at: "2025-02-03T04:06:00Z".into(),
    };
    store
        .append_message_async("duplicate-id", message.clone())
        .await
        .unwrap();
    let mut redelivery = message.clone();
    redelivery.content = "second delivery".into();
    redelivery.created_at = "2025-02-03T04:07:00Z".into();
    store
        .append_message_async("duplicate-id", redelivery.clone())
        .await
        .unwrap();

    let messages = store.get_messages_async("duplicate-id").await.unwrap();
    assert_eq!(messages, vec![message, redelivery]);
}

#[tokio::test]
async fn create_preserves_empty_and_whitespace_thread_ids() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();

    for (id, title) in [("", "Empty id"), ("  ", "Whitespace id")] {
        let created = store
            .ensure_thread_async(CreateConversationThread {
                id: id.into(),
                title: title.into(),
                created_at: "2025-02-03T04:05:06Z".into(),
                parent_thread_id: None,
                labels: None,
                personality_id: None,
            })
            .await
            .unwrap();
        assert_eq!(created.id, id);
        assert_eq!(store.get_messages_async(id).await.unwrap(), Vec::new());
    }

    let listed = store.list_threads_async().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|thread| thread.id.is_empty()));
    assert!(listed.iter().any(|thread| thread.id == "  "));
}

#[tokio::test]
async fn legacy_import_preserves_empty_and_whitespace_thread_ids() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();

    for (id, title) in [("", "Imported empty id"), ("  ", "Imported whitespace id")] {
        let transcript = LegacyConversationTranscript {
            thread: ConversationThread {
                id: id.into(),
                title: title.into(),
                chat_id: None,
                is_active: true,
                message_count: 0,
                last_message_at: "2025-02-03T04:05:06Z".into(),
                created_at: "2025-02-03T04:05:06Z".into(),
                parent_thread_id: None,
                labels: vec!["general".into()],
                personality_id: None,
            },
            updated_at: "2025-02-03T04:05:06Z".into(),
            messages: Vec::new(),
        };
        store
            .import_legacy_transcript_async(transcript.clone())
            .await
            .unwrap();
        assert_eq!(store.get_messages_async(id).await.unwrap(), Vec::new());
        assert!(store
            .list_threads_async()
            .await
            .unwrap()
            .contains(&transcript.thread));
    }
}

#[tokio::test]
async fn legacy_import_preserves_repeated_message_ids_in_source_order() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let thread = ConversationThread {
        id: "legacy-duplicate-id".into(),
        title: "Legacy duplicates".into(),
        chat_id: None,
        is_active: true,
        message_count: 2,
        last_message_at: "2025-02-03T04:07:00Z".into(),
        created_at: "2025-02-03T04:05:06Z".into(),
        parent_thread_id: None,
        labels: vec!["general".into()],
        personality_id: None,
    };
    let message = |content: &str, created_at: &str| ConversationMessage {
        id: "same-legacy-id".into(),
        content: content.into(),
        message_type: "text".into(),
        extra_metadata: serde_json::Value::Null,
        sender: "assistant".into(),
        created_at: created_at.into(),
    };
    let messages = vec![
        message("first", "2025-02-03T04:06:00Z"),
        message("second", "2025-02-03T04:07:00Z"),
    ];
    store
        .import_legacy_transcript_async(LegacyConversationTranscript {
            thread: thread.clone(),
            updated_at: thread.last_message_at.clone(),
            messages: messages.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        store.get_messages_async(&thread.id).await.unwrap(),
        messages
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
async fn stale_same_offset_delete_removes_the_document_projection() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    store
        .ensure_thread_async(CreateConversationThread {
            id: "same-offset-delete".into(),
            title: "Projected thread".into(),
            created_at: "2025-02-03T04:05:06Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    let mut deleted = store.snapshot("same-offset-delete").await.unwrap();
    deleted.thread = None;

    store.project("same-offset-delete", &deleted).await.unwrap();

    let projected = config
        .storage()
        .unwrap()
        .documents()
        .get(
            THREAD_INDEX,
            &ConversationStore::document_id("same-offset-delete"),
        )
        .await
        .unwrap();
    assert!(projected.is_none());
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

#[tokio::test]
async fn concurrent_appends_preserve_every_message_and_thread_count() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    store
        .ensure_thread_async(CreateConversationThread {
            id: "concurrent-appends".into(),
            title: "Concurrent appends".into(),
            created_at: "2025-02-03T04:05:06Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();

    let tasks = (0..100)
        .map(|index| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append_message_async(
                        "concurrent-appends",
                        ConversationMessage {
                            id: format!("user:{index}"),
                            content: format!("message {index}"),
                            message_type: "text".into(),
                            extra_metadata: serde_json::json!({}),
                            sender: "user".into(),
                            created_at: format!("2025-02-03T04:{:02}:00Z", index % 60),
                        },
                    )
                    .await
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    let messages = store
        .get_messages_async("concurrent-appends")
        .await
        .unwrap();
    assert_eq!(messages.len(), 100);
    let thread = store.list_threads_async().await.unwrap().remove(0);
    assert_eq!(thread.message_count, 100);
    assert_eq!(thread.last_message_at, messages.last().unwrap().created_at);
}

#[tokio::test]
async fn updates_truncation_delete_recreate_and_purge_preserve_logical_transcript_semantics() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let created = store
        .ensure_thread_async(CreateConversationThread {
            id: "lifecycle".into(),
            title: "Initial".into(),
            created_at: "2025-02-03T04:05:06Z".into(),
            parent_thread_id: Some("parent".into()),
            labels: Some(vec!["work".into(), "general".into()]),
            personality_id: Some("persona".into()),
        })
        .await
        .unwrap();
    assert_eq!(created.labels, vec!["general"]);
    assert_eq!(created.parent_thread_id.as_deref(), Some("parent"));
    assert_eq!(created.personality_id.as_deref(), Some("persona"));

    let renamed = store
        .update_thread_title_async("lifecycle", "Renamed", "2025-02-03T04:06:00Z")
        .await
        .unwrap();
    assert_eq!(renamed.title, "Renamed");
    assert_eq!(renamed.created_at, created.created_at);
    let relabelled = store
        .update_thread_labels_async(
            "lifecycle",
            vec!["agent-task".into(), "tasks".into()],
            "2025-02-03T04:07:00Z",
        )
        .await
        .unwrap();
    assert_eq!(relabelled.labels, vec!["tasks"]);
    assert_eq!(relabelled.parent_thread_id.as_deref(), Some("parent"));

    for (id, created_at) in [
        ("m1", "2025-02-03T04:08:00Z"),
        ("m2", "2025-02-03T04:09:00Z"),
        ("m3", "2025-02-03T04:10:00Z"),
    ] {
        store
            .append_message_async(
                "lifecycle",
                ConversationMessage {
                    id: id.into(),
                    content: format!("body {id}"),
                    message_type: "text".into(),
                    extra_metadata: serde_json::json!({}),
                    sender: "user".into(),
                    created_at: created_at.into(),
                },
            )
            .await
            .unwrap();
    }
    let updated = store
        .update_message_async(
            "lifecycle",
            "m2",
            ConversationMessagePatch {
                extra_metadata: Some(serde_json::json!({"reaction": "👍"})),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        updated.extra_metadata,
        serde_json::json!({"reaction": "👍"})
    );
    assert_eq!(
        store
            .delete_messages_from_async("lifecycle", "m2")
            .await
            .unwrap(),
        Some(2)
    );
    let retained = store.get_messages_async("lifecycle").await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, "m1");
    assert_eq!(
        store.list_threads_async().await.unwrap()[0].last_message_at,
        "2025-02-03T04:08:00Z"
    );

    assert!(store
        .delete_thread_async("lifecycle", "2025-02-03T04:11:00Z")
        .await
        .unwrap());
    assert!(!store
        .delete_thread_async("lifecycle", "2025-02-03T04:12:00Z")
        .await
        .unwrap());
    assert!(store
        .get_messages_async("lifecycle")
        .await
        .unwrap()
        .is_empty());
    store
        .ensure_thread_async(CreateConversationThread {
            id: "lifecycle".into(),
            title: "Recreated".into(),
            created_at: "2025-02-03T04:13:00Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    assert!(store
        .get_messages_async("lifecycle")
        .await
        .unwrap()
        .is_empty());

    store
        .ensure_thread_async(CreateConversationThread {
            id: "another".into(),
            title: "Another".into(),
            created_at: "2025-02-03T04:14:00Z".into(),
            parent_thread_id: None,
            labels: None,
            personality_id: None,
        })
        .await
        .unwrap();
    let purged = store.purge_threads_async().await.unwrap();
    assert_eq!(purged.thread_count, 2);
    assert_eq!(purged.message_count, 0);
    assert!(store.list_threads_async().await.unwrap().is_empty());
}
