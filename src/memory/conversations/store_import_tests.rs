use super::*;
#[tokio::test]
async fn typed_legacy_import_preserves_transcript_and_is_idempotent() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let transcript = LegacyConversationTranscript {
        thread: ConversationThread {
            id: "legacy:thread".into(),
            title: "Imported title".into(),
            chat_id: Some(42),
            is_active: true,
            message_count: 2,
            last_message_at: "2025-02-03T04:08:00Z".into(),
            created_at: "2025-02-03T04:05:00Z".into(),
            parent_thread_id: Some("legacy:parent".into()),
            labels: vec!["general".into(), "tasks".into()],
            personality_id: Some("legacy-persona".into()),
        },
        updated_at: "2025-02-03T04:07:00Z".into(),
        messages: vec![
            ConversationMessage {
                id: "legacy-m1".into(),
                content: "first legacy message".into(),
                message_type: "text".into(),
                extra_metadata: serde_json::json!({"origin": "legacy"}),
                sender: "user".into(),
                created_at: "2025-02-03T04:06:00Z".into(),
            },
            ConversationMessage {
                id: "legacy-m2".into(),
                content: "second legacy message".into(),
                message_type: "assistant".into(),
                extra_metadata: serde_json::json!({"run": 2}),
                sender: "assistant".into(),
                created_at: "2025-02-03T04:08:00Z".into(),
            },
        ],
    };

    store
        .import_legacy_transcript_async(transcript.clone())
        .await
        .unwrap();
    let before_retry = store.snapshot("legacy:thread").await.unwrap();
    store
        .import_legacy_transcript_async(transcript.clone())
        .await
        .unwrap();
    let after_retry = store.snapshot("legacy:thread").await.unwrap();

    assert_eq!(after_retry.offset, before_retry.offset);
    assert_eq!(after_retry.thread, before_retry.thread);
    assert_eq!(after_retry.messages, transcript.messages);
    assert_eq!(after_retry.updated_at, transcript.updated_at);
    let listed = store.list_threads_async().await.unwrap();
    assert_eq!(listed, vec![transcript.thread]);
}

#[tokio::test]
async fn typed_legacy_import_rejects_conflicting_transcript_without_append() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let base = LegacyConversationTranscript {
        thread: ConversationThread {
            id: "legacy:conflict".into(),
            title: "Original".into(),
            chat_id: None,
            is_active: true,
            message_count: 1,
            last_message_at: "2025-02-03T04:06:00Z".into(),
            created_at: "2025-02-03T04:05:00Z".into(),
            parent_thread_id: None,
            labels: vec!["general".into()],
            personality_id: None,
        },
        updated_at: "2025-02-03T04:05:00Z".into(),
        messages: vec![ConversationMessage {
            id: "legacy-message".into(),
            content: "original".into(),
            message_type: "text".into(),
            extra_metadata: serde_json::json!({}),
            sender: "user".into(),
            created_at: "2025-02-03T04:06:00Z".into(),
        }],
    };
    store
        .import_legacy_transcript_async(base.clone())
        .await
        .unwrap();
    let before_conflict = store.snapshot("legacy:conflict").await.unwrap();
    let mut different = base;
    different.messages[0].content = "different source data".into();

    let error = store
        .import_legacy_transcript_async(different)
        .await
        .unwrap_err();

    assert!(error.contains("conflicts with existing data"));
    let after_conflict = store.snapshot("legacy:conflict").await.unwrap();
    assert_eq!(after_conflict.offset, before_conflict.offset);
    assert_eq!(after_conflict.messages, before_conflict.messages);
}

#[tokio::test]
async fn typed_import_does_not_resurrect_a_deleted_transcript() {
    let workspace = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    let store = ConversationStore::open(&config).unwrap();
    let transcript = LegacyConversationTranscript {
        thread: ConversationThread {
            id: "deleted-legacy".into(),
            title: "Deleted source".into(),
            chat_id: None,
            is_active: true,
            message_count: 1,
            last_message_at: "2025-02-03T04:06:00Z".into(),
            created_at: "2025-02-03T04:05:00Z".into(),
            parent_thread_id: None,
            labels: vec!["general".into()],
            personality_id: None,
        },
        updated_at: "2025-02-03T04:05:00Z".into(),
        messages: vec![ConversationMessage {
            id: "deleted-legacy-message".into(),
            content: "old imported payload".into(),
            message_type: "text".into(),
            extra_metadata: serde_json::json!({}),
            sender: "user".into(),
            created_at: "2025-02-03T04:06:00Z".into(),
        }],
    };
    store
        .import_legacy_transcript_async(transcript.clone())
        .await
        .unwrap();
    store
        .delete_thread_async("deleted-legacy", "2025-02-03T04:07:00Z")
        .await
        .unwrap();
    let deleted = store.snapshot("deleted-legacy").await.unwrap();

    let error = store
        .import_legacy_transcript_async(transcript)
        .await
        .unwrap_err();

    assert!(error.contains("was deleted previously"));
    let after_retry = store.snapshot("deleted-legacy").await.unwrap();
    assert_eq!(after_retry.offset, deleted.offset);
    assert!(after_retry.thread.is_none());
    assert!(after_retry.messages.is_empty());
}
