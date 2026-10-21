use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tinycortex::memory::conversations::{
    ConversationMessage, ConversationStore, ConversationThread, LegacyConversationTranscript,
};

#[derive(Deserialize)]
struct LegacyReadback {
    source_revision: String,
    threads: Vec<ConversationThread>,
    messages: BTreeMap<String, Vec<ConversationMessage>>,
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-v0.1.3-conversations")
}

fn legacy_thread_file_id(thread_id: &str) -> String {
    thread_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn owner_stream_id(thread_id: &str) -> String {
    format!(
        "tinycortex:conversation:{}",
        legacy_thread_file_id(thread_id)
    )
}

#[tokio::test]
async fn v013_public_writer_transcripts_import_losslessly_and_retry_idempotently() {
    let fixture = fixture_root();
    let expected: LegacyReadback = serde_json::from_slice(
        &fs::read(fixture.join("expected.json")).expect("checked-in public writer readback"),
    )
    .unwrap();
    assert_eq!(expected.source_revision, "ccb4e76");

    let threads_log = fs::read_to_string(fixture.join("data/threads.jsonl")).unwrap();
    let log_rows = threads_log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(log_rows.iter().any(|row| row["op"] == "upsert"));
    assert_eq!(
        log_rows
            .iter()
            .filter(|row| row["op"] == "message_appended")
            .count(),
        5
    );

    for thread in &expected.threads {
        let messages = expected.messages.get(&thread.id).unwrap();
        let message_file = fixture
            .join("data/threads")
            .join(format!("{}.jsonl", legacy_thread_file_id(&thread.id)));
        let raw_messages = fs::read_to_string(message_file)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let public_messages = messages
            .iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            raw_messages, public_messages,
            "legacy file order for {}",
            thread.id
        );
    }

    let workspace = tempfile::tempdir().unwrap();
    let storage = tinystoragedrivers_testkit::memory_storage(
        &tinystoragedrivers_types::Scope::new("test:legacy-conversation-import").unwrap(),
    );
    let config =
        tinycortex::memory::config::MemoryConfig::open_with_storage(workspace.path(), storage)
            .await
            .unwrap();
    let store = ConversationStore::open(&config).unwrap();
    for thread in &expected.threads {
        let transcript = LegacyConversationTranscript {
            thread: thread.clone(),
            updated_at: thread.last_message_at.clone(),
            messages: expected.messages.get(&thread.id).unwrap().clone(),
        };
        store
            .import_legacy_transcript_async(transcript.clone())
            .await
            .unwrap();
        let stream_id = owner_stream_id(&thread.id);
        let streams = config.storage().unwrap().streams();
        let offset = streams.len(&stream_id).await.unwrap();
        store
            .import_legacy_transcript_async(transcript)
            .await
            .unwrap();
        assert_eq!(streams.len(&stream_id).await.unwrap(), offset);
        assert_eq!(
            store.get_messages_async(&thread.id).await.unwrap(),
            *expected.messages.get(&thread.id).unwrap()
        );
    }
    assert_eq!(store.list_threads_async().await.unwrap(), expected.threads);
}
