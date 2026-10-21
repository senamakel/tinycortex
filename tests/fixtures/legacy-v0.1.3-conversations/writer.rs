// Copied into the unmodified TinyCortex v0.1.3 archive by generate.sh.
// The records below are written and read through that release's public API.
use std::env;
use std::fs;
use std::path::PathBuf;

use tinycortex::memory::conversations::{
    ConversationMessage, ConversationStore, CreateConversationThread,
};

fn main() {
    let mut args = env::args_os().skip(1);
    let workspace = PathBuf::from(args.next().expect("workspace output path"));
    let expected_path = PathBuf::from(args.next().expect("expected readback path"));
    fs::create_dir_all(&workspace).expect("create fixture workspace");
    let store = ConversationStore::new(workspace);

    let conversations = [
        (
            "legacy:conversation-alpha",
            "Alpha transcript",
            "2025-02-03T04:05:06Z",
            vec!["general".to_string(), "research".to_string()],
            Some("legacy:parent".to_string()),
            Some("persona-alpha".to_string()),
            vec![
                ("source-repeat", "first duplicate ID", "2025-02-03T04:06:00Z"),
                ("source-repeat", "second duplicate ID", "2025-02-03T04:07:00Z"),
                ("assistant-final", "summary of the exchange", "2025-02-03T04:08:00Z"),
            ],
        ),
        (
            "legacy:conversation-beta",
            "Beta transcript",
            "2025-02-04T10:00:00Z",
            vec!["tasks".to_string()],
            None,
            None,
            vec![
                (
                    "user-question",
                    "How does the port preserve history?",
                    "2025-02-04T10:01:00Z",
                ),
                (
                    "assistant-answer",
                    "It imports ordered stream events.",
                    "2025-02-04T10:02:00Z",
                ),
            ],
        ),
    ];

    for (id, title, created_at, labels, parent, personality, messages) in conversations {
        store
            .ensure_thread(CreateConversationThread {
                id: id.to_string(),
                title: title.to_string(),
                created_at: created_at.to_string(),
                parent_thread_id: parent,
                labels: Some(labels),
                personality_id: personality,
            })
            .expect("write thread with the v0.1.3 public API");
        for (message_id, content, message_created_at) in messages {
            store
                .append_message(
                    id,
                    ConversationMessage {
                        id: message_id.to_string(),
                        content: content.to_string(),
                        message_type: "text".to_string(),
                        extra_metadata: serde_json::json!({
                            "writer": "tinycortex-v0.1.3-public-api",
                            "thread": id,
                        }),
                        sender: if message_id.starts_with("assistant") {
                            "assistant".to_string()
                        } else {
                            "user".to_string()
                        },
                        created_at: message_created_at.to_string(),
                    },
                )
                .expect("append message with the v0.1.3 public API");
        }
    }

    let threads = store.list_threads().expect("read public thread list");
    let messages = threads
        .iter()
        .map(|thread| {
            (
                thread.id.clone(),
                store
                    .get_messages(&thread.id)
                    .expect("read public thread messages"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    fs::write(
        expected_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "source_revision": "ccb4e76",
            "threads": threads,
            "messages": messages,
        }))
        .expect("serialize public API readback"),
    )
    .expect("write public API readback");
}
