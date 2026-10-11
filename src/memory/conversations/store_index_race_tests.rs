use super::*;

// ── concurrency: search cold rebuild must not block concurrent append ────────

/// Regression test for issue #2849.
///
/// Before the fix, `search_cross_thread_messages` held `CONVERSATION_STORE_LOCK`
/// for the entire cold index rebuild, stalling every concurrent
/// `append_message` call for as long as the rebuild took.  The fix
/// moved the rebuild outside that lock (`prime_index_if_cold`). The current
/// store additionally takes only the target thread lock while reading each
/// transcript, so unrelated appends complete independently.
///
/// The test seeds a fresh workspace (cold cache), races a search against
/// an append using a barrier, and asserts the append finishes within a
/// generous timeout that would be violated if the two operations were
/// serialized through one shared lock.
#[test]
fn search_cold_rebuild_does_not_block_concurrent_append() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let ts = "2026-04-10T12:00:00Z".to_string();

    // Fresh TempDir → path never seen by the process-level cache → cold.
    // Note: append_message only updates an *existing* cache entry; it never
    // inserts one, so the cache stays cold until the first search call.
    let temp = TempDir::new().unwrap();
    let store = ConversationStore::new(temp.path().to_path_buf());

    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "t1".to_string(),
            title: "Rebuild thread".to_string(),
            created_at: ts.clone(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "t2".to_string(),
            title: "Concurrent append target".to_string(),
            created_at: ts.clone(),
            labels: None,
            personality_id: None,
        })
        .unwrap();

    // Seed enough messages to give the rebuild real work.
    for i in 0..200_usize {
        store
            .append_message(
                "t1",
                ConversationMessage {
                    id: format!("seed-{i}"),
                    content: format!("seed message {i} for cold rebuild test"),
                    message_type: "text".to_string(),
                    extra_metadata: serde_json::json!({}),
                    sender: "user".to_string(),
                    created_at: ts.clone(),
                },
            )
            .unwrap();
    }

    let store_search = store.clone();
    let store_append = store.clone();

    let (scanned_tx, scanned_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let search_handle = thread::spawn(move || {
        let _lifecycle = store_search.locks.lifecycle.read();
        store_search.prime_index_if_cold_with_hook(|| {
            scanned_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })?;
        store_search.with_primed_index(|idx| idx.search("seed message", 5, None))
    });

    scanned_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("cold rebuild did not reach the post-scan test seam");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = store_append.append_message(
            "t2",
            ConversationMessage {
                id: "concurrent-append".to_string(),
                content: "written during cold rebuild".to_string(),
                message_type: "text".to_string(),
                extra_metadata: serde_json::json!({}),
                sender: "user".to_string(),
                created_at: ts,
            },
        );
        let _ = tx.send(result);
    });

    // The unrelated append must finish while publication is deliberately
    // paused. Releasing the rebuild first would let root-wide serialization
    // pass this test eventually and prove nothing about overlap.
    let append_result = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("unrelated append blocked behind a cold rebuild");
    assert!(
        append_result.is_ok(),
        "append failed: {:?}",
        append_result.err()
    );
    release_tx.send(()).unwrap();

    let search_result = search_handle.join().expect("search thread panicked");
    assert!(
        search_result.is_ok(),
        "search failed: {:?}",
        search_result.err()
    );
    let appended = store
        .search_cross_thread_messages("written during cold rebuild", 10, None)
        .unwrap();
    assert!(
        appended
            .iter()
            .any(|hit| hit.message_id == "concurrent-append"),
        "completed append was omitted from index: {appended:?}"
    );
}

#[test]
fn delete_during_cold_prime_cannot_republish_stale_messages() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let (temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "t1".to_string(),
            title: "Delete race".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    store
        .append_message(
            "t1",
            ConversationMessage {
                id: "m1".to_string(),
                content: "hello from a soon-deleted thread".to_string(),
                message_type: "text".to_string(),
                extra_metadata: serde_json::json!({}),
                sender: "user".to_string(),
                created_at: "2026-04-10T12:01:00Z".to_string(),
            },
        )
        .unwrap();
    let store_prime = store.clone();
    let store_delete = store.clone();
    let root = store.root_dir();
    CONVERSATION_INDEX_CACHE.lock().remove(&root);

    let (scanned_tx, scanned_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let prime = thread::spawn(move || {
        let _lifecycle = store_prime.locks.lifecycle.read();
        store_prime.prime_index_if_cold_with_hook(|| {
            scanned_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    });
    scanned_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("prime did not reach post-scan seam");

    let (delete_tx, delete_rx) = mpsc::channel();
    thread::spawn(move || {
        let result = store_delete.delete_thread("t1", "2026-04-10T12:02:00Z");
        delete_tx.send(result).unwrap();
    });
    assert!(
        delete_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "delete must wait for the cold prime's lifecycle read guard"
    );
    release_tx.send(()).unwrap();
    prime.join().unwrap().unwrap();
    assert!(delete_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap());

    let hits = store
        .search_cross_thread_messages("hello", 10, None)
        .unwrap();
    assert!(hits.is_empty(), "deleted content was republished: {hits:?}");
    drop(temp);
}

#[test]
fn delete_error_still_evicts_tombstoned_thread_from_warm_index() {
    let (_temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "delete-error".to_string(),
            title: "Delete error".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    store
        .append_message(
            "delete-error",
            ConversationMessage {
                id: "indexed-before-delete-error".to_string(),
                content: "must disappear after tombstone".to_string(),
                message_type: "text".to_string(),
                extra_metadata: json!({}),
                sender: "user".to_string(),
                created_at: "2026-04-10T12:01:00Z".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        store
            .search_cross_thread_messages("disappear after tombstone", 10, None)
            .unwrap()
            .len(),
        1
    );

    // A directory at the transcript path makes remove_file fail after the
    // metadata tombstone has already become durable.
    let transcript = store.thread_messages_path("delete-error");
    std::fs::remove_file(&transcript).unwrap();
    std::fs::create_dir(&transcript).unwrap();
    assert!(store
        .delete_thread("delete-error", "2026-04-10T12:02:00Z")
        .is_err());

    let hits = store
        .search_cross_thread_messages("disappear after tombstone", 10, None)
        .unwrap();
    assert!(
        hits.is_empty(),
        "tombstoned thread remained indexed: {hits:?}"
    );
}

#[test]
fn truncation_keeps_retained_messages_searchable_in_a_warm_index() {
    let (_temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "t1".to_string(),
            title: "Truncate indexed messages".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    for (id, content, minute) in [
        ("m1", "retained fern bookmark", "01"),
        ("m2", "removed lunar bookmark", "02"),
        ("m3", "later removed marker", "03"),
    ] {
        store
            .append_message(
                "t1",
                ConversationMessage {
                    id: id.to_string(),
                    content: content.to_string(),
                    message_type: "text".to_string(),
                    extra_metadata: serde_json::json!({}),
                    sender: "user".to_string(),
                    created_at: format!("2026-04-10T12:{minute}:00Z"),
                },
            )
            .unwrap();
    }
    assert_eq!(
        store
            .search_cross_thread_messages("retained fern", 10, None)
            .unwrap()
            .len(),
        1
    );

    assert_eq!(store.delete_messages_from("t1", "m2").unwrap(), Some(2));
    let retained = store
        .search_cross_thread_messages("retained fern", 10, None)
        .unwrap();
    assert_eq!(retained.len(), 1, "retained content must remain searchable");
    assert_eq!(retained[0].message_id, "m1");
    assert!(store
        .search_cross_thread_messages("removed lunar", 10, None)
        .unwrap()
        .is_empty());
}

#[test]
fn truncation_keeps_warm_index_consistent_when_stats_write_fails() {
    let (_temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "stats-failure".to_string(),
            title: "Stats failure".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    for (id, content, minute) in [
        ("m1", "retained stats survivor", "01"),
        ("m2", "removed stats tombstone", "02"),
    ] {
        store
            .append_message(
                "stats-failure",
                ConversationMessage {
                    id: id.to_string(),
                    content: content.to_string(),
                    message_type: "text".to_string(),
                    extra_metadata: serde_json::json!({}),
                    sender: "user".to_string(),
                    created_at: format!("2026-04-10T12:{minute}:00Z"),
                },
            )
            .unwrap();
    }
    assert_eq!(
        store
            .search_cross_thread_messages("survivor", 10, None)
            .unwrap()
            .len(),
        1
    );

    let metadata = store.root_dir().join(THREADS_FILENAME);
    std::fs::remove_file(&metadata).unwrap();
    std::fs::create_dir(&metadata).unwrap();
    assert!(store.delete_messages_from("stats-failure", "m2").is_err());

    let retained = store
        .search_cross_thread_messages("survivor", 10, None)
        .unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].message_id, "m1");
    assert!(store
        .search_cross_thread_messages("tombstone", 10, None)
        .unwrap()
        .is_empty());
}

#[test]
fn truncation_waits_for_cold_index_build_before_publishing_kept_messages() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let (_temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "t1".to_string(),
            title: "Cold truncation race".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    for (id, content, minute) in [
        ("m1", "kept cold build marker", "01"),
        ("m2", "deleted cold build marker", "02"),
    ] {
        store
            .append_message(
                "t1",
                ConversationMessage {
                    id: id.to_string(),
                    content: content.to_string(),
                    message_type: "text".to_string(),
                    extra_metadata: serde_json::json!({}),
                    sender: "user".to_string(),
                    created_at: format!("2026-04-10T12:{minute}:00Z"),
                },
            )
            .unwrap();
    }

    let root = store.root_dir();
    CONVERSATION_INDEX_CACHE.lock().remove(&root);
    let store_prime = store.clone();
    let (scanned_tx, scanned_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let prime = thread::spawn(move || {
        let _lifecycle = store_prime.locks.lifecycle.read();
        store_prime.prime_index_if_cold_with_hook(|| {
            scanned_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    });
    scanned_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("prime did not reach the post-scan seam");

    let store_delete = store.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (delete_tx, delete_rx) = mpsc::channel();
    let delete = thread::spawn(move || {
        started_tx.send(()).unwrap();
        delete_tx
            .send(store_delete.delete_messages_from("t1", "m2"))
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        delete_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "truncation must wait until the cold build publishes"
    );
    release_tx.send(()).unwrap();
    prime.join().unwrap().unwrap();
    assert_eq!(
        delete_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        Some(1)
    );
    delete.join().unwrap();

    let kept = store
        .search_cross_thread_messages("kept cold build", 10, None)
        .unwrap();
    assert_eq!(kept.len(), 1, "kept message must stay indexed");
    assert_eq!(kept[0].message_id, "m1");
    assert!(store
        .search_cross_thread_messages("deleted", 10, None)
        .unwrap()
        .is_empty());
}

#[test]
fn search_preserves_short_fullwidth_query_matches_after_folding() {
    let (_temp, store) = make_store();
    store
        .ensure_thread(CreateConversationThread {
            parent_thread_id: None,
            id: "fullwidth".to_string(),
            title: "Full-width query".to_string(),
            created_at: "2026-04-10T12:00:00Z".to_string(),
            labels: None,
            personality_id: None,
        })
        .unwrap();
    store
        .append_message(
            "fullwidth",
            ConversationMessage {
                id: "m1".to_string(),
                content: "ＡＢ appears in this message".to_string(),
                message_type: "text".to_string(),
                extra_metadata: serde_json::json!({}),
                sender: "user".to_string(),
                created_at: "2026-04-10T12:01:00Z".to_string(),
            },
        )
        .unwrap();

    let hits = store
        .search_cross_thread_messages("ＡＢ", 10, None)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, "m1");
}
