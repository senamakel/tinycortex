//! Tests for persona incremental-run state.

use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn file_cursor_detects_change() {
    let dir = TempDir::new().unwrap();
    let f = dir.path().join("a.jsonl");
    std::fs::write(&f, "one").unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    let store = KvStateStore::from_config(&config).unwrap();
    let key = file_key("claude_code", &f);

    // Unrecorded → not unchanged.
    assert!(!file_unchanged(&store, &key, &f).await.unwrap());
    record_file(&store, &key, &f).await.unwrap();
    assert!(file_unchanged(&store, &key, &f).await.unwrap());

    // Append changes len → cursor differs.
    std::fs::write(&f, "one-two-three").unwrap();
    assert!(!file_unchanged(&store, &key, &f).await.unwrap());
}

#[tokio::test]
async fn watermark_roundtrips() {
    let dir = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    let store = KvStateStore::from_config(&config).unwrap();
    assert!(!watermark_unchanged(&store, "git_history:/r", "sha1")
        .await
        .unwrap());
    record_watermark(&store, "git_history:/r", "sha1")
        .await
        .unwrap();
    assert!(watermark_unchanged(&store, "git_history:/r", "sha1")
        .await
        .unwrap());
    assert!(!watermark_unchanged(&store, "git_history:/r", "sha2")
        .await
        .unwrap());
}

#[tokio::test]
async fn state_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    {
        let store = KvStateStore::from_config(&config).unwrap();
        record_watermark(&store, "k", "v").await.unwrap();
    }
    // Reopen the adapter over the same host storage scope.
    let store = KvStateStore::from_config(&config).unwrap();
    assert!(watermark_unchanged(&store, "k", "v").await.unwrap());
    assert!(!dir.path().join("persona/sync-state.json").exists());
}

#[tokio::test]
async fn opaque_cursor_identity_and_json_are_preserved_exactly() {
    let dir = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    let store = KvStateStore::from_config(&config).unwrap();
    let key = "transcript:/home/alice@example.com/.agent/sessions/run.jsonl";
    let value = serde_json::json!({"opaque": "sk-test-opaque-value", "email": "alice@example.com"});

    store.set(NAMESPACE, key, &value).await.unwrap();

    assert_eq!(store.get(NAMESPACE, key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn raw_state_kv_preserves_pii_in_cursor_text() {
    let dir = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    let store = KvStateStore::from_config(&config).unwrap();
    let key = "transcript:/home/alice@example.com/.agent/sessions/run.jsonl";
    let value = serde_json::json!({
        "cursor": "Imported transcript owner contact +15551234567"
    });

    store.set(NAMESPACE, key, &value).await.unwrap();

    assert_eq!(store.get(NAMESPACE, key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn legacy_cursor_import_is_idempotent_and_refuses_conflicts_before_writes() {
    let dir = TempDir::new().unwrap();
    let config = crate::memory::chunks::test_support::host_config(dir.path());
    let store = KvStateStore::from_config(&config).unwrap();
    let imported = vec![
        LegacyPersonaStateEntry {
            key: "file:/home/alice@example.test/.agent/session.jsonl".into(),
            value: serde_json::json!({"mtime_ms": 17, "len": 29}),
        },
        LegacyPersonaStateEntry {
            key: "git_history:/work/repo".into(),
            value: serde_json::json!("abc123"),
        },
    ];

    store.import_legacy_entries(&imported).await.unwrap();
    store.import_legacy_entries(&imported).await.unwrap();
    assert_eq!(
        store.get(NAMESPACE, &imported[0].key).await.unwrap(),
        Some(imported[0].value.clone())
    );

    store
        .set(NAMESPACE, &imported[1].key, &serde_json::json!("advanced"))
        .await
        .unwrap();
    let late_import = vec![
        LegacyPersonaStateEntry {
            key: "new-before-conflict".into(),
            value: serde_json::json!("must-not-publish"),
        },
        imported[1].clone(),
    ];
    assert!(store.import_legacy_entries(&late_import).await.is_err());
    assert_eq!(
        store.get(NAMESPACE, "new-before-conflict").await.unwrap(),
        None
    );
}
