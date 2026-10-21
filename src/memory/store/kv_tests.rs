use super::*;
use serde_json::json;
use tinystoragedrivers_types::Scope;

async fn config() -> MemoryConfig {
    let scope = Scope::new(&format!("test:kv-{}", uuid::Uuid::new_v4())).unwrap();
    let storage = tinystoragedrivers_testkit::memory_storage(&scope);
    MemoryConfig::open_with_storage("workspace", storage)
        .await
        .unwrap()
}

#[tokio::test]
async fn global_and_namespace_values_roundtrip_without_identity_collisions() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_global_async("same", &json!(null)).await.unwrap();
    kv.set_namespace_async("global", "same", &json!(7))
        .await
        .unwrap();

    assert_eq!(
        kv.get_global_async("same").await.unwrap(),
        Some(json!(null))
    );
    assert_eq!(
        kv.get_namespace_async("global", "same").await.unwrap(),
        Some(json!(7))
    );
    assert!(!kv.delete_global_async("missing").await.unwrap());
}

#[tokio::test]
async fn global_and_namespace_get_delete_keep_scope_isolated() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_global_async("shared", &json!("global"))
        .await
        .unwrap();
    kv.set_namespace_async("scope", "shared", &json!("local"))
        .await
        .unwrap();

    assert_eq!(
        kv.get_global_async("shared").await.unwrap(),
        Some(json!("global"))
    );
    assert_eq!(
        kv.get_namespace_async("scope", "shared").await.unwrap(),
        Some(json!("local"))
    );
    assert!(kv.delete_namespace_async("scope", "shared").await.unwrap());
    assert!(!kv.delete_namespace_async("scope", "shared").await.unwrap());
    assert_eq!(
        kv.get_namespace_async("scope", "shared").await.unwrap(),
        None
    );
    assert_eq!(
        kv.get_global_async("shared").await.unwrap(),
        Some(json!("global"))
    );
    assert!(kv.delete_global_async("shared").await.unwrap());
    assert!(!kv.delete_global_async("shared").await.unwrap());
    assert_eq!(kv.get_global_async("shared").await.unwrap(), None);
}

#[tokio::test]
async fn namespace_sanitization_and_scope_merge_preserve_precedence() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_global_async("global-setting", &json!(true))
        .await
        .unwrap();
    kv.set_namespace_async("team alpha/#1", "state", &json!({"open": true}))
        .await
        .unwrap();
    assert_eq!(
        kv.get_namespace_async("team_alpha/_1", "state")
            .await
            .unwrap(),
        Some(json!({"open": true}))
    );

    let listed = kv.list_namespace_async("team alpha/#1").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["key"], "state");
    assert_eq!(listed[0]["value"], json!({"open": true}));
    let scoped = kv.records_for_scope_async("team alpha/#1").await.unwrap();
    assert_eq!(scoped.len(), 2);
    assert!(scoped.iter().any(|record| {
        record.namespace.as_deref() == Some("team_alpha/_1") && record.key == "state"
    }));
    assert!(scoped
        .iter()
        .any(|record| record.namespace.is_none() && record.key == "global-setting"));
}

#[tokio::test]
async fn records_for_scope_keeps_namespace_records_ahead_of_global_ties() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let namespace = KvStore::sanitize_namespace("scope");
    let key = "shared";
    kv.port
        .put(
            &KvStore::storage_key(EntryKind::Namespace, Some(&namespace), key),
            serde_json::to_value(Entry {
                kind: EntryKind::Namespace,
                namespace: Some(namespace),
                key: key.into(),
                value: json!("local"),
                updated_at: 42.0,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    kv.port
        .put(
            &KvStore::storage_key(EntryKind::Global, None, key),
            serde_json::to_value(Entry {
                kind: EntryKind::Global,
                namespace: None,
                key: key.into(),
                value: json!("global"),
                updated_at: 42.0,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let merged = kv.records_for_scope_async("scope").await.unwrap();
    assert_eq!(
        merged.len(),
        2,
        "namespace values shadow reads, not storage"
    );
    assert_eq!(merged[0].namespace.as_deref(), Some("scope"));
    assert_eq!(merged[0].value, json!("local"));
    assert_eq!(merged[1].namespace, None);
    assert_eq!(merged[1].value, json!("global"));
}

#[tokio::test]
async fn a_hashed_storage_key_with_wrong_body_identity_fails_closed() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let key = "logical-key";
    kv.port
        .put(
            &KvStore::storage_key(EntryKind::Global, None, key),
            serde_json::to_value(Entry {
                kind: EntryKind::Global,
                namespace: None,
                key: "different-key".into(),
                value: json!("wrong record"),
                updated_at: 1.0,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let error = kv.get_global_async(key).await.unwrap_err();
    assert!(error.contains("identity mismatch"));
}

#[tokio::test]
async fn global_corruption_fails_closed_for_every_read_shape() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let key = "corrupt-global";
    kv.port
        .put(
            &KvStore::storage_key(EntryKind::Global, None, key),
            json!({"kind": "global", "namespace": null, "key": "other", "value": 1, "updated_at": 1.0}),
        )
        .await
        .unwrap();

    assert!(kv
        .get_global_async(key)
        .await
        .unwrap_err()
        .contains("identity mismatch"));
    assert!(kv
        .records_global_async()
        .await
        .unwrap_err()
        .contains("identity mismatch"));
    assert!(kv
        .records_for_scope_async("scope")
        .await
        .unwrap_err()
        .contains("identity mismatch"));
}

#[tokio::test]
async fn namespace_corruption_fails_closed_for_every_read_shape() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let namespace = "scope";
    let key = "corrupt-namespace";
    kv.port
        .put(
            &KvStore::storage_key(EntryKind::Namespace, Some(namespace), key),
            serde_json::to_value(Entry {
                kind: EntryKind::Namespace,
                namespace: Some("different-scope".into()),
                key: key.into(),
                value: json!(1),
                updated_at: 1.0,
            })
            .unwrap(),
        )
        .await
        .unwrap();

    assert!(kv
        .get_namespace_async(namespace, key)
        .await
        .unwrap_err()
        .contains("identity mismatch"));
    assert!(kv
        .list_namespace_async(namespace)
        .await
        .unwrap_err()
        .contains("identity mismatch"));
    assert!(kv
        .records_namespace_async(namespace)
        .await
        .unwrap_err()
        .contains("identity mismatch"));
    assert!(kv
        .records_for_scope_async(namespace)
        .await
        .unwrap_err()
        .contains("identity mismatch"));
}

#[tokio::test]
async fn pii_like_global_and_namespace_identities_keep_the_legacy_sanitization_policy() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let private_id = "123-45-6789";
    let sanitized_id = safety::pii::redact_pii(private_id).value;
    assert_ne!(sanitized_id, private_id);

    kv.set_global_async(&format!("global-{private_id}"), &json!("global"))
        .await
        .unwrap();
    assert_eq!(
        kv.get_global_async(&format!("global-{sanitized_id}"))
            .await
            .unwrap(),
        Some(json!("global"))
    );

    kv.set_namespace_async(
        &format!("team-{private_id}"),
        &format!("key-{private_id}"),
        &json!("namespace"),
    )
    .await
    .unwrap();
    let namespace = KvStore::normalize_namespace_write(&format!("team-{private_id}"));
    let key = KvStore::normalized_namespace_key(&format!("key-{private_id}"));
    assert_eq!(
        kv.get_namespace_async(&namespace, &key).await.unwrap(),
        Some(json!("namespace"))
    );
    assert!(kv
        .set_namespace_async("safe", "sk-proj-abcdefghijklmnop", &json!(1))
        .await
        .unwrap_err()
        .contains("cannot contain secrets"));
}

#[tokio::test]
async fn imported_legacy_records_keep_values_namespaces_and_timestamps_on_retry() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let records = [
        crate::memory::types::MemoryKvRecord {
            namespace: None,
            key: "global-null".into(),
            value: Value::Null,
            updated_at: 1234.125,
        },
        crate::memory::types::MemoryKvRecord {
            namespace: Some("team_alpha/_1".into()),
            key: "arbitrary/key/雪".into(),
            value: json!([1, false, null]),
            updated_at: 1234.5,
        },
    ];
    kv.import_legacy_records(&records).await.unwrap();
    kv.import_legacy_records(&records).await.unwrap();
    assert_eq!(
        kv.records_global_async().await.unwrap()[0].updated_at,
        1234.125
    );
    let imported = kv.records_namespace_async("team_alpha/_1").await.unwrap();
    assert_eq!(imported.len(), 1);
    assert_eq!(imported[0].namespace, records[1].namespace);
    assert_eq!(imported[0].key, records[1].key);
    assert_eq!(imported[0].value, records[1].value);
    assert_eq!(imported[0].updated_at, records[1].updated_at);
    assert_eq!(
        kv.get_global_async("global-null").await.unwrap(),
        Some(Value::Null)
    );
}

#[tokio::test]
async fn import_conflicts_are_detected_before_any_new_record_is_published() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_global_async("already", &json!("new destination value"))
        .await
        .unwrap();
    let records = [
        crate::memory::types::MemoryKvRecord {
            namespace: None,
            key: "would-be-first".into(),
            value: json!(1),
            updated_at: 10.0,
        },
        crate::memory::types::MemoryKvRecord {
            namespace: None,
            key: "already".into(),
            value: json!("legacy value"),
            updated_at: 1.0,
        },
    ];
    let error = kv.import_legacy_records(&records).await.unwrap_err();
    assert!(error.contains("conflicts with existing key"));
    assert_eq!(kv.get_global_async("would-be-first").await.unwrap(), None);
    assert_eq!(
        kv.get_global_async("already").await.unwrap(),
        Some(json!("new destination value"))
    );
}

#[tokio::test]
async fn kv_logical_keys_are_not_limited_by_driver_key_constraints() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let key = format!("arbitrary/{}\0suffix", "雪".repeat(700));
    let value = json!({"preserve": [null, false, 4]});
    kv.set_global_async(&key, &value).await.unwrap();
    assert_eq!(kv.get_global_async(&key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn paged_namespace_listing_returns_all_entries_and_stable_order() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    for index in 0..600 {
        kv.set_namespace_async("page", &format!("key-{index:04}"), &json!(index))
            .await
            .unwrap();
    }
    let listed = kv.list_namespace_async("page").await.unwrap();
    assert_eq!(listed.len(), 600);
    let keys = listed
        .iter()
        .map(|record| record["key"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(keys.len(), 600);
}

#[tokio::test]
async fn secret_keys_are_rejected_and_values_are_sanitized() {
    let config = config().await;
    let kv = KvStore::from_config(&config).unwrap();
    let error = kv
        .set_global_async(&["sk-proj-", "abcdefghijklmnop"].concat(), &json!("secret"))
        .await
        .unwrap_err();
    assert!(error.contains("cannot contain secrets"));
    kv.set_global_async(
        "notes",
        &json!(format!(
            "my key is {}",
            ["sk-", "abcdefghijklmnopqrstuvwxyz1234"].concat()
        )),
    )
    .await
    .unwrap();
    let stored = kv.get_global_async("notes").await.unwrap().unwrap();
    assert!(!stored
        .as_str()
        .unwrap()
        .contains(&["sk-", "abcdefghijklmnopqrstuvwxyz1234"].concat()));
}

#[test]
fn synchronous_api_uses_prepared_host_storage_outside_async_runtime() {
    let config = std::thread::spawn(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(config())
    })
    .join()
    .unwrap();
    let kv = KvStore::from_config(&config).unwrap();
    kv.set_global("sync", &json!({"ok": true})).unwrap();
    assert_eq!(kv.get_global("sync").unwrap(), Some(json!({"ok": true})));
}

#[tokio::test]
async fn cloned_config_reopens_the_same_durable_owner_scope() {
    let scope = Scope::new(&format!("test:kv-reopen-{}", uuid::Uuid::new_v4())).unwrap();
    let storage = tinystoragedrivers_testkit::memory_storage(&scope);
    let first = MemoryConfig::open_with_storage("workspace", storage.clone())
        .await
        .unwrap();
    KvStore::from_config(&first)
        .unwrap()
        .set_namespace_async("reopen", "key", &json!("persisted"))
        .await
        .unwrap();
    let reopened = MemoryConfig::open_with_storage("workspace", storage)
        .await
        .unwrap();
    assert_eq!(
        KvStore::from_config(&reopened)
            .unwrap()
            .get_namespace_async("reopen", "key")
            .await
            .unwrap(),
        Some(json!("persisted"))
    );
}
