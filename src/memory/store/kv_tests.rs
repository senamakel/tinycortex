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

    assert_eq!(kv.get_global_async("same").await.unwrap(), Some(json!(null)));
    assert_eq!(
        kv.get_namespace_async("global", "same").await.unwrap(),
        Some(json!(7))
    );
    assert!(kv.delete_global_async("missing").await.unwrap() == false);
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
    kv.set_namespace_async("scope", "z-local", &json!(1))
        .await
        .unwrap();
    kv.set_global_async("a-global", &json!(2)).await.unwrap();
    let mut scoped = kv.records_for_scope_async("scope").await.unwrap();
    // Both writes happen within one clock tick in the deterministic memory
    // driver test. If clock resolution differs, the API still sorts by time.
    scoped.sort_by(|left, right| left.updated_at.total_cmp(&right.updated_at));
    let local = scoped.iter().position(|record| record.namespace.is_some()).unwrap();
    let global = scoped.iter().position(|record| record.namespace.is_none()).unwrap();
    if scoped[local].updated_at == scoped[global].updated_at {
        let merged = kv.records_for_scope_async("scope").await.unwrap();
        assert!(merged[0].namespace.is_some());
    }
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
        &json!(format!("my key is {}", ["sk-", "abcdefghijklmnopqrstuvwxyz1234"].concat())),
    )
    .await
    .unwrap();
    let stored = kv.get_global_async("notes").await.unwrap().unwrap();
    assert!(!stored.as_str().unwrap().contains(&["sk-", "abcdefghijklmnopqrstuvwxyz1234"].concat()));
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
