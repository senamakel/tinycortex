//! Tests for source validation and host-config concurrency semantics.

use super::*;
use crate::memory::sources::types::SourceKind;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct TestSourceConfig {
    state: std::sync::Mutex<(u64, Vec<MemorySourceEntry>)>,
    inject_conflict: AtomicBool,
}

#[async_trait]
impl SourceConfigPort for TestSourceConfig {
    async fn read(&self) -> Result<SourceConfigSnapshot> {
        let state = self.state.lock().unwrap();
        Ok(SourceConfigSnapshot {
            revision: state.0.to_string(),
            entries: state.1.clone(),
        })
    }

    async fn compare_and_set(
        &self,
        expected_revision: &str,
        entries: Vec<MemorySourceEntry>,
    ) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if self.inject_conflict.swap(false, Ordering::SeqCst) {
            state.0 += 1;
            state.1.push(folder_entry("host_added"));
            return Ok(false);
        }
        if state.0.to_string() != expected_revision {
            return Ok(false);
        }
        state.0 += 1;
        state.1 = entries;
        Ok(true)
    }
}

fn registry() -> (tempfile::TempDir, SourceRegistry, Arc<TestSourceConfig>) {
    let temp = tempfile::tempdir().unwrap();
    let port = Arc::new(TestSourceConfig::default());
    let registry = SourceRegistry::from_port(port.clone());
    (temp, registry, port)
}

fn folder_entry(id: &str) -> MemorySourceEntry {
    MemorySourceEntry {
        id: id.into(),
        kind: SourceKind::Folder,
        label: "Notes".into(),
        enabled: true,
        toolkit: None,
        connection_id: None,
        path: Some("/tmp/notes".into()),
        glob: Some("**/*.md".into()),
        url: None,
        branch: None,
        paths: Vec::new(),
        max_commits: None,
        max_issues: None,
        max_prs: None,
        query: None,
        since_days: None,
        max_items: None,
        selector: None,
        max_tokens_per_sync: None,
        max_cost_per_sync_usd: None,
        sync_depth_days: None,
    }
}

#[tokio::test]
async fn list_is_empty_for_missing_host_config() {
    let (_temp, registry, _) = registry();
    assert!(registry.list().await.unwrap().is_empty());
    assert!(registry.get("anything").await.unwrap().is_none());
}

#[tokio::test]
async fn add_get_list_round_trip() {
    let (_temp, registry, _) = registry();
    let added = registry.add(folder_entry("src_1")).await.unwrap();
    assert_eq!(added.id, "src_1");

    let got = registry.get("src_1").await.unwrap().unwrap();
    assert_eq!(got.kind, SourceKind::Folder);
    assert_eq!(got.path.as_deref(), Some("/tmp/notes"));
    assert_eq!(registry.list().await.unwrap().len(), 1);
}

#[tokio::test]
async fn add_rejects_duplicate_id() {
    let (_temp, registry, _) = registry();
    registry.add(folder_entry("src_dup")).await.unwrap();
    assert!(registry.add(folder_entry("src_dup")).await.is_err());
}

#[tokio::test]
async fn legacy_source_import_is_idempotent_and_conflicts_before_config_write() {
    let (_temp, registry, port) = registry();
    let existing = folder_entry("existing");
    registry.add(existing.clone()).await.unwrap();

    let mut changed_existing = existing.clone();
    changed_existing.label = "different".into();
    let imports = vec![folder_entry("new-source"), changed_existing];
    assert!(registry.import_legacy_entries(&imports).await.is_err());
    assert_eq!(registry.list().await.unwrap().len(), 1);
    assert_eq!(
        registry.get("existing").await.unwrap().unwrap().label,
        existing.label
    );

    let entry = folder_entry("legacy-source");
    assert_eq!(registry.import_legacy_entries(std::slice::from_ref(&entry)).await.unwrap(), 1);
    assert_eq!(registry.import_legacy_entries(std::slice::from_ref(&entry)).await.unwrap(), 0);
    assert!(port.state.lock().unwrap().1.iter().any(|source| source.id == "legacy-source"));
}

#[tokio::test]
async fn add_rejects_invalid_entry_without_mutating_host_config() {
    let (_temp, registry, _) = registry();
    let mut bad = folder_entry("src_bad");
    bad.path = None;
    assert!(registry.add(bad).await.is_err());
    assert!(registry.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_adds_preserve_every_source() {
    let (_temp, registry, _) = registry();
    let tasks = (0..24).map(|index| {
        let registry = registry.clone();
        tokio::spawn(async move {
            registry
                .add(folder_entry(&format!("src_concurrent_{index}")))
                .await
                .unwrap();
        })
    });
    for result in futures::future::join_all(tasks).await {
        result.unwrap();
    }
    let ids: std::collections::HashSet<_> = registry
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    assert_eq!(ids.len(), 24);
    for index in 0..24 {
        assert!(ids.contains(&format!("src_concurrent_{index}")));
    }
}

#[tokio::test]
async fn update_applies_patch_and_persists_through_host_port() {
    let (_temp, registry, _) = registry();
    registry.add(folder_entry("src_u")).await.unwrap();
    let patch = MemorySourcePatch {
        label: Some("Renamed".into()),
        enabled: Some(false),
        ..Default::default()
    };
    let updated = registry.update("src_u", patch).await.unwrap();
    assert_eq!(updated.label, "Renamed");
    assert!(!updated.enabled);
    let got = registry.get("src_u").await.unwrap().unwrap();
    assert_eq!(got.label, "Renamed");
    assert!(!got.enabled);
}

#[tokio::test]
async fn update_missing_id_errors() {
    let (_temp, registry, _) = registry();
    assert!(registry
        .update("nope", MemorySourcePatch::default())
        .await
        .is_err());
}

#[tokio::test]
async fn remove_returns_whether_anything_was_removed() {
    let (_temp, registry, _) = registry();
    registry.add(folder_entry("src_r")).await.unwrap();
    assert!(registry.remove("src_r").await.unwrap());
    assert!(!registry.remove("src_r").await.unwrap());
    assert!(registry.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn list_enabled_by_kind_filters() {
    let (_temp, registry, _) = registry();
    registry.add(folder_entry("src_a")).await.unwrap();
    let mut disabled = folder_entry("src_b");
    disabled.enabled = false;
    registry.add(disabled).await.unwrap();

    let enabled = registry
        .list_enabled_by_kind(SourceKind::Folder)
        .await
        .unwrap();
    assert_eq!(enabled.len(), 1);
    assert_eq!(enabled[0].id, "src_a");
    assert!(registry
        .list_enabled_by_kind(SourceKind::Conversation)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn source_update_retries_after_host_revision_conflict() {
    let (_temp, registry, port) = registry();
    port.inject_conflict.store(true, Ordering::SeqCst);
    registry.add(folder_entry("caller_added")).await.unwrap();
    let mut ids: Vec<_> = registry
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["caller_added", "host_added"]);
}

#[tokio::test]
async fn registry_never_writes_workspace_files() {
    let (temp, registry, _) = registry();
    registry.add(folder_entry("src_opaque")).await.unwrap();
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn composio_upsert_and_remove_preserve_source_policy() {
    let (_temp, registry, _) = registry();
    let first = registry
        .upsert_composio_source("gmail", "conn_a", "Gmail")
        .await
        .unwrap();
    assert_eq!(first.max_items, Some(100));
    let mut custom = MemorySourcePatch {
        max_items: Some(Some(7)),
        ..Default::default()
    };
    registry.update(&first.id, custom.clone()).await.unwrap();
    let updated = registry
        .upsert_composio_source("gmail", "conn_a", "Gmail renamed")
        .await
        .unwrap();
    assert_eq!(updated.id, first.id);
    assert_eq!(updated.label, "Gmail renamed");
    assert_eq!(updated.max_items, Some(7));
    custom.max_items = None;

    assert_eq!(
        registry
            .remove_composio_source_by_connection_id("conn_a")
            .await
            .unwrap(),
        1
    );
    assert!(registry.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn apply_all_in_enables_sources_and_clears_limits() {
    let (_temp, registry, _) = registry();
    let mut source = folder_entry("src_all");
    source.enabled = false;
    source.max_items = Some(4);
    source.sync_depth_days = Some(9);
    registry.add(source).await.unwrap();

    let updated = registry.apply_all_in().await.unwrap();
    assert!(updated[0].enabled);
    assert_eq!(updated[0].max_items, None);
    assert_eq!(updated[0].sync_depth_days, None);
}
