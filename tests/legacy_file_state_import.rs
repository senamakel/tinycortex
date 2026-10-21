use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinycortex::memory::{
    config::MemoryConfig,
    persona::state::{KvStateStore, LegacyPersonaStateEntry, PersonaStateStore},
    sources::{MemorySourceEntry, SourceConfigPort, SourceConfigSnapshot, SourceRegistry},
    sync::{
        audit::{import_legacy_audit_entries_async, read_audit_log_async, SyncAuditEntry},
        composio::connect::EntityStore,
    },
};

const FIXTURE: &str = "tests/fixtures/legacy-v0.1.3-file-state-public-writer/workspace";
const AUDIT_STREAM: &str = "tinycortex:sync-audit:v1";

#[derive(Default)]
struct TestSourceConfig(Mutex<(u64, Vec<MemorySourceEntry>)>);

#[async_trait]
impl SourceConfigPort for TestSourceConfig {
    async fn read(&self) -> Result<SourceConfigSnapshot> {
        let state = self.0.lock().expect("source config lock");
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
        let mut state = self.0.lock().expect("source config lock");
        if state.0.to_string() != expected_revision {
            return Ok(false);
        }
        state.0 += 1;
        state.1 = entries;
        Ok(true)
    }
}

#[derive(Deserialize)]
struct LegacyEntityFile {
    entities: HashMap<String, String>,
}

#[tokio::test]
async fn public_v013_file_state_import_preserves_all_records_and_retries() -> Result<()> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let source_files = [
        workspace.join("memory_tree/sync_audit.jsonl"),
        workspace.join("persona/sync-state.json"),
        workspace.join(".composio-harness.json"),
        workspace.join("config.toml"),
    ];
    let source_hashes = source_files
        .iter()
        .map(|path| Ok((path.clone(), sha256_file(path)?)))
        .collect::<Result<Vec<_>>>()?;

    let audit_bytes = std::fs::read(&source_files[0])?;
    let audit_source_id = format!("sha256:{}", sha256(&audit_bytes));
    let audit_entries = audit_bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<SyncAuditEntry>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(audit_entries.len(), 2);
    assert!(audit_entries[0].timestamp > audit_entries[1].timestamp);

    let persona_values: HashMap<String, Value> =
        serde_json::from_slice(&std::fs::read(&source_files[1])?)?;
    let persona_entries = persona_values
        .iter()
        .map(|(key, value)| {
            let key = key
                .strip_prefix("persona-sync-state:")
                .context("legacy persona key has an unexpected namespace")?;
            Ok(LegacyPersonaStateEntry {
                key: key.to_owned(),
                value: value.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let entity_file: LegacyEntityFile = serde_json::from_slice(&std::fs::read(&source_files[2])?)?;
    let entity_entries = entity_file.entities.into_iter().collect::<Vec<_>>();

    let source_toml: toml::Value = toml::from_str(&std::fs::read_to_string(&source_files[3])?)?;
    let source_entries = source_toml
        .get("memory_sources")
        .and_then(toml::Value::as_array)
        .context("legacy TOML has no memory_sources array")?
        .iter()
        .cloned()
        .map(|entry| entry.try_into::<MemorySourceEntry>())
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let scope = tinystoragedrivers_types::Scope::new(&format!(
        "test:legacy-file-state:{}",
        uuid::Uuid::new_v4()
    ))?;
    let storage = tinystoragedrivers_testkit::memory_storage(&scope);
    let config = MemoryConfig::open_with_storage(&workspace, storage).await?;

    import_legacy_audit_entries_async(&config, &audit_source_id, &audit_entries).await?;
    let stream = config.storage()?.streams();
    assert_eq!(stream.len(AUDIT_STREAM).await?, 0);
    let legacy_stream = "tinycortex:sync-audit:legacy-import:v1";
    assert_eq!(stream.len(legacy_stream).await?, 2);
    import_legacy_audit_entries_async(&config, &audit_source_id, &audit_entries).await?;
    assert_eq!(stream.len(legacy_stream).await?, 2);
    let imported_audit = read_audit_log_async(&config).await?;
    assert_eq!(
        imported_audit
            .iter()
            .map(|entry| entry.source_id.as_str())
            .collect::<Vec<_>>(),
        ["legacy-second", "legacy-first"]
    );
    assert_eq!(imported_audit[0], audit_entries[1]);
    assert_eq!(imported_audit[1], audit_entries[0]);

    let persona = KvStateStore::from_config(&config)?;
    persona.import_legacy_entries(&persona_entries).await?;
    persona.import_legacy_entries(&persona_entries).await?;
    let persona_reader: &dyn PersonaStateStore = &persona;
    for entry in &persona_entries {
        assert_eq!(
            persona_reader.get("persona-sync-state", &entry.key).await?,
            Some(entry.value.clone()),
            "persona key {:?}",
            entry.key
        );
    }
    assert_eq!(
        persona_reader
            .get(
                "persona-sync-state",
                "transcript:/home/alice@example.test/.agent/sessions/run.jsonl"
            )
            .await?,
        Some(Value::String("cursor:opaque-value@example.test".into()))
    );

    let entities = EntityStore::from_config(&config)?;
    entities.import_legacy_entities(&entity_entries).await?;
    entities.import_legacy_entities(&entity_entries).await?;
    assert_eq!(
        entities.get("slack").await?.as_deref(),
        Some("legacy-user@example.test")
    );
    assert_eq!(
        entities.get("gmail").await?.as_deref(),
        Some("legacy-gmail-user")
    );

    let source_port = Arc::new(TestSourceConfig::default());
    let registry = SourceRegistry::from_port(source_port.clone());
    assert_eq!(registry.import_legacy_entries(&source_entries).await?, 2);
    assert_eq!(registry.import_legacy_entries(&source_entries).await?, 0);
    let imported_sources = registry.list().await?;
    assert_eq!(imported_sources.len(), 2);
    assert_eq!(
        serde_json::to_value(&imported_sources)?,
        serde_json::to_value(&source_entries)?,
        "all legacy source configuration fields must survive the import"
    );
    let folder = imported_sources
        .iter()
        .find(|entry| entry.id == "legacy-folder")
        .context("legacy folder source missing")?;
    assert_eq!(folder.path.as_deref(), Some("/legacy/notes"));
    assert_eq!(folder.glob.as_deref(), Some("**/*.md"));
    assert_eq!(folder.max_tokens_per_sync, Some(987));
    assert_eq!(folder.max_cost_per_sync_usd, Some(0.42));
    let composio = imported_sources
        .iter()
        .find(|entry| entry.id == "legacy-slack")
        .context("legacy Composio source missing")?;
    assert!(!composio.enabled);
    assert_eq!(composio.toolkit.as_deref(), Some("slack"));
    assert_eq!(composio.connection_id.as_deref(), Some("legacy-connection"));
    assert_eq!(composio.max_items, Some(17));
    assert_eq!(composio.sync_depth_days, Some(12));

    for (path, expected_hash) in source_hashes {
        assert_eq!(
            sha256_file(&path)?,
            expected_hash,
            "source changed: {}",
            path.display()
        );
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    Ok(sha256(&std::fs::read(path)?))
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
