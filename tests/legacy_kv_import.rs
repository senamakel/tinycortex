use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use tinycortex::memory::{config::MemoryConfig, store::KvStore, types::MemoryKvRecord};

const FIXTURE: &str = "tests/fixtures/legacy-v0.1.3-kv-public-writer/memory_tree/skill_docs.db";

#[tokio::test]
async fn public_v013_kv_fixture_imports_exact_records_and_retries_idempotently() -> Result<()> {
    let source_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let original_hash = sha256_file(&source_path)?;
    let records = read_legacy_records(&source_path)?;
    assert_eq!(records.len(), 5);

    let scope = tinystoragedrivers_types::Scope::new(&format!(
        "test:legacy-kv-import:{}",
        uuid::Uuid::new_v4()
    ))?;
    let storage = tinystoragedrivers_testkit::memory_storage(&scope);
    let config = MemoryConfig::open_with_storage("workspace", storage).await?;
    let store = KvStore::from_config(&config)?;

    store
        .import_legacy_records(&records)
        .await
        .map_err(anyhow::Error::msg)?;
    for record in &records {
        let actual = match &record.namespace {
            Some(namespace) => store
                .get_namespace_async(namespace, &record.key)
                .await
                .map_err(anyhow::Error::msg)?
                .context("imported namespace value missing")?,
            None => store
                .get_global_async(&record.key)
                .await
                .map_err(anyhow::Error::msg)?
                .context("imported global value missing")?,
        };
        assert_eq!(
            actual, record.value,
            "value for {:?}/{}",
            record.namespace, record.key
        );
    }
    let imported_global = store
        .records_global_async()
        .await
        .map_err(anyhow::Error::msg)?;
    let imported_namespace = store
        .records_namespace_async("team_alpha/_1")
        .await
        .map_err(anyhow::Error::msg)?;
    for expected in &records {
        let actual = match expected.namespace.as_ref() {
            Some(_) => imported_namespace
                .iter()
                .find(|row| row.key == expected.key),
            None => imported_global.iter().find(|row| row.key == expected.key),
        }
        .context("imported record metadata missing")?;
        assert_eq!(actual.namespace, expected.namespace);
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.value, expected.value);
        assert_eq!(actual.updated_at, expected.updated_at);
    }

    store
        .import_legacy_records(&records)
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(
        store
            .records_global_async()
            .await
            .map_err(anyhow::Error::msg)?
            .len(),
        imported_global.len()
    );
    assert_eq!(
        store
            .records_namespace_async("team_alpha/_1")
            .await
            .map_err(anyhow::Error::msg)?
            .len(),
        imported_namespace.len()
    );
    assert_eq!(
        sha256_file(&source_path)?,
        original_hash,
        "source fixture is read-only"
    );
    Ok(())
}

fn read_legacy_records(path: &Path) -> Result<Vec<MemoryKvRecord>> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open legacy KV source {} read-only", path.display()))?;
    let mut records = Vec::new();
    {
        let mut statement =
            connection.prepare("SELECT key, value_json, updated_at FROM kv_global ORDER BY key")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let value_json: String = row.get(1)?;
            records.push(MemoryKvRecord {
                namespace: None,
                key: row.get(0)?,
                value: serde_json::from_str(&value_json)?,
                updated_at: row.get(2)?,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT namespace, key, value_json, updated_at FROM kv_namespace ORDER BY namespace, key",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let value_json: String = row.get(2)?;
            records.push(MemoryKvRecord {
                namespace: Some(row.get(0)?),
                key: row.get(1)?,
                value: serde_json::from_str::<Value>(&value_json)?,
                updated_at: row.get(3)?,
            });
        }
    }
    Ok(records)
}

fn sha256_file(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}
