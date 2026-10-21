use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use tinycortex::memory::{
    config::MemoryConfig,
    store::vectors::{LegacyVectorMetadata, LegacyVectorRecord, InertEmbedding, VectorStore},
};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::{Filter, Query, Scope};

const FIXTURE: &str = "tests/fixtures/legacy-v0.1.3-vector-public-writer/vectors.db";

#[tokio::test(flavor = "multi_thread")]
async fn v013_public_vector_writer_imports_and_reads_back() -> Result<()> {
    let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let (source, records) = read_legacy_vectors(&fixture_path)?;
    assert_eq!(source.provider_name, "fixture-provider");
    assert_eq!(source.dimensions, 3);
    assert_eq!(records.len(), 3);

    let workspace = tempfile::tempdir()?;
    let scope = Scope::new(&format!("test:legacy-vector-import:{}", uuid::Uuid::new_v4()))?;
    let config = MemoryConfig::open_with_storage(
        workspace.path(),
        tinystoragedrivers_testkit::memory_storage(&scope),
    )
    .await?;
    let store = VectorStore::from_config_async(&config, std::sync::Arc::new(InertEmbedding::new(3)))
        .await?;

    assert_eq!(store.import_legacy_records(&source, &records).await?, 3);
    assert_eq!(store.import_legacy_records(&source, &records).await?, 0);
    assert_eq!(store.count_async(None).await?, 3);
    assert_eq!(store.count_async(Some("notes")).await?, 2);
    assert_eq!(store.count_async(Some("archive")).await?, 1);
    assert_eq!(
        store.list_namespaces_async().await?,
        vec!["archive".to_owned(), "notes".to_owned()]
    );

    let notes = store
        .search_by_vector_async("notes", &[1.0, 0.0, 0.0], 2)
        .await?;
    assert_eq!(notes[0].id, "legacy-vector-a");
    assert_eq!(notes[0].text, "Historical vector fixture alpha.");
    assert_eq!(
        notes[0].metadata,
        json!({"origin": "v0.1.3-public-writer", "rank": 7, "nested": {"ok": true}})
    );
    assert_eq!(notes[1].id, "legacy-vector-b");
    assert_eq!(notes[1].metadata, json!(["legacy", 2, null]));
    let archive = store
        .search_by_vector_async("archive", &[0.5, 0.5, 0.0], 1)
        .await?;
    assert_eq!(archive[0].id, "same-id-other-namespace");
    assert_eq!(archive[0].metadata, json!({"namespace": "archive"}));

    // Query the owner documents to verify import preserved source bytes and
    // timestamps that the public search result intentionally does not expose.
    let docs = config.storage()?.documents().clone();
    let page = docs
        .query("memory_vectors", &Query::filter(Filter::All).limit(10))
        .await?;
    assert_eq!(page.items.len(), records.len());
    for row in page.items {
        let id = row.doc["id"].as_str().context("vector id missing")?;
        let source_row = records.iter().find(|record| record.id == id).context("source row missing")?;
        assert_eq!(row.doc["namespace"], source_row.namespace);
        assert_eq!(row.doc["text"], source_row.text);
        assert_eq!(row.doc["dimensions"], source_row.dimensions);
        assert_eq!(row.doc["embedding_le"], json!(source_row.embedding_le));
        assert_eq!(row.doc["metadata_json"], source_row.metadata_json);
        assert_eq!(
            row.doc["created_at"].as_f64().context("created_at missing")?.to_bits(),
            source_row.created_at.to_bits()
        );
        assert_eq!(
            row.doc["updated_at"].as_f64().context("updated_at missing")?.to_bits(),
            source_row.updated_at.to_bits()
        );
    }

    let metadata_page = docs
        .query("memory_vector_metadata", &Query::filter(Filter::All).limit(10))
        .await?;
    let source_metadata = metadata_page
        .items
        .iter()
        .map(|row| row.doc.clone())
        .find(|doc| doc.get("provider_name").and_then(Value::as_str) == Some("fixture-provider"))
        .context("legacy provider provenance missing")?;
    assert_eq!(source_metadata["dimensions"], 3);

    let wrong_dims = VectorStore::from_config_async(
        &config,
        std::sync::Arc::new(InertEmbedding::new(4)),
    )
    .await
    .expect_err("a reopened store must retain the historical dimension guard");
    assert!(wrong_dims.to_string().contains("dimension mismatch"));
    Ok(())
}

fn read_legacy_vectors(path: &std::path::Path) -> Result<(LegacyVectorMetadata, Vec<LegacyVectorRecord>)> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let provider_name = conn.query_row(
        "SELECT value FROM store_meta WHERE key = 'embed_provider'",
        [],
        |row| row.get::<_, String>(0),
    )?;
    let dimensions = conn.query_row(
        "SELECT value FROM store_meta WHERE key = 'embed_dims'",
        [],
        |row| row.get::<_, String>(0),
    )?.parse::<usize>()?;
    let mut statement = conn.prepare(
        "SELECT namespace, id, text, embedding, metadata, created_at, updated_at \
         FROM vectors ORDER BY namespace, id",
    )?;
    let records = statement
        .query_map([], |row| {
            Ok(LegacyVectorRecord {
                namespace: row.get(0)?,
                id: row.get(1)?,
                text: row.get(2)?,
                dimensions,
                embedding_le: row.get(3)?,
                metadata_json: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((
        LegacyVectorMetadata {
            provider_name,
            dimensions,
        },
        records,
    ))
}
