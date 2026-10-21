use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
use tinycortex::memory::{
    chunks::{
        self, Chunk, LegacyChunkRecord, LegacyContentPointer, LegacyEmbedding, LegacyEntityEdge,
        LegacyEntityOccurrence, LegacyImportBatch, LegacySourceGate, LegacySummaryContent,
        LegacySummaryEmbedding, RawRef, SourceKind,
    },
    config::MemoryConfig,
    graph,
    queue::{self, ImportedJob, JobKind, JobStatus},
    score::{self, signals::ScoreSignals, store::ScoreRow},
    store::{
        content::{read_chunk_body, read_summary_body},
        entity_index::EntityIndex,
    },
    tree::{self, SummaryNode, Tree, TreeKind, TreeStatus},
};

const FIXTURE: &str = "fixtures/legacy-v0.1.3-extended-public-writer/memory_tree";

/// Import an authentic v0.1.3 database through the typed owner API and verify
/// every populated record family through the current public readers.
#[tokio::test(flavor = "multi_thread")]
async fn extended_v013_public_writer_batch_imports_and_reads_back() -> Result<()> {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let batch = read_legacy_batch(&fixture_root)?;
    assert_eq!(batch.chunks.len(), 1);
    assert_eq!(batch.chunks[0].score.as_ref().map(|_| 1), Some(1));
    assert_eq!(batch.trees.len(), 1);
    assert_eq!(batch.summaries.len(), 1);
    assert_eq!(batch.entities.len(), 2);
    assert_eq!(batch.edges.len(), 1);
    assert_eq!(batch.jobs.len(), 1);

    let workspace = tempfile::tempdir()?;
    let destination_content = workspace.path().join("content");
    copy_tree(&fixture_root.join("content"), &destination_content)?;
    let scope = tinystoragedrivers_types::Scope::new(&format!(
        "test:legacy-extended-import:{}",
        uuid::Uuid::new_v4()
    ))?;
    let mut config = MemoryConfig::open_with_storage(
        workspace.path(),
        tinystoragedrivers_testkit::memory_storage(&scope),
    )
    .await?;
    config.content_root = Some(destination_content);

    let imported = chunks::import_legacy_batch(&config, &batch).await?;
    assert_eq!(imported.chunks, 1);
    assert_eq!(imported.scores, 1);
    assert_eq!(imported.entities, 2);
    assert_eq!(imported.trees, 2);
    assert_eq!(imported.jobs, 1);

    let source_chunk = &batch.chunks[0];
    let actual_chunk =
        chunks::get_chunk(&config, &source_chunk.chunk.id)?.context("chunk missing")?;
    assert_eq!(actual_chunk, source_chunk.chunk);
    assert_eq!(
        read_chunk_body(&config, &actual_chunk.id)?,
        actual_chunk.content
    );
    let pointer = source_chunk
        .content
        .as_ref()
        .context("chunk pointer missing")?;
    assert_eq!(
        chunks::get_chunk_content_pointers(&config, &actual_chunk.id)?,
        Some((pointer.path.clone(), pointer.sha256.clone()))
    );
    let actual_raw_refs =
        chunks::get_chunk_raw_refs(&config, &actual_chunk.id)?.unwrap_or_default();
    assert_eq!(actual_raw_refs, source_chunk.raw_refs);
    assert_eq!(
        chunks::get_chunk_lifecycle_status(&config, &actual_chunk.id)?,
        source_chunk.lifecycle_status
    );

    let source_score = source_chunk.score.as_ref().context("score missing")?;
    let actual_score =
        score::store::get_score(&config, &actual_chunk.id)?.context("score did not import")?;
    assert_score_eq(&actual_score, source_score);

    let source_tree = &batch.trees[0];
    assert_eq!(
        tree::store::get_tree(&config, &source_tree.id)?,
        Some(source_tree.clone())
    );
    let source_summary = &batch.summaries[0];
    assert_eq!(
        tree::store::get_summary(&config, &source_summary.id)?,
        Some(source_summary.clone())
    );
    assert_eq!(
        read_summary_body(&config, &source_summary.id)?,
        source_summary.content
    );
    let summary_pointer = batch
        .summary_content
        .iter()
        .find(|pointer| pointer.summary_id == source_summary.id)
        .context("summary content pointer missing")?;
    assert_eq!(
        chunks::get_summary_content_pointers(&config, &source_summary.id)?,
        Some((summary_pointer.path.clone(), summary_pointer.sha256.clone()))
    );

    let node_entities = score::store::list_entity_ids_for_node(&config, &source_summary.id)?;
    let mut expected_entities = batch
        .entities
        .iter()
        .map(|occurrence| occurrence.entity_id.clone())
        .collect::<Vec<_>>();
    expected_entities.sort();
    assert_eq!(node_entities, expected_entities);
    let entity_index = EntityIndex::for_memory_config(&config)?;
    for source_occurrence in &batch.entities {
        let hits = entity_index.lookup_entity(&source_occurrence.entity_id, None)?;
        let imported_hit = hits
            .iter()
            .find(|hit| hit.node_id == source_occurrence.node_id)
            .context("entity occurrence did not import")?;
        assert_eq!(imported_hit.node_kind, source_occurrence.node_kind);
        assert_eq!(
            imported_hit.entity_kind.as_str(),
            source_occurrence.entity_kind
        );
        assert_eq!(imported_hit.surface, source_occurrence.surface);
        assert_eq!(imported_hit.score, source_occurrence.score);
        assert_eq!(imported_hit.timestamp_ms, source_occurrence.timestamp_ms);
        assert_eq!(imported_hit.tree_id, source_occurrence.tree_id);
        assert_eq!(imported_hit.is_user, source_occurrence.is_user);
    }

    let edge = &batch.edges[0];
    assert!(graph::edge_neighbors(&config, &edge.entity_a)?
        .contains(&(edge.entity_b.clone(), edge.weight)));
    assert_eq!(graph::count_edges(&config)?, 1);

    let source_job = &batch.jobs[0];
    let actual_job = queue::get_job(&config, &source_job.id)?.context("job missing")?;
    assert_eq!(actual_job.kind, source_job.kind);
    assert_eq!(actual_job.payload_json, source_job.payload_json);
    assert_eq!(actual_job.dedupe_key, source_job.dedupe_key);
    assert_eq!(actual_job.status, JobStatus::Ready);
    assert_eq!(actual_job.attempts, source_job.attempts);
    assert_eq!(actual_job.max_attempts, source_job.max_attempts);
    assert_eq!(actual_job.created_at_ms, source_job.created_at_ms);
    assert_eq!(actual_job.available_at_ms, source_job.available_at_ms);

    Ok(())
}

fn read_legacy_batch(root: &Path) -> Result<LegacyImportBatch> {
    let db = root.join("chunks.db");
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut batch = LegacyImportBatch::default();

    let mut statement = conn.prepare(
        "SELECT id, source_kind, source_id, path_scope, source_ref, owner, timestamp_ms, \
         time_range_start_ms, time_range_end_ms, tags_json, content, token_count, \
         seq_in_source, created_at_ms, lifecycle_status, content_path, content_sha256, \
         raw_refs_json FROM mem_tree_chunks ORDER BY id",
    )?;
    for row in statement.query_map([], read_chunk)? {
        let (chunk, content, raw_refs, lifecycle_status) = row?;
        let score = conn
            .query_row(
                "SELECT total, token_count_signal, unique_words_signal, metadata_weight, \
                 source_weight, interaction_weight, entity_density, dropped, reason, \
                 computed_at_ms, llm_importance FROM mem_tree_score WHERE chunk_id = ?1",
                [&chunk.id],
                |row| {
                    Ok(ScoreRow {
                        chunk_id: chunk.id.clone(),
                        total: row.get(0)?,
                        signals: ScoreSignals {
                            token_count: row.get(1)?,
                            unique_words: row.get(2)?,
                            metadata_weight: row.get(3)?,
                            source_weight: row.get(4)?,
                            interaction: row.get(5)?,
                            entity_density: row.get(6)?,
                            llm_importance: row.get(10)?,
                        },
                        dropped: row.get::<_, i64>(7)? != 0,
                        reason: row.get(8)?,
                        computed_at_ms: row.get(9)?,
                        llm_importance_reason: None,
                    })
                },
            )
            .optional()?;
        let embeddings = read_embeddings(&conn, &chunk.id)?;
        batch.chunks.push(LegacyChunkRecord {
            chunk,
            content,
            raw_refs,
            score,
            lifecycle_status,
            embeddings,
            entities: Vec::new(),
        });
    }

    let mut trees = conn.prepare(
        "SELECT id, kind, scope, root_id, max_level, status, created_at_ms, last_sealed_at_ms, \
         ask FROM mem_tree_trees ORDER BY id",
    )?;
    batch.trees = trees
        .query_map([], |row| {
            let created = datetime(row.get(6)?).map_err(to_sql_error)?;
            let last_sealed: Option<i64> = row.get(7)?;
            Ok(Tree {
                id: row.get(0)?,
                kind: TreeKind::parse(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                scope: row.get(2)?,
                root_id: row.get(3)?,
                max_level: row.get::<_, i64>(4)? as u32,
                status: TreeStatus::parse(&row.get::<_, String>(5)?).map_err(to_sql_error)?,
                created_at: created,
                last_sealed_at: last_sealed.map(datetime).transpose()?,
                ask: row.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut summaries = conn.prepare(
        "SELECT id, tree_id, tree_kind, level, parent_id, child_ids_json, content, token_count, \
         entities_json, topics_json, time_range_start_ms, time_range_end_ms, score, sealed_at_ms, \
         deleted, embedding, doc_id, version_ms FROM mem_tree_summaries ORDER BY id",
    )?;
    batch.summaries = summaries
        .query_map([], |row| {
            let children: String = row.get(5)?;
            let entities: String = row.get(8)?;
            let topics: String = row.get(9)?;
            let embedding: Option<Vec<u8>> = row.get(15)?;
            Ok(SummaryNode {
                id: row.get(0)?,
                tree_id: row.get(1)?,
                tree_kind: TreeKind::parse(&row.get::<_, String>(2)?).map_err(to_sql_error)?,
                level: row.get::<_, i64>(3)? as u32,
                parent_id: row.get(4)?,
                child_ids: serde_json::from_str(&children).map_err(to_sql_error)?,
                content: row.get(6)?,
                token_count: row.get::<_, i64>(7)? as u32,
                entities: serde_json::from_str(&entities).map_err(to_sql_error)?,
                topics: serde_json::from_str(&topics).map_err(to_sql_error)?,
                time_range_start: datetime(row.get(10)?).map_err(to_sql_error)?,
                time_range_end: datetime(row.get(11)?).map_err(to_sql_error)?,
                score: row.get(12)?,
                sealed_at: datetime(row.get(13)?).map_err(to_sql_error)?,
                deleted: row.get::<_, i64>(14)? != 0,
                embedding: embedding
                    .map(decode_f32)
                    .transpose()
                    .map_err(to_sql_error)?,
                doc_id: row.get(16)?,
                version_ms: row.get(17)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for summary in &batch.summaries {
        let pointer: Option<(String, String)> = conn
            .query_row(
                "SELECT content_path, content_sha256 FROM mem_tree_summaries WHERE id = ?1",
                [&summary.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .filter(|(path, hash)| !path.is_empty() && !hash.is_empty());
        if let Some((path, sha256)) = pointer {
            batch.summary_content.push(LegacySummaryContent {
                summary_id: summary.id.clone(),
                path,
                sha256,
            });
        }
    }

    let mut occurrences = conn.prepare(
        "SELECT entity_id, node_id, node_kind, entity_kind, surface, score, timestamp_ms, \
         tree_id, is_user FROM mem_tree_entity_index ORDER BY entity_id, node_id",
    )?;
    batch.entities = occurrences
        .query_map([], |row| {
            Ok(LegacyEntityOccurrence {
                entity_id: row.get(0)?,
                node_id: row.get(1)?,
                node_kind: row.get(2)?,
                entity_kind: row.get(3)?,
                surface: row.get(4)?,
                score: row.get(5)?,
                timestamp_ms: row.get(6)?,
                tree_id: row.get(7)?,
                is_user: row.get::<_, i64>(8)? != 0,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut edges = conn.prepare(
        "SELECT entity_a, entity_b, weight, updated_ms FROM mem_tree_entity_edges ORDER BY entity_a, entity_b",
    )?;
    batch.edges = edges
        .query_map([], |row| {
            Ok(LegacyEntityEdge {
                entity_a: row.get(0)?,
                entity_b: row.get(1)?,
                weight: row.get(2)?,
                updated_at_ms: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut source_gates = conn.prepare(
        "SELECT source_kind, source_id, ingested_at_ms FROM mem_tree_ingested_sources ORDER BY source_kind, source_id",
    )?;
    batch.source_gates = source_gates
        .query_map([], |row| {
            Ok(LegacySourceGate {
                source_kind: SourceKind::parse(&row.get::<_, String>(0)?).map_err(to_sql_error)?,
                source_id: row.get(1)?,
                ingested_at_ms: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut jobs = conn.prepare(
        "SELECT id, kind, payload_json, dedupe_key, status, attempts, max_attempts, \
         available_at_ms, created_at_ms, started_at_ms, completed_at_ms, last_error, \
         failure_reason, failure_class FROM mem_tree_jobs ORDER BY id",
    )?;
    batch.jobs = jobs
        .query_map([], |row| {
            Ok(ImportedJob {
                id: row.get(0)?,
                kind: JobKind::parse(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                payload_json: row.get(2)?,
                dedupe_key: row.get(3)?,
                status: JobStatus::parse(&row.get::<_, String>(4)?).map_err(to_sql_error)?,
                attempts: row.get::<_, i64>(5)? as u32,
                max_attempts: row.get::<_, i64>(6)? as u32,
                available_at_ms: row.get(7)?,
                created_at_ms: row.get(8)?,
                started_at_ms: row.get(9)?,
                completed_at_ms: row.get(10)?,
                last_error: row.get(11)?,
                failure_reason: row.get(12)?,
                failure_class: row.get(13)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut summary_embeddings = conn.prepare(
        "SELECT summary_id, model_signature, vector, created_at FROM mem_tree_summary_embeddings ORDER BY summary_id, model_signature",
    )?;
    batch.summary_embeddings = summary_embeddings
        .query_map([], |row| {
            let created_at: f64 = row.get(3)?;
            Ok(LegacySummaryEmbedding {
                summary_id: row.get(0)?,
                model_signature: row.get(1)?,
                vector: decode_f32(row.get(2)?).map_err(to_sql_error)?,
                created_at_ms: (created_at * 1000.0) as i64,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(batch)
}

fn read_chunk(
    row: &Row<'_>,
) -> rusqlite::Result<(
    Chunk,
    Option<LegacyContentPointer>,
    Vec<RawRef>,
    Option<String>,
)> {
    let source_kind = SourceKind::parse(&row.get::<_, String>(1)?).map_err(to_sql_error)?;
    let timestamp_ms: i64 = row.get(6)?;
    let source_ref: Option<String> = row.get(4)?;
    let tags: String = row.get(9)?;
    let content_path: Option<String> = row.get(15)?;
    let content_sha256: Option<String> = row.get(16)?;
    let raw_refs: Option<String> = row.get(17)?;
    let timestamp = datetime(timestamp_ms).map_err(to_sql_error)?;
    Ok((
        Chunk {
            id: row.get(0)?,
            content: row.get(10)?,
            metadata: chunks::Metadata {
                source_kind,
                source_id: row.get(2)?,
                owner: row.get(5)?,
                timestamp,
                time_range: (
                    datetime(row.get(7)?).map_err(to_sql_error)?,
                    datetime(row.get(8)?).map_err(to_sql_error)?,
                ),
                tags: serde_json::from_str(&tags).map_err(to_sql_error)?,
                source_ref: source_ref.map(chunks::SourceRef::new),
                path_scope: row.get(3)?,
            },
            token_count: row.get::<_, i64>(11)? as u32,
            seq_in_source: row.get::<_, i64>(12)? as u32,
            created_at: datetime(row.get(13)?).map_err(to_sql_error)?,
            partial_message: false,
        },
        content_path
            .zip(content_sha256)
            .filter(|(path, hash)| !path.is_empty() && !hash.is_empty())
            .map(|(path, sha256)| LegacyContentPointer { path, sha256 }),
        raw_refs
            .filter(|refs| !refs.is_empty())
            .map(|refs| serde_json::from_str(&refs).map_err(to_sql_error))
            .transpose()?
            .unwrap_or_default(),
        row.get(14)?,
    ))
}

fn read_embeddings(conn: &Connection, chunk_id: &str) -> Result<Vec<LegacyEmbedding>> {
    let mut statement = conn.prepare(
        "SELECT model_signature, vector, created_at FROM mem_tree_chunk_embeddings WHERE chunk_id = ?1 ORDER BY model_signature",
    )?;
    let rows = statement.query_map([chunk_id], |row| {
        let created_at: f64 = row.get(2)?;
        Ok(LegacyEmbedding {
            model_signature: row.get(0)?,
            vector: decode_f32(row.get(1)?).map_err(to_sql_error)?,
            created_at_ms: (created_at * 1000.0) as i64,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn assert_score_eq(actual: &ScoreRow, expected: &ScoreRow) {
    assert_eq!(actual.chunk_id, expected.chunk_id);
    assert_eq!(actual.total, expected.total);
    assert_eq!(actual.signals.token_count, expected.signals.token_count);
    assert_eq!(actual.signals.unique_words, expected.signals.unique_words);
    assert_eq!(
        actual.signals.metadata_weight,
        expected.signals.metadata_weight
    );
    assert_eq!(actual.signals.source_weight, expected.signals.source_weight);
    assert_eq!(actual.signals.interaction, expected.signals.interaction);
    assert_eq!(
        actual.signals.entity_density,
        expected.signals.entity_density
    );
    assert_eq!(actual.dropped, expected.dropped);
    assert_eq!(actual.reason, expected.reason);
    assert_eq!(actual.computed_at_ms, expected.computed_at_ms);
}

fn datetime(timestamp_ms: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(timestamp_ms)
        .with_context(|| format!("invalid fixture timestamp {timestamp_ms}"))
}

fn decode_f32(bytes: Vec<u8>) -> Result<Vec<f32>> {
    anyhow::ensure!(
        bytes.len() % 4 == 0,
        "legacy embedding byte length is not divisible by four"
    );
    Ok(bytes
        .chunks_exact(4)
        .map(|part| f32::from_le_bytes([part[0], part[1], part[2], part[3]]))
        .collect())
}

fn to_sql_error(error: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(error.to_string())),
    )
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else {
            std::fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}
