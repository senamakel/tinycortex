//! Host-owned document persistence and brute-force cosine search for vectors.
//!
//! Each vector is one document in a prepared TinyCortex collection. Embedding
//! bytes are stored exactly as little-endian `f32` bytes (represented as a JSON
//! byte array), and metadata remains raw JSON text until after top-K selection.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinystoragedrivers_api::{DocumentStore, DocumentStoreExt};
use tinystoragedrivers_types::{Filter, Precondition, Query, Versioned, WriteOp};

use super::embedding::EmbeddingBackend;
use crate::memory::{config::MemoryConfig, storage::Blocking};

/// Collection containing one record per `(namespace, id)` vector.
pub const VECTOR_RECORDS: &str = "memory_vectors";
/// Collection containing the embedding backend compatibility record.
pub const VECTOR_METADATA: &str = "memory_vector_metadata";
const VECTOR_META_ID: &str = "embedding-config";
const LEGACY_SOURCE_META_PREFIX: &str = "legacy-source-";
const QUERY_PAGE_SIZE: usize = 512;

/// Collections owned by vector persistence. Hosts prepare these before use.
#[must_use]
pub fn collections() -> Vec<tinystoragedrivers_types::CollectionSpec> {
    use tinystoragedrivers_types::{CollectionSpec, IndexSpec};
    vec![
        CollectionSpec::new(VECTOR_RECORDS).index(IndexSpec::new("by_namespace", ["namespace"])),
        CollectionSpec::new(VECTOR_METADATA),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorRecord {
    namespace: String,
    id: String,
    text: String,
    dimensions: usize,
    embedding_le: Vec<u8>,
    /// Keep this raw so invalid legacy metadata can fall back to JSON null
    /// after top-K, without making one malformed candidate break the search.
    metadata_json: String,
    created_at: f64,
    updated_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorMetadata {
    /// Informational provider name from the writer that initialized the store.
    backend_name: String,
    dimensions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyVectorMetadataRecord {
    provider_name: String,
    dimensions: usize,
}

/// Source embedding metadata read by a host adapter from a legacy vector DB.
/// The provider is provenance only; compatibility is guarded by dimensions,
/// matching the historical store's reopen behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyVectorMetadata {
    /// Historical provider name from the source database.
    pub provider_name: String,
    /// Dimension count recorded by the historical source store.
    pub dimensions: usize,
}

/// One vector row decoded by a host's legacy storage reader.
///
/// `embedding_le` and `metadata_json` remain opaque so import preserves every
/// float bit and the source metadata string without normalizing either value.
#[derive(Debug, Clone)]
pub struct LegacyVectorRecord {
    /// Historical namespace.
    pub namespace: String,
    /// Historical logical vector ID.
    pub id: String,
    /// Stored document text.
    pub text: String,
    /// Dimension count stored with this vector.
    pub dimensions: usize,
    /// Original little-endian `f32` bytes.
    pub embedding_le: Vec<u8>,
    /// Raw serialized JSON metadata from the source row.
    pub metadata_json: String,
    /// Original source creation timestamp in seconds since epoch.
    pub created_at: f64,
    /// Original source update timestamp in seconds since epoch.
    pub updated_at: f64,
}

/// A single search result from the vector store.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The stored document ID.
    pub id: String,
    /// The namespace.
    pub namespace: String,
    /// The original text.
    pub text: String,
    /// Cosine similarity score (`-1.0` through `1.0`).
    pub score: f64,
    /// Arbitrary JSON metadata attached at insert time. Invalid stored JSON is
    /// returned as `Null`, matching the historical behavior.
    pub metadata: serde_json::Value,
}

/// Vector store over host-supplied, prepared document storage.
///
/// The store never opens a database or creates an in-memory production
/// fallback. `MemoryConfig::prepare_storage` declares its collections, and
/// this constructor validates the persisted embedding dimension. The provider
/// name is retained as provenance; as in the historical SQLite store, it does
/// not prevent reopening with a different provider of the same dimension.
pub struct VectorStore {
    documents: Arc<dyn DocumentStore>,
    blocking: Blocking,
    backend: Arc<dyn EmbeddingBackend>,
}

impl VectorStore {
    /// Attach the vector store to a host-prepared memory configuration.
    pub fn from_config(
        config: &MemoryConfig,
        backend: Arc<dyn EmbeddingBackend>,
    ) -> anyhow::Result<Self> {
        let documents = Arc::clone(config.storage()?.documents());
        let blocking = config.storage_blocking()?.clone();
        let expected = VectorMetadata {
            backend_name: backend.name().to_owned(),
            dimensions: backend.dimensions(),
        };
        let docs = Arc::clone(&documents);
        let stored_expected = expected.clone();
        blocking
            .run(async move { check_or_store_metadata(docs.as_ref(), stored_expected).await })
            .map_err(anyhow::Error::from)??;
        Ok(Self {
            documents,
            blocking,
            backend,
        })
    }

    /// Async constructor for hosts running on a current-thread Tokio runtime.
    pub async fn from_config_async(
        config: &MemoryConfig,
        backend: Arc<dyn EmbeddingBackend>,
    ) -> anyhow::Result<Self> {
        let documents = Arc::clone(config.storage()?.documents());
        let blocking = config.storage_blocking()?.clone();
        check_or_store_metadata(
            documents.as_ref(),
            VectorMetadata {
                backend_name: backend.name().to_owned(),
                dimensions: backend.dimensions(),
            },
        )
        .await?;
        Ok(Self {
            documents,
            blocking,
            backend,
        })
    }

    /// Returns a reference to the embedding backend.
    pub fn backend(&self) -> &dyn EmbeddingBackend {
        self.backend.as_ref()
    }

    /// Inserts or updates a text entry after embedding it.
    pub async fn insert(
        &self,
        id: &str,
        namespace: &str,
        text: &str,
        metadata: serde_json::Value,
    ) -> anyhow::Result<()> {
        let embedding = self.backend.embed_one(text).await?;
        self.insert_with_vector_async(id, namespace, text, &embedding, metadata)
            .await
    }

    /// Inserts with a pre-computed embedding vector.
    pub fn insert_with_vector(
        &self,
        id: &str,
        namespace: &str,
        text: &str,
        embedding: &[f32],
        metadata: serde_json::Value,
    ) -> anyhow::Result<()> {
        validate_embedding(embedding, self.backend.dimensions())?;
        let documents = Arc::clone(&self.documents);
        let record = make_record(id, namespace, text, embedding, metadata)?;
        self.blocking
            .run(async move { put_record(documents.as_ref(), record).await })
            .map_err(anyhow::Error::from)?
    }

    /// Async counterpart to [`Self::insert_with_vector`] for current-thread
    /// runtimes.
    pub async fn insert_with_vector_async(
        &self,
        id: &str,
        namespace: &str,
        text: &str,
        embedding: &[f32],
        metadata: serde_json::Value,
    ) -> anyhow::Result<()> {
        validate_embedding(embedding, self.backend.dimensions())?;
        let record = make_record(id, namespace, text, embedding, metadata)?;
        put_record(self.documents.as_ref(), record).await
    }

    /// Bulk-insert entries atomically. The backend must return one embedding
    /// per input and the host document driver must support atomic batches.
    pub async fn insert_batch(
        &self,
        namespace: &str,
        entries: &[(&str, &str, serde_json::Value)],
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let texts = entries.iter().map(|(_, text, _)| *text).collect::<Vec<_>>();
        let embeddings = self.backend.embed(&texts).await?;
        anyhow::ensure!(
            embeddings.len() == entries.len(),
            "embedding count mismatch: got {} embeddings for {} entries",
            embeddings.len(),
            entries.len()
        );
        let mut ops = Vec::with_capacity(entries.len());
        let now = now_ts();
        for ((id, text, metadata), embedding) in entries.iter().zip(embeddings) {
            validate_embedding(&embedding, self.backend.dimensions())?;
            let record = make_record_at(id, namespace, text, &embedding, metadata.clone(), now)?;
            ops.push(WriteOp::Put {
                collection: VECTOR_RECORDS.to_owned(),
                id: vector_document_id(namespace, id),
                doc: serde_json::to_value(record)?,
                precondition: Precondition::None,
            });
        }
        self.documents
            .atomic_batch(ops)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    /// Import rows decoded from a legacy SQLite vector store.
    ///
    /// The host owns reading the old database and supplies typed records here.
    /// Dimensions must match both the source metadata and this store's active
    /// backend. Identical retries are no-ops; a conflicting existing logical
    /// ID fails before any new row is published. New rows and source-provider
    /// provenance are written in one atomic document batch.
    pub async fn import_legacy_records(
        &self,
        source: &LegacyVectorMetadata,
        records: &[LegacyVectorRecord],
    ) -> anyhow::Result<usize> {
        anyhow::ensure!(
            source.dimensions == self.backend.dimensions(),
            "legacy vector dimension mismatch: source has {}, active backend uses {}",
            source.dimensions,
            self.backend.dimensions()
        );

        let mut seen = BTreeSet::new();
        let mut validated = Vec::with_capacity(records.len());
        for record in records {
            anyhow::ensure!(
                record.dimensions == source.dimensions,
                "legacy vector {} dimension metadata does not match source",
                record.id
            );
            anyhow::ensure!(
                record.embedding_le.len() == record.dimensions.saturating_mul(4),
                "legacy vector {} has an invalid packed embedding length",
                record.id
            );
            let key = vector_document_id(&record.namespace, &record.id);
            anyhow::ensure!(
                seen.insert(key.clone()),
                "legacy vector batch repeats logical ID {} in namespace {}",
                record.id,
                record.namespace
            );
            validated.push((key, VectorRecord {
                namespace: record.namespace.clone(),
                id: record.id.clone(),
                text: record.text.clone(),
                dimensions: record.dimensions,
                embedding_le: record.embedding_le.clone(),
                metadata_json: record.metadata_json.clone(),
                created_at: record.created_at,
                updated_at: record.updated_at,
            }));
        }

        let provenance_id = legacy_source_metadata_id(source);
        for _ in 0..3 {
            let mut ops = Vec::with_capacity(validated.len() + 1);
            let mut inserted = 0;
            for (key, record) in &validated {
                match self
                    .documents
                    .get_as::<VectorRecord>(VECTOR_RECORDS, key)
                    .await?
                {
                    Some(existing) => {
                        anyhow::ensure!(
                            same_record(&existing.doc, record),
                            "legacy vector import conflicts with existing ID {} in namespace {}",
                            record.id,
                            record.namespace
                        );
                    }
                    None => {
                        ops.push(WriteOp::Put {
                            collection: VECTOR_RECORDS.to_owned(),
                            id: key.clone(),
                            doc: serde_json::to_value(record)?,
                            precondition: Precondition::Absent,
                        });
                        inserted += 1;
                    }
                }
            }

            let source_doc = LegacyVectorMetadataRecord {
                provider_name: source.provider_name.clone(),
                dimensions: source.dimensions,
            };
            match self
                .documents
                .get_as::<LegacyVectorMetadataRecord>(VECTOR_METADATA, &provenance_id)
                .await?
            {
                Some(existing) => anyhow::ensure!(
                    existing.doc == source_doc,
                    "legacy vector source metadata conflicts with existing provenance"
                ),
                None => ops.push(WriteOp::Put {
                    collection: VECTOR_METADATA.to_owned(),
                    id: provenance_id.clone(),
                    doc: serde_json::to_value(source_doc)?,
                    precondition: Precondition::Absent,
                }),
            }

            if ops.is_empty() {
                return Ok(0);
            }
            match self.documents.atomic_batch(ops).await {
                Ok(()) => return Ok(inserted),
                Err(error)
                    if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict =>
                {
                    // A concurrent identical importer may have won. Re-read
                    // and compare all affected rows before accepting a retry.
                }
                Err(error) => return Err(anyhow::Error::from(error)),
            }
        }
        anyhow::bail!("legacy vector import remained contended after retries")
    }

    /// Searches by embedding text, using the configured backend.
    pub async fn search(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<SearchResult>> {
        let query_vec = self.backend.embed_one(query).await?;
        self.search_by_vector_async(namespace, &query_vec, limit)
            .await
    }

    /// Searches using a pre-computed query vector.
    pub fn search_by_vector(
        &self,
        namespace: &str,
        query_vec: &[f32],
        limit: usize,
    ) -> anyhow::Result<Vec<SearchResult>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let documents = Arc::clone(&self.documents);
        let namespace = namespace.to_owned();
        let query_vec = query_vec.to_vec();
        self.blocking
            .run(async move {
                search_documents(documents.as_ref(), &namespace, &query_vec, limit).await
            })
            .map_err(anyhow::Error::from)?
    }

    /// Async counterpart to [`Self::search_by_vector`] for current-thread
    /// runtimes.
    pub async fn search_by_vector_async(
        &self,
        namespace: &str,
        query_vec: &[f32],
        limit: usize,
    ) -> anyhow::Result<Vec<SearchResult>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        search_documents(self.documents.as_ref(), namespace, query_vec, limit).await
    }

    /// Deletes an entry by ID within a namespace; returns whether it existed.
    pub fn delete(&self, namespace: &str, id: &str) -> anyhow::Result<bool> {
        let documents = Arc::clone(&self.documents);
        let namespace = namespace.to_owned();
        let id = id.to_owned();
        self.blocking
            .run(async move { delete_record(documents.as_ref(), &namespace, &id).await })
            .map_err(anyhow::Error::from)?
    }

    /// Async deletion for callers on current-thread runtimes.
    pub async fn delete_async(&self, namespace: &str, id: &str) -> anyhow::Result<bool> {
        delete_record(self.documents.as_ref(), namespace, id).await
    }

    /// Deletes all vectors in a namespace and returns the number removed.
    pub fn clear_namespace(&self, namespace: &str) -> anyhow::Result<usize> {
        let documents = Arc::clone(&self.documents);
        let namespace = namespace.to_owned();
        self.blocking
            .run(async move { clear_namespace_documents(documents.as_ref(), &namespace).await })
            .map_err(anyhow::Error::from)?
    }

    /// Async namespace clearing for callers on current-thread runtimes.
    pub async fn clear_namespace_async(&self, namespace: &str) -> anyhow::Result<usize> {
        clear_namespace_documents(self.documents.as_ref(), namespace).await
    }

    /// Returns the number of vectors in one namespace (or all namespaces).
    pub fn count(&self, namespace: Option<&str>) -> anyhow::Result<usize> {
        let documents = Arc::clone(&self.documents);
        let filter = namespace.map_or(Filter::All, |namespace| {
            Filter::eq("namespace", namespace.to_owned())
        });
        self.blocking
            .run(async move { count_documents(documents.as_ref(), &filter).await })
            .map_err(anyhow::Error::from)?
    }

    /// Async count for callers on current-thread runtimes.
    pub async fn count_async(&self, namespace: Option<&str>) -> anyhow::Result<usize> {
        let filter = namespace.map_or(Filter::All, |namespace| {
            Filter::eq("namespace", namespace.to_owned())
        });
        count_documents(self.documents.as_ref(), &filter).await
    }

    /// Lists namespaces in lexical order.
    pub fn list_namespaces(&self) -> anyhow::Result<Vec<String>> {
        let documents = Arc::clone(&self.documents);
        self.blocking
            .run(async move { list_namespaces_documents(documents.as_ref()).await })
            .map_err(anyhow::Error::from)?
    }

    /// Async namespace listing for callers on current-thread runtimes.
    pub async fn list_namespaces_async(&self) -> anyhow::Result<Vec<String>> {
        list_namespaces_documents(self.documents.as_ref()).await
    }
}

async fn clear_namespace_documents(
    documents: &dyn DocumentStore,
    namespace: &str,
) -> anyhow::Result<usize> {
    let removed = documents
        .delete_where(
            VECTOR_RECORDS,
            &Filter::eq("namespace", namespace.to_owned()),
        )
        .await?;
    usize::try_from(removed).context("vector count did not fit in usize")
}

async fn count_documents(documents: &dyn DocumentStore, filter: &Filter) -> anyhow::Result<usize> {
    let count = documents.count(VECTOR_RECORDS, filter).await?;
    usize::try_from(count).context("vector count did not fit in usize")
}

async fn list_namespaces_documents(documents: &dyn DocumentStore) -> anyhow::Result<Vec<String>> {
    let rows = query_all(documents, Filter::All).await?;
    let mut namespaces = BTreeSet::new();
    for row in rows {
        let record = decode_record(row)?;
        namespaces.insert(record.namespace);
    }
    Ok(namespaces.into_iter().collect())
}

async fn check_or_store_metadata(
    documents: &dyn DocumentStore,
    expected: VectorMetadata,
) -> tinystoragedrivers_types::Result<()> {
    match documents
        .get_as::<VectorMetadata>(VECTOR_METADATA, VECTOR_META_ID)
        .await?
    {
        Some(stored) => validate_metadata(&stored.doc, &expected),
        None => match documents
            .put_as(
                VECTOR_METADATA,
                VECTOR_META_ID,
                &expected,
                Precondition::Absent,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                let stored = documents
                    .get_as::<VectorMetadata>(VECTOR_METADATA, VECTOR_META_ID)
                    .await?
                    .ok_or_else(|| {
                        tinystoragedrivers_types::StorageError::conflict(
                            "vector metadata changed during initialization",
                        )
                    })?;
                validate_metadata(&stored.doc, &expected)
            }
            Err(error) => Err(error),
        },
    }
}

fn validate_metadata(
    stored: &VectorMetadata,
    expected: &VectorMetadata,
) -> tinystoragedrivers_types::Result<()> {
    if stored.dimensions != expected.dimensions {
        return Err(tinystoragedrivers_types::StorageError::invalid_input(format!(
            "vector store dimension mismatch: database was created with {}-dim embeddings but the current backend ({}) uses {} dims",
            stored.dimensions, expected.backend_name, expected.dimensions
        )));
    }
    Ok(())
}

fn legacy_source_metadata_id(source: &LegacyVectorMetadata) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tinycortex-legacy-vector-source-v1\0");
    digest.update((source.provider_name.len() as u64).to_le_bytes());
    digest.update(source.provider_name.as_bytes());
    digest.update((source.dimensions as u64).to_le_bytes());
    format!("{LEGACY_SOURCE_META_PREFIX}{:x}", digest.finalize())
}

fn same_record(left: &VectorRecord, right: &VectorRecord) -> bool {
    left.namespace == right.namespace
        && left.id == right.id
        && left.text == right.text
        && left.dimensions == right.dimensions
        && left.embedding_le == right.embedding_le
        && left.metadata_json == right.metadata_json
        && left.created_at.to_bits() == right.created_at.to_bits()
        && left.updated_at.to_bits() == right.updated_at.to_bits()
}

fn validate_embedding(embedding: &[f32], dimensions: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        embedding.len() == dimensions,
        "embedding dimension mismatch: expected {dimensions}, got {}",
        embedding.len()
    );
    Ok(())
}

fn make_record(
    id: &str,
    namespace: &str,
    text: &str,
    embedding: &[f32],
    metadata: Value,
) -> anyhow::Result<VectorRecord> {
    make_record_at(id, namespace, text, embedding, metadata, now_ts())
}

fn make_record_at(
    id: &str,
    namespace: &str,
    text: &str,
    embedding: &[f32],
    metadata: Value,
    now: f64,
) -> anyhow::Result<VectorRecord> {
    anyhow::ensure!(!id.is_empty(), "vector id must not be empty");
    let metadata_json = serde_json::to_string(&metadata)?;
    Ok(VectorRecord {
        namespace: namespace.to_owned(),
        id: id.to_owned(),
        text: text.to_owned(),
        dimensions: embedding.len(),
        embedding_le: vec_to_bytes(embedding),
        metadata_json,
        created_at: now,
        updated_at: now,
    })
}

async fn put_record(documents: &dyn DocumentStore, record: VectorRecord) -> anyhow::Result<()> {
    documents
        .put_as(
            VECTOR_RECORDS,
            &vector_document_id(&record.namespace, &record.id),
            &record,
            Precondition::None,
        )
        .await
        .map_err(anyhow::Error::from)?;
    Ok(())
}

async fn delete_record(
    documents: &dyn DocumentStore,
    namespace: &str,
    id: &str,
) -> anyhow::Result<bool> {
    let document_id = vector_document_id(namespace, id);
    let Some(stored) = documents.get(VECTOR_RECORDS, &document_id).await? else {
        return Ok(false);
    };
    let record = decode_record(stored.clone())?;
    anyhow::ensure!(
        record.namespace == namespace && record.id == id,
        "vector document identity mismatch"
    );
    documents
        .delete(
            VECTOR_RECORDS,
            &document_id,
            Precondition::Version(stored.version),
        )
        .await
        .map_err(anyhow::Error::from)
}

async fn query_all(
    documents: &dyn DocumentStore,
    filter: Filter,
) -> tinystoragedrivers_types::Result<Vec<Versioned<Value>>> {
    let mut query = Query::filter(filter).limit(QUERY_PAGE_SIZE);
    let mut all = Vec::new();
    let mut cursor_seen = BTreeSet::new();
    loop {
        let page = documents.query(VECTOR_RECORDS, &query).await?;
        all.extend(page.items);
        match page.next {
            Some(cursor) => {
                if !cursor_seen.insert(cursor.0.clone()) {
                    return Err(tinystoragedrivers_types::StorageError::backend(
                        "vector query returned a repeated cursor",
                    ));
                }
                query = query.after(cursor);
            }
            None => break,
        }
    }
    Ok(all)
}

async fn search_documents(
    documents: &dyn DocumentStore,
    namespace: &str,
    query_vec: &[f32],
    limit: usize,
) -> anyhow::Result<Vec<SearchResult>> {
    let rows = query_all(documents, Filter::eq("namespace", namespace.to_owned())).await?;
    struct ScoredRow {
        score: f64,
        record: VectorRecord,
    }
    let mut scored = Vec::with_capacity(rows.len());
    for row in rows {
        let record = decode_record(row)?;
        anyhow::ensure!(
            record.namespace == namespace,
            "vector document namespace mismatch"
        );
        let vector = bytes_to_vec(&record.embedding_le)?;
        anyhow::ensure!(
            vector.len() == record.dimensions,
            "stored vector dimension does not match its payload"
        );
        let score = cosine_similarity(query_vec, &vector);
        scored.push(ScoredRow { score, record });
    }
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            // SQL's historical query had no ORDER BY, so ties were
            // unspecified. Use logical id as a deterministic tie-breaker.
            .then_with(|| a.record.id.cmp(&b.record.id))
    });
    scored.truncate(limit);
    Ok(scored
        .into_iter()
        .map(|row| SearchResult {
            id: row.record.id,
            namespace: row.record.namespace,
            text: row.record.text,
            score: row.score,
            metadata: serde_json::from_str(&row.record.metadata_json)
                .unwrap_or(serde_json::Value::Null),
        })
        .collect())
}

fn decode_record(row: Versioned<Value>) -> anyhow::Result<VectorRecord> {
    let record: VectorRecord = serde_json::from_value(row.doc).context("invalid vector record")?;
    anyhow::ensure!(
        vector_document_id(&record.namespace, &record.id) == row.id,
        "vector document identity does not match its key"
    );
    Ok(record)
}

fn vector_document_id(namespace: &str, id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tinycortex-vector-v1\0");
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    digest.update((id.len() as u64).to_be_bytes());
    digest.update(id.as_bytes());
    format!("vector-{:x}", digest.finalize())
}

/// Serializes a float vector to its exact little-endian byte representation.
pub fn vec_to_bytes(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

/// Deserializes little-endian bytes, rejecting a corrupt partial trailing word.
#[allow(clippy::manual_is_multiple_of)]
pub fn bytes_to_vec(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    anyhow::ensure!(
        bytes.len() % 4 == 0,
        "invalid embedding blob byte length: {}",
        bytes.len()
    );
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect())
}

/// Computes cosine similarity using `f64` accumulation and clamps drift.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0_f64;
    let mut norm_a = 0.0_f64;
    let mut norm_b = 0.0_f64;
    for (x, y) in a.iter().zip(b) {
        let x = f64::from(*x);
        let y = f64::from(*y);
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom <= f64::EPSILON {
        return 0.0;
    }
    (dot / denom).clamp(-1.0, 1.0)
}

fn now_ts() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
