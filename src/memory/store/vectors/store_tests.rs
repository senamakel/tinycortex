use super::*;
use crate::memory::store::vectors::embedding::{
    format_embedding_signature, EmbeddingBackend, InertEmbedding,
};
use serde_json::json;
use std::sync::Arc;
use tinystoragedrivers_types::Precondition;

/// A test embedding backend that returns deterministic vectors.
struct FakeEmbedding {
    dims: usize,
}

#[async_trait::async_trait]
impl EmbeddingBackend for FakeEmbedding {
    fn name(&self) -> &str {
        "fake"
    }
    fn model_id(&self) -> &str {
        "fake"
    }
    fn dimensions(&self) -> usize {
        self.dims
    }
    async fn embed(&self, texts: &[&str]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| text_to_vec(t, self.dims)).collect())
    }
}

fn text_to_vec(text: &str, dims: usize) -> Vec<f32> {
    let mut vec = vec![0.0_f32; dims];
    for (i, byte) in text.bytes().enumerate() {
        vec[i % dims] += byte as f32 / 255.0;
    }
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut vec {
            *x /= norm;
        }
    }
    vec
}

struct MismatchEmbedding;

#[async_trait::async_trait]
impl EmbeddingBackend for MismatchEmbedding {
    fn name(&self) -> &str {
        "mismatch"
    }
    fn model_id(&self) -> &str {
        "mismatch"
    }
    fn dimensions(&self) -> usize {
        2
    }
    async fn embed(&self, _texts: &[&str]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(vec![vec![1.0, 0.0]])
    }
}

fn fake_store(dims: usize) -> VectorStore {
    fake_store_with_backend(Arc::new(FakeEmbedding { dims }))
}

fn fake_store_with_backend(backend: Arc<dyn EmbeddingBackend>) -> VectorStore {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    std::thread::spawn(move || VectorStore::from_config(&config, backend))
        .join()
        .unwrap()
        .unwrap()
}

fn put_raw_record(store: &VectorStore, namespace: &str, id: &str, doc: serde_json::Value) {
    let documents = Arc::clone(&store.documents);
    let document_id = super::vector_document_id(namespace, id);
    store
        .blocking
        .run(async move {
            documents
                .put(super::VECTOR_RECORDS, &document_id, doc, Precondition::None)
                .await
        })
        .unwrap()
        .unwrap();
}

// ── vec_to_bytes / bytes_to_vec ─────────────────────────

#[test]
fn roundtrip_vec_bytes() {
    let original = vec![1.0_f32, -2.5, 3.125, 0.0, f32::MAX, f32::MIN];
    let bytes = vec_to_bytes(&original);
    assert_eq!(bytes.len(), original.len() * 4);
    assert_eq!(original, bytes_to_vec(&bytes).unwrap());
}

#[test]
fn vector_bytes_preserve_all_f32_bit_patterns() {
    let original = [f32::from_bits(0x7fc0_1234), f32::INFINITY, -0.0];
    let decoded = bytes_to_vec(&vec_to_bytes(&original)).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        original
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn empty_vec_roundtrip() {
    assert!(bytes_to_vec(&vec_to_bytes(&[])).unwrap().is_empty());
}

#[test]
fn bytes_to_vec_rejects_partial_bytes() {
    assert!(bytes_to_vec(&[0u8; 5]).is_err());
}

// ── cosine_similarity ───────────────────────────────────

#[test]
fn cosine_identical() {
    let v = vec![1.0_f32, 2.0, 3.0];
    assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
}

#[test]
fn cosine_orthogonal() {
    assert!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
}

#[test]
fn cosine_opposite() {
    assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-6);
}

#[test]
fn cosine_mismatched_lengths() {
    assert_eq!(cosine_similarity(&[1.0, 2.0], &[1.0, 2.0, 3.0]), 0.0);
}

#[test]
fn cosine_empty() {
    assert_eq!(cosine_similarity(&[], &[]), 0.0);
}

#[test]
fn cosine_zero_vector() {
    assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 0.0]), 0.0);
}

#[test]
fn cosine_similar_high() {
    assert!(cosine_similarity(&[1.0, 2.0, 3.0], &[1.1, 2.1, 3.1]) > 0.99);
}

// ── VectorStore: open / metadata ────────────────────────

#[test]
fn prepared_host_store_starts_empty() {
    let store = fake_store(3);
    assert_eq!(store.count(None).unwrap(), 0);
}

#[test]
fn reopening_prepared_store_with_same_backend_succeeds() {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 4 })).unwrap();
    VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 4 })).unwrap();
}

#[test]
fn reopening_prepared_store_with_different_dims_errors() {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 4 })).unwrap();
    let result = VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 8 }));
    let msg = result.err().expect("should be an error").to_string();
    assert!(msg.contains("dimension mismatch"), "msg: {msg}");
    assert!(msg.contains("4"), "should mention stored dims: {msg}");
    assert!(msg.contains("8"), "should mention runtime dims: {msg}");
}

#[test]
fn reopening_prepared_store_with_different_backend_name_and_same_dims_succeeds() {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 2 })).unwrap();
    VectorStore::from_config(&config, Arc::new(MismatchEmbedding))
        .expect("the historical store only guards embedding dimensions");
}

#[test]
fn equal_scores_use_a_deterministic_logical_id_tiebreaker() {
    let store = fake_store(2);
    store
        .insert_with_vector("z-id", "ns", "z", &[1.0, 0.0], json!({}))
        .unwrap();
    store
        .insert_with_vector("a-id", "ns", "a", &[1.0, 0.0], json!({}))
        .unwrap();

    let results = store.search_by_vector("ns", &[1.0, 0.0], 2).unwrap();

    assert_eq!(
        results
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a-id", "z-id"]
    );
}

#[test]
fn open_rejects_corrupt_dimension_metadata() {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    let store = VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 4 })).unwrap();
    let documents = Arc::clone(&store.documents);
    store
        .blocking
        .run(async move {
            documents
                .put(
                    super::VECTOR_METADATA,
                    super::VECTOR_META_ID,
                    json!({ "dimensions": "not-a-number" }),
                    Precondition::None,
                )
                .await
        })
        .unwrap()
        .unwrap();
    let err = VectorStore::from_config(&config, Arc::new(FakeEmbedding { dims: 4 }))
        .err()
        .expect("corrupt metadata must fail closed");
    assert!(err.to_string().contains("invalid type"));
}

#[test]
fn insert_with_vector_rejects_wrong_dimensions() {
    let store = fake_store(3);
    let err = store
        .insert_with_vector("id", "ns", "text", &[1.0, 2.0], json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("expected 3, got 2"));
    assert_eq!(store.count(None).unwrap(), 0);
}

#[test]
fn search_rejects_corrupt_embedding_blob() {
    let store = fake_store(2);
    put_raw_record(
        &store,
        "ns",
        "bad",
        json!({
            "namespace": "ns", "id": "bad", "text": "text", "dimensions": 2,
            "embedding_le": [0, 0, 0, 0, 0], "metadata_json": "{}",
            "created_at": 0.0, "updated_at": 0.0
        }),
    );
    assert!(store.search_by_vector("ns", &[1.0, 0.0], 5).is_err());
}

#[test]
fn backend_accessor() {
    let store = fake_store(3);
    assert_eq!(store.backend().name(), "fake");
    assert_eq!(store.backend().dimensions(), 3);
}

#[test]
fn embedding_signature_format_is_stable() {
    assert_eq!(
        format_embedding_signature("openai", "text-embedding-3-small", 1536),
        "provider=openai;model=text-embedding-3-small;dims=1536"
    );
    let backend = FakeEmbedding { dims: 7 };
    assert_eq!(backend.signature(), "provider=fake;model=fake;dims=7");
}

#[tokio::test(flavor = "current_thread")]
async fn vector_store_uses_the_prepared_host_document_port() {
    let config = crate::memory::chunks::test_support::host_config("unused-vector-workspace");
    let store = VectorStore::from_config_async(&config, Arc::new(FakeEmbedding { dims: 2 }))
        .await
        .unwrap();

    store
        .insert_with_vector_async(
            "host-backed",
            "notes",
            "text",
            &[1.0, 0.0],
            json!({"source": "test"}),
        )
        .await
        .unwrap();

    let results = store
        .search_by_vector_async("notes", &[1.0, 0.0], 1)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "host-backed");
    assert_eq!(results[0].metadata["source"], "test");

    let stored = config
        .storage()
        .unwrap()
        .documents()
        .get(
            super::VECTOR_RECORDS,
            &super::vector_document_id("notes", "host-backed"),
        )
        .await
        .unwrap()
        .unwrap();
    let record: super::VectorRecord = serde_json::from_value(stored.doc).unwrap();
    assert_eq!(record.embedding_le, vec_to_bytes(&[1.0, 0.0]));
}

#[tokio::test]
async fn inert_embedding_is_deterministic_zero_vector_mock_backend() {
    let backend = InertEmbedding::new(3);
    assert_eq!(backend.name(), "inert");
    assert_eq!(backend.model_id(), "inert");
    assert_eq!(backend.dimensions(), 3);
    assert_eq!(backend.signature(), "provider=inert;model=inert;dims=3");

    let batch = backend.embed(&["alpha", "beta"]).await.unwrap();
    assert_eq!(batch, vec![vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]);
    assert_eq!(
        backend.embed_one("single").await.unwrap(),
        vec![0.0, 0.0, 0.0]
    );
}

// ── insert + count ──────────────────────────────────────

#[tokio::test]
async fn insert_and_count() {
    let store = fake_store(4);
    store.insert("a", "ns1", "hello", json!({})).await.unwrap();
    store.insert("b", "ns1", "world", json!({})).await.unwrap();
    store.insert("c", "ns2", "other", json!({})).await.unwrap();
    assert_eq!(store.count_async(Some("ns1")).await.unwrap(), 2);
    assert_eq!(store.count_async(Some("ns2")).await.unwrap(), 1);
    assert_eq!(store.count_async(None).await.unwrap(), 3);
}

#[tokio::test]
async fn insert_upsert_replaces() {
    let store = fake_store(4);
    store
        .insert("a", "ns", "original", json!({"v": 1}))
        .await
        .unwrap();
    store
        .insert("a", "ns", "updated", json!({"v": 2}))
        .await
        .unwrap();
    assert_eq!(store.count_async(Some("ns")).await.unwrap(), 1);
    let results = store
        .search_by_vector_async("ns", &text_to_vec("updated", 4), 10)
        .await
        .unwrap();
    assert_eq!(results[0].text, "updated");
    assert_eq!(results[0].metadata["v"], 2);
}

#[test]
fn insert_with_vector_sync() {
    let store = fake_store(3);
    store
        .insert_with_vector("id1", "ns", "text", &[1.0, 0.0, 0.0], json!({"k": "v"}))
        .unwrap();
    assert_eq!(store.count(Some("ns")).unwrap(), 1);
}

// ── insert_batch ────────────────────────────────────────

#[tokio::test]
async fn insert_batch_multiple() {
    let store = fake_store(4);
    let entries = vec![
        ("a", "alpha", json!({})),
        ("b", "beta", json!({})),
        ("c", "gamma", json!({})),
    ];
    store.insert_batch("ns", &entries).await.unwrap();
    assert_eq!(store.count_async(Some("ns")).await.unwrap(), 3);
}

#[tokio::test]
async fn insert_batch_empty() {
    let store = fake_store(4);
    store.insert_batch("ns", &[]).await.unwrap();
    assert_eq!(store.count_async(None).await.unwrap(), 0);
}

#[tokio::test]
async fn insert_batch_mismatch_error() {
    let store = fake_store_with_backend(Arc::new(MismatchEmbedding));
    let entries = vec![("a", "alpha", json!({})), ("b", "beta", json!({}))];
    let err = store.insert_batch("ns", &entries).await.unwrap_err();
    assert!(err.to_string().contains("mismatch"));
}

// ── search ──────────────────────────────────────────────

#[tokio::test]
async fn search_returns_ranked_results() {
    let store = fake_store(8);
    store
        .insert("a", "ns", "the quick brown fox", json!({}))
        .await
        .unwrap();
    store
        .insert("b", "ns", "a lazy dog sleeps", json!({}))
        .await
        .unwrap();
    store
        .insert("c", "ns", "the quick brown fox jumps", json!({}))
        .await
        .unwrap();
    let results = store.search("ns", "the quick brown fox", 2).await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].score >= results[1].score);
}

#[tokio::test]
async fn search_respects_limit() {
    let store = fake_store(4);
    for i in 0..10 {
        store
            .insert(&format!("id-{i}"), "ns", &format!("text {i}"), json!({}))
            .await
            .unwrap();
    }
    assert_eq!(store.search("ns", "text", 3).await.unwrap().len(), 3);
}

#[tokio::test]
async fn search_empty_namespace() {
    let store = fake_store(4);
    assert!(store.search("empty", "query", 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn search_namespace_isolation() {
    let store = fake_store(4);
    store.insert("a", "ns1", "hello", json!({})).await.unwrap();
    store.insert("b", "ns2", "hello", json!({})).await.unwrap();
    assert_eq!(store.search("ns1", "hello", 10).await.unwrap()[0].id, "a");
    assert_eq!(store.search("ns2", "hello", 10).await.unwrap()[0].id, "b");
}

// ── search_by_vector ────────────────────────────────────

#[test]
fn search_by_vector_limit_zero() {
    let store = fake_store(3);
    store
        .insert_with_vector("a", "ns", "t", &[1.0, 0.0, 0.0], json!({}))
        .unwrap();
    assert!(store
        .search_by_vector("ns", &[1.0, 0.0, 0.0], 0)
        .unwrap()
        .is_empty());
}

/// Metadata is parsed only for rows that survive truncation, so the parse
/// must align with the post-sort order — each returned row must carry its
/// own metadata, and dropped rows must not appear.
#[test]
fn search_by_vector_returns_metadata_of_surviving_rows() {
    let store = fake_store(3);
    store
        .insert_with_vector("near", "ns", "t", &[1.0, 0.0, 0.0], json!({"tag": "near"}))
        .unwrap();
    store
        .insert_with_vector("mid", "ns", "t", &[0.7, 0.7, 0.0], json!({"tag": "mid"}))
        .unwrap();
    store
        .insert_with_vector("far", "ns", "t", &[0.0, 0.0, 1.0], json!({"tag": "far"}))
        .unwrap();

    let results = store.search_by_vector("ns", &[1.0, 0.0, 0.0], 2).unwrap();

    assert_eq!(results.len(), 2, "limit should drop the least similar row");
    assert_eq!(results[0].id, "near");
    assert_eq!(results[1].id, "mid");
    assert!(
        results.iter().all(|hit| hit.id != "far"),
        "the truncated row must not leak into the results"
    );
    for hit in &results {
        assert_eq!(
            hit.metadata.get("tag").and_then(|v| v.as_str()),
            Some(hit.id.as_str()),
            "metadata tag should match the row id for {}",
            hit.id
        );
    }
}

/// A row with corrupt metadata JSON must not break the search — the deferred
/// parse falls back to `Null` rather than dropping the row or erroring.
#[test]
fn search_by_vector_falls_back_to_null_on_invalid_metadata() {
    let store = fake_store(3);
    put_raw_record(
        &store,
        "ns",
        "bad",
        json!({
            "namespace": "ns", "id": "bad", "text": "t", "dimensions": 3,
            "embedding_le": vec_to_bytes(&[1.0, 0.0, 0.0]),
            "metadata_json": "{not valid json", "created_at": 0.0, "updated_at": 0.0
        }),
    );

    let results = store.search_by_vector("ns", &[1.0, 0.0, 0.0], 5).unwrap();

    assert_eq!(results.len(), 1, "the row must still be returned");
    assert_eq!(results[0].id, "bad");
    assert!(
        results[0].metadata.is_null(),
        "invalid metadata json must fall back to Null"
    );
}

#[path = "store_tests_more.rs"]
mod more;
