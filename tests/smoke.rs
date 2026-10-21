use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use tempfile::TempDir;
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::ingest::canonicalize::chat::{ChatBatch, ChatMessage};
use tinycortex::memory::ingest::{ingest_chat, QueueJobSink};
use tinycortex::memory::queue::{
    drain_until_idle, AppendDecision, AppendTarget, DelegateOutput, ExtractDecision, JobMutation,
    JobStateCollection, NodeRef, QueueDelegates, ReembedProgress, SealDocumentPayload, SealPayload,
    StaleBuffer,
};
use tinycortex::memory::score::ScoringConfig;
use tinycortex::memory::tree::{ConcatSummariser, SealObserver};
use tinycortex::memory::{InMemoryMemoryStore, MemoryInput, MemoryQuery, MemoryStore};
use tinystoragedrivers_types::Scope;

struct EngineDelegates;
struct IgnoreSealObserver;

impl SealObserver for IgnoreSealObserver {}

#[async_trait]
impl QueueDelegates for EngineDelegates {
    async fn extract_chunk(
        &self,
        config: &MemoryConfig,
        chunk_id: &str,
    ) -> anyhow::Result<Option<DelegateOutput<ExtractDecision>>> {
        Ok(
            tinycortex::memory::chunks::get_chunk_async(config, chunk_id)
                .await?
                .map(|chunk| DelegateOutput {
                    result: ExtractDecision {
                        kept: true,
                        uses_document_subtree: false,
                        tree_scope: chunk.metadata.source_id,
                    },
                    mutations: vec![JobMutation {
                        collection: JobStateCollection::ChunkLifecycle,
                        key: chunk.id,
                        expected: Some(serde_json::json!({
                            "status": tinycortex::memory::chunks::CHUNK_STATUS_PENDING_EXTRACTION
                        })),
                        value: Some(serde_json::json!({
                            "status": tinycortex::memory::chunks::CHUNK_STATUS_ADMITTED
                        })),
                    }],
                }),
        )
    }

    async fn append_node(
        &self,
        config: &MemoryConfig,
        node: &NodeRef,
        target: &AppendTarget,
    ) -> anyhow::Result<Option<DelegateOutput<AppendDecision>>> {
        let Some(output) =
            tinycortex::memory::tree::plan_append_node_for_queue(config, node, target).await?
        else {
            return Ok(None);
        };
        Ok(Some(output))
    }

    async fn seal_level(
        &self,
        config: &MemoryConfig,
        payload: &SealPayload,
    ) -> anyhow::Result<DelegateOutput<Option<SealPayload>>> {
        let observer = IgnoreSealObserver;
        tinycortex::memory::tree::plan_tree_level_seal_for_queue(
            config,
            &payload.tree_id,
            payload.level,
            &tinycortex::memory::tree::SealServices {
                summariser: &ConcatSummariser::new(),
                embedder: None,
                observer: &observer,
            },
            &tinycortex::memory::tree::LabelStrategy::Empty,
        )
        .await
    }

    async fn list_stale_buffers(
        &self,
        _config: &MemoryConfig,
        _max_age_secs: i64,
    ) -> anyhow::Result<Vec<StaleBuffer>> {
        Ok(vec![])
    }

    async fn seal_document(
        &self,
        _config: &MemoryConfig,
        _payload: &SealDocumentPayload,
    ) -> anyhow::Result<DelegateOutput<tinycortex::memory::tree::DocumentSealPlanResult>> {
        anyhow::bail!("document sealing is not used by this smoke fixture")
    }

    async fn reembed_batch(
        &self,
        _config: &MemoryConfig,
        _signature: &str,
    ) -> anyhow::Result<DelegateOutput<ReembedProgress>> {
        Ok(DelegateOutput {
            result: ReembedProgress::Covered,
            mutations: Vec::new(),
        })
    }

    fn active_signature(&self, _config: &MemoryConfig) -> String {
        "inert".into()
    }

    async fn has_uncovered_reembed_work(
        &self,
        _config: &MemoryConfig,
        _signature: &str,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn stores_and_finds_memory() {
    let store = InMemoryMemoryStore::new();
    store
        .insert(MemoryInput::new(
            "default",
            "TinyCortex starts as a Rust memory core",
        ))
        .await
        .expect("insert memory");

    let hits = store
        .search(MemoryQuery::text("Rust memory"))
        .await
        .expect("search memory");

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].record.namespace, "default");
}

#[tokio::test]
async fn ingests_and_retrieves_chunk_data_with_host_storage() {
    let workspace = TempDir::new().unwrap();
    let scope = Scope::new("tinycortex-smoke").unwrap();
    let mut config = MemoryConfig::open_with_storage(
        workspace.path(),
        tinystoragedrivers_testkit::memory_storage(&scope),
    )
    .await
    .unwrap();
    config.tree.input_token_budget = 1;
    let batch = ChatBatch {
        platform: "slack".into(),
        channel_label: "#launch".into(),
        messages: vec![ChatMessage {
            author: "alice".into(),
            timestamp: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
            text: "The Phoenix launch is Friday after the staging review and runbook sign-off."
                .into(),
            source_ref: Some("slack://launch/1".into()),
        }],
    };

    let ingested = ingest_chat(
        &config,
        "slack:#launch",
        "alice",
        vec![],
        batch,
        &QueueJobSink,
        &ScoringConfig::from_memory_config(&config),
    )
    .await
    .unwrap();
    assert_eq!(ingested.chunks_written, 1);
    assert_eq!(ingested.extract_jobs_enqueued, 1);

    drain_until_idle(&config, &EngineDelegates).await.unwrap();
    let stored = tinycortex::memory::chunks::get_chunk_async(&config, &ingested.chunk_ids[0])
        .await
        .unwrap()
        .expect("ingested chunk remains readable after worker settlement");
    assert!(stored.content.contains("Phoenix launch"));
    let tree = tinycortex::memory::tree::store::get_tree_by_scope_async(
        &config,
        tinycortex::memory::tree::TreeKind::Source,
        "slack:#launch",
    )
    .await
    .unwrap()
    .expect("worker append creates the source tree");
    let summaries =
        tinycortex::memory::tree::store::list_summaries_at_level_async(&config, &tree.id, 1)
            .await
            .unwrap();
    assert_eq!(summaries.len(), 1, "worker seal publishes the L1 summary");
}
