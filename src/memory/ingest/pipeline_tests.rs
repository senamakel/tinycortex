use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use chrono::{TimeZone, Utc};
use tempfile::TempDir;

use super::{ingest_chat, ingest_document, ingest_document_versioned, ingest_email_with_raw_refs};
use crate::memory::chunks::{
    count_chunks, get_chunk_content_pointers, get_chunk_lifecycle_status, is_source_ingested,
    SourceKind, CHUNK_STATUS_PENDING_EXTRACTION,
};
use crate::memory::chunks::{get_chunk_raw_refs, RawRef};
use crate::memory::config::MemoryConfig;
use crate::memory::ingest::canonicalize::chat::{ChatBatch, ChatMessage};
use crate::memory::ingest::canonicalize::document::DocumentInput;
use crate::memory::ingest::canonicalize::email::{EmailMessage, EmailThread};
use crate::memory::ingest::types::{NullJobSink, QueueJobSink, QueuePublication, TreeJobSink};
use crate::memory::queue::test_support::RecordingDelegates;
use crate::memory::queue::QUEUE_NAME;
use crate::memory::score::ScoringConfig;

/// Records every enqueued extract job id for assertions.
#[derive(Default)]
struct RecordingJobSink {
    ids: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TreeJobSink for RecordingJobSink {
    async fn enqueue_extract(
        &self,
        _config: &MemoryConfig,
        _intent_id: &str,
        chunk_id: &str,
        _default_max_attempts: u32,
    ) -> anyhow::Result<QueuePublication> {
        self.ids.lock().unwrap().push(chunk_id.to_string());
        Ok(QueuePublication::Published)
    }
}

/// Sink that fails enqueue while `fail` is set, to simulate a post-gate stage
/// error. Records ids on the successful path so a retry can be asserted.
#[derive(Default)]
struct TogglingJobSink {
    fail: AtomicBool,
    ids: Mutex<Vec<String>>,
}

#[derive(Default)]
struct LostQueueAckSink {
    lose_next_ack: AtomicBool,
}

#[async_trait::async_trait]
impl TreeJobSink for LostQueueAckSink {
    async fn enqueue_extract(
        &self,
        config: &MemoryConfig,
        intent_id: &str,
        chunk_id: &str,
        default_max_attempts: u32,
    ) -> anyhow::Result<QueuePublication> {
        let publication = QueueJobSink
            .enqueue_extract(config, intent_id, chunk_id, default_max_attempts)
            .await?;
        if publication == QueuePublication::Published
            && self.lose_next_ack.swap(false, Ordering::SeqCst)
        {
            anyhow::bail!("simulated lost queue acknowledgement after enqueue");
        }
        Ok(publication)
    }
}

#[async_trait::async_trait]
impl TreeJobSink for TogglingJobSink {
    async fn enqueue_extract(
        &self,
        _config: &MemoryConfig,
        _intent_id: &str,
        chunk_id: &str,
        _default_max_attempts: u32,
    ) -> anyhow::Result<QueuePublication> {
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("simulated enqueue failure");
        }
        self.ids.lock().unwrap().push(chunk_id.to_string());
        Ok(QueuePublication::Published)
    }
}

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn on_plain_thread<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("sync storage assertion thread")
}

fn substantive_batch() -> ChatBatch {
    ChatBatch {
        platform: "slack".into(),
        channel_label: "#eng".into(),
        messages: vec![
            ChatMessage {
                author: "alice".into(),
                timestamp: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
                text: "We are planning to ship the Phoenix migration on Friday after reviewing the runbook and staging results. alice@example.com"
                    .into(),
                source_ref: Some("slack://m1".into()),
            },
            ChatMessage {
                author: "bob".into(),
                timestamp: Utc.timestamp_millis_opt(1_700_000_010_000).unwrap(),
                text: "Confirmed, I will handle the coordination and launch tracking tonight."
                    .into(),
                source_ref: None,
            },
        ],
    }
}

#[tokio::test]
async fn ingest_chat_writes_chunks_and_enqueues_extract_jobs() {
    let (_tmp, cfg) = test_config();
    let sink = RecordingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();

    let out = ingest_chat(
        &cfg,
        "slack:#eng",
        "alice",
        vec![],
        substantive_batch(),
        &sink,
        &scoring,
    )
    .await
    .unwrap();

    assert!(!out.already_ingested);
    assert!(out.chunks_written >= 1);
    let read_cfg = cfg.clone();
    assert_eq!(on_plain_thread(move || count_chunks(&read_cfg)).unwrap(), out.chunks_written as u64);
    assert_eq!(out.chunk_ids.len(), out.chunks_written);
    let read_cfg = cfg.clone();
    let chunk_id = out.chunk_ids[0].clone();
    let pointers = on_plain_thread(move || get_chunk_content_pointers(&read_cfg, &chunk_id))
        .unwrap()
        .unwrap();
    assert!(!pointers.0.is_empty());
    assert!(!pointers.1.is_empty());

    // Every scheduled chunk got an extract job and is parked at pending.
    assert_eq!(out.extract_jobs_enqueued, out.chunk_ids.len());
    assert_eq!(sink.ids.lock().unwrap().len(), out.extract_jobs_enqueued);
    let read_cfg = cfg.clone();
    let chunk_id = out.chunk_ids[0].clone();
    assert_eq!(
        on_plain_thread(move || get_chunk_lifecycle_status(&read_cfg, &chunk_id)).unwrap(),
        Some(CHUNK_STATUS_PENDING_EXTRACTION.to_string())
    );
}

#[tokio::test]
async fn queue_sink_commits_chunk_lifecycle_and_extract_job_together() {
    let (_tmp, cfg) = test_config();
    let out = ingest_chat(
        &cfg,
        "slack:#atomic",
        "alice",
        vec![],
        substantive_batch(),
        &QueueJobSink,
        &ScoringConfig::default_regex_only(),
    )
    .await
    .unwrap();

    assert_eq!(out.extract_jobs_enqueued, out.chunk_ids.len());
    assert_eq!(
        ready_queue_messages(&cfg).await,
        out.extract_jobs_enqueued as u64
    );
    for chunk_id in out.chunk_ids {
        let read_cfg = cfg.clone();
        assert_eq!(
            on_plain_thread(move || get_chunk_lifecycle_status(&read_cfg, &chunk_id))
                .unwrap()
                .as_deref(),
            Some(CHUNK_STATUS_PENDING_EXTRACTION)
        );
    }
}

#[tokio::test]
async fn ingest_chat_empty_batch_is_noop() {
    let (_tmp, cfg) = test_config();
    let sink = NullJobSink;
    let scoring = ScoringConfig::default_regex_only();
    let batch = ChatBatch {
        platform: "slack".into(),
        channel_label: "#eng".into(),
        messages: vec![],
    };
    let out = ingest_chat(&cfg, "slack:#eng", "alice", vec![], batch, &sink, &scoring)
        .await
        .unwrap();
    assert_eq!(out.chunks_written, 0);
    assert_eq!(out.extract_jobs_enqueued, 0);
    let read_cfg = cfg.clone();
    assert_eq!(on_plain_thread(move || count_chunks(&read_cfg)).unwrap(), 0);
}

#[tokio::test]
async fn second_document_ingest_with_same_source_id_is_short_circuited() {
    let (_tmp, cfg) = test_config();
    let sink = RecordingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();

    let doc = DocumentInput {
        provider: "notion".into(),
        title: "Launch plan".into(),
        body: "Phoenix ships Friday after staging review. alice@example.com owns this.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        source_ref: Some("notion://page/abc".into()),
    };
    let first = ingest_document(
        &cfg,
        "notion:abc",
        "alice",
        vec![],
        doc.clone(),
        &sink,
        &scoring,
    )
    .await
    .unwrap();
    assert!(!first.already_ingested);
    assert!(first.chunks_written >= 1);
    let staged_files_before = walkdir::WalkDir::new(crate::memory::chunks::content_root(&cfg))
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .count();

    // Even with completely different content under the same source_id the second
    // ingest must write nothing: documents are append-only and source_id is the
    // dedup key.
    let mutated = DocumentInput {
        body: "totally different content that should NOT make it into the tree".into(),
        ..doc
    };
    let second = ingest_document(
        &cfg,
        "notion:abc",
        "alice",
        vec![],
        mutated,
        &sink,
        &scoring,
    )
    .await
    .unwrap();
    assert!(second.already_ingested);
    assert_eq!(second.chunks_written, 0);
    assert!(second.chunk_ids.is_empty());

    let read_cfg = cfg.clone();
    assert_eq!(on_plain_thread(move || count_chunks(&read_cfg)).unwrap(), first.chunks_written as u64);
    let staged_files_after = walkdir::WalkDir::new(crate::memory::chunks::content_root(&cfg))
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .count();
    assert_eq!(
        staged_files_after, staged_files_before,
        "a losing gate attempt must reclaim its unreferenced staged bodies"
    );
}

#[tokio::test]
async fn failed_queue_hint_publication_keeps_committed_intent_recoverable() {
    let (_tmp, cfg) = test_config();
    let sink = TogglingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();

    let doc = DocumentInput {
        provider: "notion".into(),
        title: "Launch plan".into(),
        body: "Phoenix ships Friday after staging review. alice@example.com owns this.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        source_ref: Some("notion://page/abc".into()),
    };

    // The source gate, staged chunks, and queue intent commit together. A
    // failure publishing the non-authoritative queue hint leaves the durable
    // intent available for repair.
    sink.fail.store(true, Ordering::SeqCst);
    let err = ingest_document(
        &cfg,
        "notion:abc",
        "alice",
        vec![],
        doc.clone(),
        &sink,
        &scoring,
    )
    .await;
    assert!(err.is_err(), "forced enqueue failure should surface as Err");
    let read_cfg = cfg.clone();
    assert!(on_plain_thread(move || is_source_ingested(&read_cfg, SourceKind::Document, "notion:abc"))
        .unwrap()
        .unwrap());
    let read_cfg = cfg.clone();
    let persisted_chunks = on_plain_thread(move || count_chunks(&read_cfg)).unwrap().unwrap() as usize;
    assert!(persisted_chunks > 0, "the committed source has chunks");
    assert_eq!(
        super::dispatch_pending_extract_jobs_async(&cfg, &NullJobSink)
            .await
            .unwrap(),
        0,
        "a sink that declines work must leave another sink's intent pending"
    );
    let unrelated = ingest_chat(
        &cfg,
        "slack:#null-sink",
        "alice",
        vec![],
        substantive_batch(),
        &NullJobSink,
        &scoring,
    )
    .await
    .unwrap();
    assert_eq!(unrelated.extract_jobs_enqueued, 0);
    assert_eq!(
        ready_queue_messages(&cfg).await,
        0,
        "an unrelated no-queue ingest cannot acknowledge another source's intent"
    );

    // Recovery can publish the pending intent without replaying source parsing
    // or scoring, and repeating the dispatcher does not enqueue it again.
    sink.fail.store(false, Ordering::SeqCst);
    assert_eq!(
        super::dispatch_pending_extract_jobs_async(&cfg, &sink)
            .await
            .unwrap(),
        persisted_chunks
    );
    assert_eq!(
        super::dispatch_pending_extract_jobs_async(&cfg, &sink)
            .await
            .unwrap(),
        0
    );
    let retry = ingest_document(&cfg, "notion:abc", "alice", vec![], doc, &sink, &scoring)
        .await
        .unwrap();
    assert!(
        retry.already_ingested,
        "the source gate committed with the recoverable job intent",
    );
    assert_eq!(sink.ids.lock().unwrap().len(), persisted_chunks);
    let read_cfg = cfg.clone();
    assert!(on_plain_thread(move || is_source_ingested(&read_cfg, SourceKind::Document, "notion:abc"))
        .unwrap()
        .unwrap());
}

#[tokio::test]
async fn ingest_head_failure_does_not_publish_or_expose_its_outbox_intent() {
    use std::sync::Arc;
    use tinystoragedrivers_api::Storage;
    use tinystoragedrivers_testkit::Failing;
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let tmp = TempDir::new().unwrap();
    let scope = Scope::new("test:ingest-before-head-failure").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let failing = Failing::new(raw.documents().clone());
    let faults = failing.faults.clone();
    let storage = Storage::new(
        Arc::new(failing),
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    let cfg = MemoryConfig::open_with_storage(tmp.path(), storage)
        .await
        .unwrap();
    faults.fail_operation_nth("documents.put_with", 1, ErrorKind::Unavailable);

    let sink = RecordingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();
    let doc = DocumentInput {
        provider: "notion".into(),
        title: "Uncommitted plan".into(),
        body: "Phoenix ships Friday after staging review.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        source_ref: Some("notion://page/uncommitted".into()),
    };
    assert!(ingest_document(
        &cfg,
        "notion:uncommitted",
        "alice",
        vec![],
        doc,
        &sink,
        &scoring
    )
    .await
    .is_err());
    assert!(sink.ids.lock().unwrap().is_empty());
    let read_cfg = cfg.clone();
    assert_eq!(on_plain_thread(move || count_chunks(&read_cfg)).unwrap(), 0);
    let read_cfg = cfg.clone();
    assert!(!on_plain_thread(move || is_source_ingested(
        &read_cfg,
        SourceKind::Document,
        "notion:uncommitted",
    ))
    .unwrap()
    .unwrap());
}

#[tokio::test]
async fn current_thread_ingest_repairs_lost_hint_ack_and_worker_claims_owner_job() {
    let (_tmp, cfg) = test_config();
    let sink = LostQueueAckSink {
        lose_next_ack: AtomicBool::new(true),
    };
    let scoring = ScoringConfig::default_regex_only();
    let doc = DocumentInput {
        provider: "notion".into(),
        title: "Current thread queue".into(),
        body: "Phoenix ships Friday after staging review.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        source_ref: Some("notion://page/current-thread".into()),
    };
    assert!(ingest_document(
        &cfg,
        "notion:current-thread",
        "alice",
        vec![],
        doc,
        &sink,
        &scoring
    )
    .await
    .is_err());
    let expected_jobs = crate::memory::chunks::versioned::list_state_async(
        &cfg,
        crate::memory::chunks::collections::CHUNK_LIFECYCLE,
    )
    .await
    .unwrap()
    .len() as u64;
    assert!(expected_jobs > 0);
    let published_before_repair = ready_queue_messages(&cfg).await;
    assert!(published_before_repair > 0);
    assert!(published_before_repair <= expected_jobs);

    let repaired = super::dispatch_pending_extract_jobs_async(&cfg, &QueueJobSink)
        .await
        .unwrap();
    assert_eq!(published_before_repair + repaired as u64, expected_jobs);
    assert_eq!(ready_queue_messages(&cfg).await, expected_jobs);
    let records =
        crate::memory::chunks::versioned::list_state_async(&cfg, crate::memory::queue::store::JOBS)
            .await
            .unwrap();
    let extract_records: Vec<_> = records
        .into_iter()
        .filter(|(_, record)| record["kind"] == "extract_chunk")
        .collect();
    assert_eq!(extract_records.len() as u64, expected_jobs);

    let delegates = RecordingDelegates::admitting();
    assert!(crate::memory::queue::worker::run_once(&cfg, &delegates)
        .await
        .unwrap());
    assert_eq!(delegates.counts.extract.load(Ordering::Relaxed), 1);
    let job_id = extract_records[0].1["id"].as_str().unwrap();
    assert_eq!(
        crate::memory::queue::store::get_job_async(&cfg, job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crate::memory::queue::types::JobStatus::Done
    );
    let gate_key = crate::memory::chunks::store_sources::source_gate_key(
        SourceKind::Document,
        "notion:current-thread",
    );
    assert!(crate::memory::chunks::versioned::read_state_async(
        &cfg,
        crate::memory::chunks::collections::INGESTED_SOURCES,
        &gate_key,
    )
    .await
    .unwrap()
    .is_some());
}

async fn ready_queue_messages(config: &MemoryConfig) -> u64 {
    config
        .storage()
        .unwrap()
        .queues()
        .stats(QUEUE_NAME)
        .await
        .unwrap()
        .ready
}

#[tokio::test]
async fn versioned_document_admits_second_revision() {
    let (_tmp, cfg) = test_config();
    let sink = RecordingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();

    let doc_v1 = DocumentInput {
        provider: "notion".into(),
        title: "Roadmap".into(),
        body: "Phoenix ships Friday. alice@example.com owns the rollout.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        source_ref: Some("notion://page/v".into()),
    };
    let v1 = ingest_document_versioned(
        &cfg,
        "notion:v",
        "alice",
        vec![],
        doc_v1,
        None,
        Some(1),
        &sink,
        &scoring,
    )
    .await
    .unwrap();
    assert!(!v1.already_ingested);
    assert!(v1.chunks_written >= 1);

    let doc_v2 = DocumentInput {
        provider: "notion".into(),
        title: "Roadmap".into(),
        body: "Phoenix now ships next Monday. bob@example.com owns the rollout.".into(),
        modified_at: Utc.timestamp_millis_opt(1_700_000_100_000).unwrap(),
        source_ref: Some("notion://page/v".into()),
    };
    let v2 = ingest_document_versioned(
        &cfg,
        "notion:v",
        "alice",
        vec![],
        doc_v2,
        None,
        Some(2),
        &sink,
        &scoring,
    )
    .await
    .unwrap();
    assert!(!v2.already_ingested, "a new version must be admitted");
    assert!(v2.chunks_written >= 1);
}

#[tokio::test]
async fn ingest_email_with_raw_refs_attaches_archive_refs_to_every_chunk() {
    let (_tmp, cfg) = test_config();
    let sink = RecordingJobSink::default();
    let scoring = ScoringConfig::default_regex_only();
    let thread = EmailThread {
        provider: "gmail".into(),
        thread_subject: "Launch review".into(),
        messages: vec![
            EmailMessage {
                from: "Alice Smith <alice@example.com>".into(),
                to: vec!["Bob Jones <bob@example.com>".into()],
                cc: vec![],
                subject: "Launch review".into(),
                sent_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
                body: "Alice sent the launch checklist to Bob. Kitchen is north of Garden.".into(),
                source_ref: Some("gmail://msg-1".into()),
                list_unsubscribe: None,
            },
            EmailMessage {
                from: "Bob Jones <bob@example.com>".into(),
                to: vec!["Alice Smith <alice@example.com>".into()],
                cc: vec![],
                subject: "Re: Launch review".into(),
                sent_at: Utc.timestamp_millis_opt(1_700_000_060_000).unwrap(),
                body: "Bob confirmed the checklist and added staging notes.".into(),
                source_ref: Some("gmail://msg-2".into()),
                list_unsubscribe: None,
            },
        ],
    };
    let raw_refs = vec![
        RawRef {
            path: "raw/gmail/thread-1/message-1.md".into(),
            start: 0,
            end: Some(128),
        },
        RawRef {
            path: "raw/gmail/thread-1/message-2.md".into(),
            start: 128,
            end: None,
        },
    ];

    let out = ingest_email_with_raw_refs(
        &cfg,
        "gmail:thread-1",
        "alice",
        vec!["mock-email".into()],
        thread,
        raw_refs.clone(),
        &sink,
        &scoring,
    )
    .await
    .unwrap();

    assert!(!out.already_ingested);
    assert!(out.chunks_written >= 1);
    assert_eq!(out.extract_jobs_enqueued, out.chunk_ids.len());
    for chunk_id in &out.chunk_ids {
        let read_cfg = cfg.clone();
        let read_id = chunk_id.clone();
        let stored = on_plain_thread(move || get_chunk_raw_refs(&read_cfg, &read_id))
            .unwrap()
            .unwrap();
        assert_eq!(stored.len(), raw_refs.len());
        assert_eq!(stored[0].path, raw_refs[0].path);
        assert_eq!(stored[1].start, raw_refs[1].start);
    }
}
