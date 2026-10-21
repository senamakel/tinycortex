use super::*;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::handlers::{ExtractDecision, ReembedProgress};
use crate::memory::queue::store::{
    count_by_status_async, count_total_async, enqueue_async, insert_test_record_async,
};
use crate::memory::queue::test_support::RecordingDelegates;
use crate::memory::queue::types::{ExtractChunkPayload, JobStatus, NewJob};
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let mut cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    // Drain tests enqueue LLM-bound jobs (ExtractChunk / ReembedBackfill /
    // Seal). `run_once` routes them to a process-wide gate keyed by
    // `queue.llm_permits`, so the default of `1` would share a single slot
    // with the worker gate-semantics tests, which can briefly hold the sole
    // permit and defer this test's job (`llm concurrency gate busy`), ending
    // the drain early. Use a distinct count so these tests get their own
    // gate and never contend for the shared slot.
    cfg.queue.llm_permits = crate::memory::queue::DEFAULT_LLM_PERMITS.saturating_mul(16);
    (tmp, cfg)
}

#[tokio::test]
async fn drain_until_idle_is_noop_when_queue_is_empty() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    drain_until_idle(&cfg, &d).await.unwrap();
}

/// The whole pipeline drains: one `extract_chunk` fans out to `append_buffer`
/// then `seal`, and all three settle as `done`.
#[tokio::test]
async fn drain_runs_the_full_extract_append_seal_pipeline() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();

    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c1".into(),
    })
    .unwrap();
    enqueue_async(&cfg, &nj).await.unwrap().unwrap();

    drain_until_idle(&cfg, &d).await.unwrap();

    use std::sync::atomic::Ordering::Relaxed;
    assert_eq!(d.counts.extract.load(Relaxed), 1, "extracted once");
    assert_eq!(d.counts.append.load(Relaxed), 1, "appended once");
    assert_eq!(d.counts.seal.load(Relaxed), 1, "sealed once");
    // extract + append + seal all settled done; nothing left ready/running.
    assert_eq!(
        count_by_status_async(&cfg, JobStatus::Done).await.unwrap(),
        3
    );
    assert_eq!(
        count_by_status_async(&cfg, JobStatus::Ready).await.unwrap(),
        0
    );
}

/// A deferring job parks itself and the drain terminates (it is no longer
/// immediately claimable), without burning its retry budget.
#[tokio::test]
async fn drain_terminates_on_a_deferred_job() {
    use crate::memory::queue::types::ReembedBackfillPayload;
    let _flag_guard = crate::memory::queue::test_support::BACKFILL_FLAG_TEST_LOCK
        .lock()
        .await;
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    *d.reembed.lock() =
        std::collections::VecDeque::from([ReembedProgress::Wrote { more_pending: true }]);

    let nj = NewJob::reembed_backfill(&ReembedBackfillPayload {
        signature: d.signature.clone(),
    })
    .unwrap();
    enqueue_async(&cfg, &nj).await.unwrap().unwrap();

    drain_until_idle(&cfg, &d).await.unwrap();
    // The reembed batch ran exactly once, then the deferred row parked.
    assert_eq!(
        d.counts.reembed.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        count_by_status_async(&cfg, JobStatus::Ready).await.unwrap(),
        1
    );
}

/// Retired-kind rows left in an old queue must not crash the drain: they are
/// skipped on claim, the live work still completes, and `purge_retired_jobs`
/// removes them afterwards.
#[tokio::test]
async fn drain_tolerates_retired_kind_rows() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();

    insert_test_record_async(
        &cfg,
        &crate::memory::queue::store::JobRecord {
            id: "retired".to_owned(),
            kind: "digest_daily".to_owned(),
            payload_json: "{}".to_owned(),
            dedupe_key: None,
            status: JobStatus::Ready.as_str().to_owned(),
            attempts: 0,
            max_attempts: 5,
            available_at_ms: 0,
            locked_until_ms: None,
            last_error: None,
            failure_reason: None,
            failure_class: None,
            created_at_ms: 0,
            started_at_ms: None,
            completed_at_ms: None,
            hint_generation: 0,
            lease_epoch: 0,
            lease_token: None,
        },
    )
    .await
    .unwrap();

    // A live, terminal job (drops → no follow-ups) so the drain is bounded.
    let mut dd = RecordingDelegates::admitting();
    dd.extract = Some(ExtractDecision {
        kept: false,
        uses_document_subtree: false,
        tree_scope: "slack:#eng".into(),
    });
    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c1".into(),
    })
    .unwrap();
    enqueue_async(&cfg, &nj).await.unwrap().unwrap();

    // Drain must not panic on the retired row.
    drain_until_idle(&cfg, &dd).await.unwrap();
    assert_eq!(
        count_by_status_async(&cfg, JobStatus::Done).await.unwrap(),
        1
    );

    // The retired row is still present (skipped, not processed) until purged.
    assert_eq!(count_total_async(&cfg).await.unwrap(), 2);
    assert_eq!(
        crate::memory::queue::store::purge_retired_jobs_async(&cfg)
            .await
            .unwrap(),
        1
    );
    assert_eq!(count_total_async(&cfg).await.unwrap(), 1);

    let _ = &d; // silence unused in case of edits
}
