use super::*;
use crate::memory::chunks::with_connection;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::types::{
    AppendBufferPayload, AppendTarget, ExtractChunkPayload, FlushStalePayload, NodeRef,
    ReembedBackfillPayload, SealDocumentPayload, SealPayload,
};
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = MemoryConfig::new(tmp.path());
    (tmp, cfg)
}

#[test]
fn enqueue_and_claim_roundtrip() {
    let (_tmp, cfg) = test_config();
    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c1".into(),
    })
    .unwrap();
    let id = enqueue(&cfg, &nj).unwrap().expect("inserted");

    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.status, JobStatus::Running);
    assert_eq!(claimed.attempts, 1);
    assert!(claimed.locked_until_ms.is_some());

    // Second claim should see no eligible row (the only one is now running).
    let again = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap();
    assert!(again.is_none());
}

#[test]
fn typed_failure_columns_roundtrip_as_none_by_default() {
    let (_tmp, cfg) = test_config();
    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c-typed".into(),
    })
    .unwrap();
    let id = enqueue(&cfg, &nj).unwrap().expect("inserted");

    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(claimed.failure_reason, None);
    assert_eq!(claimed.failure_class, None);

    let row = get_job(&cfg, &id).unwrap().unwrap();
    assert_eq!(row.failure_reason, None);
    assert_eq!(row.failure_class, None);
}

#[test]
fn enqueue_dedupes_active_jobs() {
    let (_tmp, cfg) = test_config();
    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c1".into(),
    })
    .unwrap();
    let id1 = enqueue(&cfg, &nj).unwrap();
    let id2 = enqueue(&cfg, &nj).unwrap();
    assert!(id1.is_some());
    assert!(id2.is_none(), "duplicate should be suppressed while ready");
    assert_eq!(count_total(&cfg).unwrap(), 1);
}

#[test]
fn enqueue_after_done_creates_fresh_row() {
    use crate::memory::queue::store_settle::mark_done;
    let (_tmp, cfg) = test_config();
    let nj = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c1".into(),
    })
    .unwrap();
    let id1 = enqueue(&cfg, &nj).unwrap().unwrap();
    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(claimed.id, id1);
    mark_done(&cfg, &claimed).unwrap();

    // The dedupe key is free now (the partial index excludes 'done').
    let id2 = enqueue(&cfg, &nj).unwrap();
    assert!(id2.is_some());
    assert_ne!(id2.unwrap(), id1);
    assert_eq!(count_total(&cfg).unwrap(), 2);
}

#[test]
fn stable_outbox_hint_id_suppresses_republication_after_job_completion() {
    use crate::memory::queue::store_settle::mark_done;
    let (_tmp, cfg) = test_config();
    let mut job = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c-outbox".into(),
    })
    .unwrap();
    job.id = Some("job:extract:stable-intent".into());
    let id = enqueue(&cfg, &job).unwrap().unwrap();
    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    mark_done(&cfg, &claimed).unwrap();

    assert_eq!(enqueue(&cfg, &job).unwrap(), None);
    assert_eq!(count_total(&cfg).unwrap(), 1);
    assert_eq!(get_job(&cfg, &id).unwrap().unwrap().status, JobStatus::Done);
}

#[test]
fn count_by_status_reports_each_state() {
    use crate::memory::queue::store_settle::mark_done;
    let (_tmp, cfg) = test_config();
    for i in 0..3 {
        let nj = NewJob::extract_chunk(&ExtractChunkPayload {
            chunk_id: format!("c{i}"),
        })
        .unwrap();
        enqueue(&cfg, &nj).unwrap();
    }
    assert_eq!(count_by_status(&cfg, JobStatus::Ready).unwrap(), 3);
    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    mark_done(&cfg, &claimed).unwrap();
    assert_eq!(count_by_status(&cfg, JobStatus::Done).unwrap(), 1);
    assert_eq!(count_by_status(&cfg, JobStatus::Ready).unwrap(), 2);
}

#[test]
fn backoff_grows_then_caps() {
    assert_eq!(backoff_ms(1), 60_000);
    assert_eq!(backoff_ms(2), 120_000);
    assert_eq!(backoff_ms(3), 240_000);
    assert_eq!(backoff_ms(20), RETRY_CAP_MS);
    assert_eq!(backoff_ms(99), RETRY_CAP_MS);
}

/// Retired-kind tolerance: a leftover `topic_route` / `digest_daily` row (from
/// before the global/topic trees were removed) must NOT be claimed (which would
/// crash `row_to_job` on the unknown kind), and `purge_retired_jobs` removes it
/// while leaving live rows untouched.
#[test]
fn retired_kind_rows_are_skipped_then_purged() {
    let (_tmp, cfg) = test_config();

    // Insert two raw retired rows directly (no NewJob path exists for them).
    with_connection(&cfg, |conn| {
        for (id, kind) in [
            ("job:retired-1", "topic_route"),
            ("job:retired-2", "digest_daily"),
        ] {
            conn.execute(
                "INSERT INTO mem_tree_jobs (id, kind, payload_json, status, attempts,
                    max_attempts, available_at_ms, created_at_ms)
                 VALUES (?1, ?2, '{}', 'ready', 0, 5, 0, 0)",
                params![id, kind],
            )?;
        }
        Ok(())
    })
    .unwrap();

    // A live job alongside them.
    let live = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "live".into(),
    })
    .unwrap();
    let live_id = enqueue(&cfg, &live).unwrap().unwrap();

    // claim_next must pick the live row and never crash on the retired ones.
    let claimed = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(claimed.id, live_id);
    assert_eq!(claimed.kind, JobKind::ExtractChunk);
    // No further claimable rows (retired ones are excluded).
    assert!(claim_next(&cfg, DEFAULT_LOCK_DURATION_MS)
        .unwrap()
        .is_none());

    // Purge removes exactly the two retired rows; the live row stays.
    let purged = purge_retired_jobs(&cfg).unwrap();
    assert_eq!(purged, 2);
    assert_eq!(count_total(&cfg).unwrap(), 1);
    assert!(get_job(&cfg, &live_id).unwrap().is_some());
}

#[test]
fn is_retired_kind_recognises_legacy_strings() {
    assert!(is_retired_kind("topic_route"));
    assert!(is_retired_kind("digest_daily"));
    assert!(!is_retired_kind("extract_chunk"));
    assert!(!is_retired_kind("seal"));
}

/// tinyhumansai/tinycortex#168: the deduped `reembed_backfill` row is the only
/// writer of chunk vectors and shares the LLM gate with every `extract_chunk`.
/// It must be claimed ahead of an *older* due `extract_chunk`, otherwise the
/// gate-busy defer round-robins it behind the whole extraction backlog.
#[test]
fn claim_next_prefers_reembed_backfill_over_older_extract_chunk() {
    let (_tmp, cfg) = test_config();
    let now_ms = Utc::now().timestamp_millis();

    let mut extract = NewJob::extract_chunk(&ExtractChunkPayload {
        chunk_id: "c-older".into(),
    })
    .unwrap();
    // Due well before the backfill row, so an age-ordered claim would pick it.
    extract.available_at_ms = Some(now_ms - 5_000);
    let extract_id = enqueue(&cfg, &extract).unwrap().expect("inserted");

    let backfill = NewJob::reembed_backfill(&ReembedBackfillPayload {
        signature: "provider=test;model=x;dims=3".into(),
    })
    .unwrap();
    let backfill_id = enqueue(&cfg, &backfill).unwrap().expect("inserted");

    let first = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(first.id, backfill_id);
    assert_eq!(first.kind, JobKind::ReembedBackfill);

    let second = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap().unwrap();
    assert_eq!(second.id, extract_id);
    assert_eq!(second.kind, JobKind::ExtractChunk);

    assert!(claim_next(&cfg, DEFAULT_LOCK_DURATION_MS)
        .unwrap()
        .is_none());
}

/// Pins the whole claim ladder: `seal` > `reembed_backfill` > `flush_stale` >
/// `append_buffer` > everything else, and `available_at_ms` (oldest first)
/// only inside a rank. Rows are enqueued in the reverse of the expected claim
/// order with strictly *older* due times, so a FIFO / age-ordered claim would
/// return them in enqueue order and fail.
#[test]
fn claim_next_ranks_seal_then_backfill_then_flush_then_append_then_age() {
    let (_tmp, cfg) = test_config();
    let now_ms = Utc::now().timestamp_millis();

    // (job, age_ms): a larger age is an older, earlier-due row.
    let mut jobs = [
        (
            NewJob::extract_chunk(&ExtractChunkPayload {
                chunk_id: "c1".into(),
            })
            .unwrap(),
            60_000,
        ),
        (
            NewJob::seal_document(&SealDocumentPayload {
                tree_scope: "gmail:acct".into(),
                doc_id: "doc-1".into(),
                version_ms: None,
                chunk_ids: vec!["c1".into()],
            })
            .unwrap(),
            50_000,
        ),
        (
            NewJob::append_buffer(&AppendBufferPayload {
                node: NodeRef::Leaf {
                    chunk_id: "c1".into(),
                },
                target: AppendTarget::Source {
                    source_id: "src-1".into(),
                },
            })
            .unwrap(),
            40_000,
        ),
        (
            NewJob::flush_stale(&FlushStalePayload::default(), "2026-09-07", 4).unwrap(),
            30_000,
        ),
        (
            NewJob::reembed_backfill(&ReembedBackfillPayload {
                signature: "provider=test;model=x;dims=3".into(),
            })
            .unwrap(),
            20_000,
        ),
        (
            NewJob::seal(&SealPayload {
                tree_id: "tree:1".into(),
                level: 0,
                force_now_ms: None,
            })
            .unwrap(),
            10_000,
        ),
    ];
    for (job, age_ms) in jobs.iter_mut() {
        job.available_at_ms = Some(now_ms - *age_ms);
        enqueue(&cfg, job).unwrap().expect("inserted");
    }

    let mut claimed = Vec::new();
    while let Some(job) = claim_next(&cfg, DEFAULT_LOCK_DURATION_MS).unwrap() {
        claimed.push(job.kind);
    }
    assert_eq!(
        claimed,
        [
            JobKind::Seal,
            JobKind::ReembedBackfill,
            JobKind::FlushStale,
            JobKind::AppendBuffer,
            // ELSE bucket: oldest `available_at_ms` first.
            JobKind::ExtractChunk,
            JobKind::SealDocument,
        ]
    );
}
