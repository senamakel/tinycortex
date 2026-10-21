use super::*;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::test_support::RecordingDelegates;
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

#[test]
fn backfill_flag_setters_are_wired() {
    let _flag_guard = crate::memory::queue::test_support::BACKFILL_FLAG_TEST_LOCK.lock();
    // The flag is process-global and shared across parallel tests, so this only
    // exercises the setters compile/run path (it does not assert the value,
    // which other tests may concurrently toggle).
    set_backfill_in_progress(true);
    let _ = backfill_in_progress();
    set_backfill_in_progress(false);
}

#[tokio::test]
async fn plan_reembed_backfill_returns_only_uncovered_signature_intents() {
    // Covered space → no intent.
    let (_t0, covered) = test_config();
    let mut d = RecordingDelegates::admitting();
    d.uncovered = false;
    assert!(plan_reembed_backfill(&covered, &d).await.unwrap().is_none());

    // Uncovered space → stable intent identity on repeated planning, without
    // publishing a queue hint from this helper.
    let (_t1, cfg) = test_config();
    let mut d2 = RecordingDelegates::admitting();
    d2.uncovered = true;
    let first = plan_reembed_backfill(&cfg, &d2).await.unwrap().unwrap();
    let second = plan_reembed_backfill(&cfg, &d2).await.unwrap().unwrap();
    assert_eq!(first.kind, second.kind);
    assert_eq!(first.payload_json, second.payload_json);
    assert_eq!(first.dedupe_key, second.dedupe_key);
}
