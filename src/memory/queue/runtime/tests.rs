use std::time::Duration;
use std::{fs, sync::Arc};

use super::*;
use crate::memory::config::MemoryConfig;
use crate::memory::queue::store::{
    count_by_status_async, enqueue_async, get_job_async, update_test_record_async,
};
use crate::memory::queue::test_support::RecordingDelegates;
use crate::memory::queue::types::{FlushStalePayload, JobStatus, NewJob};
use tempfile::TempDir;
use tokio::time::sleep;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_testkit::{memory_storage, Failing};
use tinystoragedrivers_types::{ErrorKind, Scope, StorageError};

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().unwrap();
    let cfg = crate::memory::chunks::test_support::host_config(tmp.path());
    (tmp, cfg)
}

/// Small backoffs so the loops make progress quickly under test.
fn fast_worker_opts() -> WorkerLoopConfig {
    WorkerLoopConfig {
        idle_backoff: Duration::from_millis(2),
        conflict_backoff: Duration::from_millis(2),
        unavailable_backoff: Duration::from_millis(2),
        host_io_backoff: Duration::from_millis(2),
        error_backoff: Duration::from_millis(2),
    }
}

#[tokio::test]
async fn worker_loop_drains_queued_jobs_then_stops_on_shutdown() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    let new_job = NewJob::flush_stale(&FlushStalePayload::default(), "2026-05-24", 3).unwrap();
    enqueue_async(&cfg, &new_job)
        .await
        .unwrap()
        .expect("enqueued");

    let opts = fast_worker_opts();
    let shutdown = Shutdown::new();

    // Concurrent stopper: once the flush job has been settled, ask the worker to
    // stop. `join!` polls both on this task, so no `Send` bound is required.
    let stopper = async {
        loop {
            if count_by_status_async(&cfg, JobStatus::Done).await.unwrap() >= 1 {
                shutdown.trigger();
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    };

    let (worker_res, ()) = tokio::join!(run_worker(&cfg, &d, &opts, &shutdown), stopper);
    worker_res.unwrap();

    assert_eq!(
        count_by_status_async(&cfg, JobStatus::Done).await.unwrap(),
        1
    );
}

#[tokio::test]
async fn worker_loop_returns_immediately_when_already_shut_down() {
    let (_tmp, cfg) = test_config();
    let d = RecordingDelegates::admitting();
    let opts = fast_worker_opts();
    let shutdown = Shutdown::new();
    shutdown.trigger();

    // Pre-triggered: the loop condition is false on entry, so it never polls.
    run_worker(&cfg, &d, &opts, &shutdown).await.unwrap();
    assert!(shutdown.is_triggered());
}

#[tokio::test]
async fn scheduler_loop_enqueues_flush_stale_then_stops() {
    let (_tmp, cfg) = test_config();
    let opts = SchedulerLoopConfig {
        tick: Duration::from_millis(5),
    };
    let shutdown = Shutdown::new();

    let stopper = async {
        loop {
            let queued = count_by_status_async(&cfg, JobStatus::Ready).await.unwrap();
            if queued >= 1 {
                shutdown.trigger();
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    };

    let (sched_res, ()) = tokio::join!(run_scheduler(&cfg, &opts, &shutdown), stopper);
    sched_res.unwrap();

    // At least one flush_stale job was enqueued by the loop.
    assert!(count_by_status_async(&cfg, JobStatus::Ready).await.unwrap() >= 1);
}

#[tokio::test]
async fn scheduler_tick_recovers_an_expired_running_job() {
    let (_tmp, cfg) = test_config();
    let new_job = NewJob::flush_stale(&FlushStalePayload::default(), "expired", 1).unwrap();
    let id = enqueue_async(&cfg, &new_job).await.unwrap().unwrap();
    update_test_record_async(&cfg, &id, |record| {
        record.status = JobStatus::Running.as_str().to_owned();
        record.locked_until_ms = Some(0);
        record.started_at_ms = Some(0);
    })
    .await
    .unwrap();

    let shutdown = Shutdown::new();
    let opts = SchedulerLoopConfig {
        tick: Duration::from_secs(60),
    };
    let stop_after_first_tick = async {
        loop {
            if get_job_async(&cfg, &id).await.unwrap().unwrap().status == JobStatus::Ready {
                shutdown.trigger();
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    let (result, ()) = tokio::join!(run_scheduler(&cfg, &opts, &shutdown), stop_after_first_tick);
    result.unwrap();
    assert_eq!(
        get_job_async(&cfg, &id).await.unwrap().unwrap().status,
        JobStatus::Ready
    );
}

#[test]
fn backoff_classifies_host_io_error_as_long_backoff() {
    let opts = WorkerLoopConfig::default();
    // EIO (raw OS error 5): a persistent host-filesystem failure surfaced as a
    // std::io::Error. It must take the long host-IO arm, not
    // the generic error_backoff.
    let host_io = anyhow::Error::from(std::io::Error::from_raw_os_error(5));
    assert_eq!(backoff_for(&host_io, &opts), Some(opts.host_io_backoff));
    assert_ne!(backoff_for(&host_io, &opts), Some(opts.error_backoff));
}

#[test]
fn backoff_classifies_typed_storage_failures() {
    let opts = WorkerLoopConfig::default();
    let conflict = anyhow::Error::new(StorageError::conflict("stale version"));
    let unavailable = anyhow::Error::new(StorageError::unavailable("temporarily offline"));
    let backend = anyhow::Error::new(StorageError::backend("storage failure"));
    assert_eq!(backoff_for(&conflict, &opts), Some(opts.conflict_backoff));
    assert_eq!(
        backoff_for(&unavailable, &opts),
        Some(opts.unavailable_backoff)
    );
    assert_eq!(backoff_for(&backend, &opts), None);
    assert_eq!(
        backoff_for(&anyhow::anyhow!("other"), &opts),
        Some(opts.error_backoff)
    );
}

#[tokio::test]
async fn scheduler_returns_typed_backend_failure_without_renaming_workspace_files() {
    let tmp = TempDir::new().unwrap();
    let workspace = tmp.path();
    let legacy_dir = workspace.join("memory_tree");
    fs::create_dir_all(&legacy_dir).unwrap();
    let legacy_db = legacy_dir.join("chunks.db");
    fs::write(&legacy_db, b"legacy source bytes").unwrap();

    let scope = Scope::new(&format!("test:queue-scheduler-{}", uuid::Uuid::new_v4())).unwrap();
    let base = memory_storage(&scope);
    let documents = Failing::new(Arc::clone(base.documents()));
    let faults = documents.faults.clone();
    let storage = Storage::new(
        Arc::new(documents),
        Arc::clone(base.streams()),
        Arc::clone(base.kv()),
        Arc::clone(base.blobs()),
        Arc::clone(base.queues()),
        Arc::clone(base.secrets()),
    );
    let _cfg = crate::memory::config::MemoryConfig::open_with_storage(workspace, storage)
        .await
        .unwrap();
    faults.fail_operation_nth("documents.query", 1, ErrorKind::Backend);
    let cfg = _cfg;

    let error = run_scheduler(
        &cfg,
        &SchedulerLoopConfig {
            tick: Duration::from_millis(1),
        },
        &Shutdown::new(),
    )
    .await
    .expect_err("generic host storage failures must reach the host");
    assert_eq!(
        error.downcast_ref::<StorageError>().map(StorageError::kind),
        Some(ErrorKind::Backend)
    );
    assert_eq!(fs::read(&legacy_db).unwrap(), b"legacy source bytes");
    assert_eq!(
        fs::read_dir(&legacy_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>(),
        vec![std::ffi::OsString::from("chunks.db")]
    );
}

#[tokio::test]
async fn run_bootstraps_and_returns_when_shutdown_is_pretriggered() {
    let (_tmp, cfg) = test_config();
    let delegates = RecordingDelegates::admitting();
    let shutdown = Shutdown::new();
    shutdown.trigger();

    run(
        &cfg,
        &delegates,
        &fast_worker_opts(),
        &SchedulerLoopConfig {
            tick: Duration::from_millis(1),
        },
        &shutdown,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn interruptible_sleep_handles_zero_and_pretriggered_shutdown() {
    let shutdown = Shutdown::new();
    interruptible_sleep(Duration::ZERO, &shutdown).await;
    shutdown.trigger();
    interruptible_sleep(Duration::from_secs(1), &shutdown).await;
}
