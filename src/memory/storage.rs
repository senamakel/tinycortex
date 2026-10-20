//! Host storage ports and the synchronous compatibility bridge.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc as std_mpsc;
use std::thread;

use tinystoragedrivers_types::StorageError;
use tokio::sync::mpsc;

type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

thread_local! {
    static ON_BRIDGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs host-supplied asynchronous storage calls for synchronous and async APIs.
///
/// The bridge owns a Tokio runtime thread, so a caller can use it from either
/// a synchronous host or a Tokio worker without nesting `block_on` on that
/// worker. Synchronous calls cannot run from a current-thread Tokio runtime:
/// blocking that thread could prevent caller-runtime work required by a host
/// driver from progressing. Async APIs should use [`Self::run_async`] so the
/// caller runtime remains schedulable.
#[derive(Clone, Debug)]
pub(crate) struct Blocking {
    jobs: mpsc::UnboundedSender<Job>,
}

impl Blocking {
    /// Start the runtime thread used by this bridge.
    pub(crate) fn new() -> tinystoragedrivers_types::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|error| {
                StorageError::backend("cannot build storage runtime").with_source(error)
            })?;
        let (jobs, mut receiver) = mpsc::unbounded_channel::<Job>();
        thread::Builder::new()
            .name("tinycortex-storage".into())
            .spawn(move || {
                ON_BRIDGE.with(|flag| flag.set(true));
                runtime.block_on(async move {
                    let mut running = tokio::task::JoinSet::new();
                    while let Some(job) = receiver.recv().await {
                        running.spawn(job);
                        while running.try_join_next().is_some() {}
                    }
                    while running.join_next().await.is_some() {}
                });
            })
            .map_err(|error| {
                StorageError::backend("cannot start storage runtime").with_source(error)
            })?;
        Ok(Self { jobs })
    }

    /// Execute one storage future and return its result to the caller.
    pub(crate) fn run<F, T>(&self, future: F) -> tinystoragedrivers_types::Result<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if ON_BRIDGE.with(std::cell::Cell::get) {
            return Err(StorageError::backend(
                "a storage future called back into its own blocking bridge",
            ));
        }
        if tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread
        }) {
            return Err(StorageError::backend(
                "synchronous storage calls cannot run on a current-thread Tokio runtime",
            ));
        }
        let (reply, wait) = std_mpsc::sync_channel(1);
        let job: Job = Box::pin(async move {
            let _ = reply.send(future.await);
        });
        self.jobs
            .send(job)
            .map_err(|_| StorageError::backend("the storage runtime has stopped"))?;
        let receive = || {
            wait.recv()
                .map_err(|_| StorageError::backend("the storage call stopped unexpectedly"))
        };
        if on_multi_thread_worker() {
            tokio::task::block_in_place(receive)
        } else {
            receive()
        }
    }

    /// Execute a storage future without blocking the caller's async runtime.
    pub(crate) async fn run_async<F, T>(&self, future: F) -> tinystoragedrivers_types::Result<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if ON_BRIDGE.with(std::cell::Cell::get) {
            return Err(StorageError::backend(
                "a storage future called back into its own blocking bridge",
            ));
        }
        let (reply, wait) = tokio::sync::oneshot::channel();
        let job: Job = Box::pin(async move {
            let _ = reply.send(future.await);
        });
        self.jobs
            .send(job)
            .map_err(|_| StorageError::backend("the storage runtime has stopped"))?;
        wait.await
            .map_err(|_| StorageError::backend("the storage call stopped unexpectedly"))
    }
}

fn on_multi_thread_worker() -> bool {
    tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
