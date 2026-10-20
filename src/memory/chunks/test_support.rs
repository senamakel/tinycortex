//! Shared host-storage setup for synchronous chunk tests.

use std::path::Path;

use crate::memory::config::MemoryConfig;
use tinystoragedrivers_types::Scope;

pub(crate) fn host_config(workspace: impl AsRef<Path>) -> MemoryConfig {
    let workspace = workspace.as_ref().to_path_buf();
    let scope =
        Scope::new(&format!("test:tinycortex-{}", uuid::Uuid::new_v4())).expect("valid test scope");
    let storage = tinystoragedrivers_testkit::memory_storage(&scope);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        runtime.block_on(MemoryConfig::open_with_storage(workspace, storage))
    })
    .join()
    .expect("test storage setup thread")
    .expect("open test host storage")
}
