//! Test-only prepared host storage for people behavior tests.

use crate::memory::config::MemoryConfig;
use crate::memory::people::store::PeopleStore;

pub(crate) fn config() -> (tempfile::TempDir, MemoryConfig) {
    let workspace = tempfile::tempdir().expect("people test workspace");
    let config = crate::memory::chunks::test_support::host_config(workspace.path());
    (workspace, config)
}

pub(crate) fn store() -> PeopleStore {
    let (_workspace, config) = config();
    PeopleStore::open(&config).expect("prepared test people store")
}
