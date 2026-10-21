//! Cross-file integration tests for the people domain.

use std::sync::Arc;

use chrono::Utc;

#[cfg(not(target_os = "macos"))]
use crate::memory::people::address_book;
use crate::memory::people::resolver::HandleResolver;
use crate::memory::people::store::PeopleStore;
use crate::memory::people::types::{Handle, PersonId};

/// Serialises every test that mutates the process-global people store.
///
/// The slot is process-wide, so two tests rebinding it concurrently race and
/// either may observe the other's store through `get()`.
static GLOBAL_STORE_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[tokio::test]
async fn resolver_and_store_cooperate_across_handle_kinds() {
    let s = crate::memory::people::test_support::store();
    let r = HandleResolver::new(&s);

    // Email mints.
    let id = r
        .resolve_or_create(&Handle::Email("a@b.c".into()))
        .await
        .unwrap();
    // iMessage handle linked to same person.
    let id2 = r
        .link(
            &Handle::Email("a@b.c".into()),
            Handle::IMessage("+15551234".into()),
        )
        .await
        .unwrap();
    assert_eq!(id, id2);

    // Resolving by the linked iMessage handle returns the same id.
    let via_imsg = r
        .resolve(&Handle::IMessage("+15551234".into()))
        .await
        .unwrap();
    assert_eq!(via_imsg, Some(id));
}

#[cfg(not(target_os = "macos"))]
#[test]
fn address_book_is_empty_on_non_mac() {
    assert!(address_book::read().unwrap().is_empty());
}

/// Verify global binding accepts only prepared host configs, is an idempotent
/// no-op for the same workspace, and rebinds to a different active workspace.
///
/// Takes [`GLOBAL_STORE_LOCK`] because it mutates the process-global store
/// slot that any test may observe through `get()`. `#[test]` alone does **not**
/// serialise anything — the harness runs tests in parallel by default — so the
/// lock is what makes that true, and it must be taken by every future test that
/// touches the global.
#[test]
fn init_rebinds_global_store_using_prepared_host_configs() {
    use crate::memory::people::store;

    let _serial = GLOBAL_STORE_LOCK.lock();

    let (_ws_a, config_a) = crate::memory::people::test_support::config();
    let store_a = store::init(&config_a).unwrap();

    // Previously-dead global is now reachable — the 8NM fix.
    let via_global = store::get().expect("people store reachable after seed");
    assert!(Arc::ptr_eq(&store_a, &via_global));

    // Same workspace → idempotent no-op, returns the same instance.
    let again = store::init(&config_a).unwrap();
    assert!(Arc::ptr_eq(&store_a, &again));

    // Different workspace (active-user switch) → rebind to a new store. #4378.
    let (_ws_b, config_b) = crate::memory::people::test_support::config();
    let store_b = store::init(&config_b).unwrap();
    assert!(
        !Arc::ptr_eq(&store_a, &store_b),
        "a new workspace must rebind to a fresh store, not reuse the old one"
    );
    let after_switch = store::get().expect("people store reachable after rebind");
    assert!(
        Arc::ptr_eq(&store_b, &after_switch),
        "get() must return the rebound (workspace B) store after a switch"
    );

    // Keep the testkit config's workspace stable while the process-global
    // handle remains bound for tests running later in the process.
    let keep: &'static (tempfile::TempDir, crate::memory::config::MemoryConfig) =
        Box::leak(Box::new(crate::memory::people::test_support::config()));
    store::init(&keep.1).unwrap();
}

#[test]
fn person_id_uuid_format() {
    let id = PersonId::new();
    // Round-trips through a string.
    let s = id.to_string();
    let parsed: uuid::Uuid = s.parse().unwrap();
    assert_eq!(parsed, id.0);
    let _now = Utc::now();
}
