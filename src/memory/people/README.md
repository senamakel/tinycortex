# People

The optional `memory::people` module resolves iMessage handles, email addresses,
and display names to stable `PersonId` values. It records interactions and
computes a deterministic recency × frequency × reciprocity × depth score. The
macOS Address Book reader can seed contacts; its platform access remains
separate from persistence.

## Persistence

`PeopleStore::open(&MemoryConfig)` binds the store to the host's already
prepared, scope-bound `DocumentStore`. The host prepares the schemas returned
by `people::collections()` together with the other TinyCortex-owned schemas.
People records, canonical handle mappings, and interactions use the existing
memory owner-head compare-and-swap, so inserting a person and its initial
aliases is one visible commit. The store has no production engine default and
does not open workspace paths.

The store APIs are async and use the configured host document port directly.
The process-global `store::init(&MemoryConfig)` / `store::get()` accessor is
available for free-function handlers; `PeopleStore::open` is available for
context-scoped use. Rebinding `init` to a different workspace changes the
active global store, while repeated initialization for the same workspace is
idempotent.

## Domain behavior

- Handles are canonicalized before lookup or storage: email and iMessage
  values are trimmed and lowercased; display-name whitespace is collapsed.
- A canonical handle resolves to at most one person. `resolve_or_create` is
  safe when concurrent callers race for the same handle.
- Explicit `link` attaches another handle to a resolved person. The resolver
  does not merge identities based only on a shared display name.
- People are listed in display-name order and their handles in `(kind, value)`
  order. Interaction reads are newest-first; batch reads return rows only for
  requested people that have interactions.
- Stored timestamps retain the previous Unix-second precision.
- Address-book permission errors remain distinct from an empty contact list.

All test stores use the TSD testkit with a prepared `MemoryConfig`; no SQLite
connection or filesystem database is used by the people module.
