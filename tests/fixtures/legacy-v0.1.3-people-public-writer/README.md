# TinyCortex v0.1.3 public-writer people fixture

This database is produced by the unchanged TinyCortex source commit
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53` (tag `v0.1.3`) using only its
public `PeopleStore` APIs. The checked-in example is injected into an archive
of that exact source; no production source, manifest, lockfile, or SQL rows
are hand-edited. The tag is `publish = false`; this is authentic source-tag
output, not a crates.io release. The archived manifest and lockfile match
their source Git blobs exactly (`Cargo.toml` blob
`36b776e3400dc619d79941314c8b76caceb993fb`, `Cargo.lock` blob
`0cf2edab1b1b4ea83f807ac74e1821ccfd21e71c`). It writes two people, five
canonical aliases, and three interactions through `insert_person`,
`resolve_or_insert_person`, `add_alias`, and `record_interaction`.

The SQLite file is input for the host-side reader in
`tests/legacy_people_import.rs`; production people storage has no SQLite
reader. The reader maps rows into the typed `LegacyPeopleBatch`, preserving
person timestamps, alias creation times, interaction values, and interaction
`rowid`s as stable imported identities. Tests import that DTO to a real
`tinystoragedrivers-testkit` backend and read it through the public people
store.

Generated database SHA-256: `13d4e9c6bdecafe379795f70695dbae82fa87db9f3aa8cd4d5a122942cc4f594`.
The exact successful command used `cargo run --locked --features people`
against the archived source and exited 0 (`target/logs/people-legacy-writer-03.log`).

The generator source and original DB are provenance-controlled fixture data.
Regenerate into the persistent per-run workspace described below, then compare
the resulting hashes before replacing either checked-in file. Person UUIDs and
alias timestamps use the old writer's randomness/clock, so regeneration can
produce different bytes.

## Regeneration

From a TinyCortex checkout containing this fixture, extract the exact tag to a
fresh archive, inject only the public writer example, and run it with an empty
output directory:

```sh
repo_root=$PWD
run_id=$(date -u +%Y%m%dT%H%M%SZ)
run_root="$repo_root/.epic-repositories/legacy-v013-people-runs/$run_id"
git_dir="$run_root/git"
archive="$run_root/source"
fixture_out="$run_root/output"
mkdir -p "$run_root" "$archive" "$fixture_out" "$archive/target/compiler-scratch"
git clone --no-checkout https://github.com/senamakel/tinycortex.git "$git_dir"
git -C "$git_dir" fetch --depth=1 origin 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53
git -C "$git_dir" archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-people-public-writer/write_legacy_people_fixture.rs \
  "$archive/examples/"
cd "$archive"
TMPDIR="$archive/target/compiler-scratch" CFLAGS=-pipe cargo run --locked \
  --features people --manifest-path "$archive/Cargo.toml" \
  --example write_legacy_people_fixture \
  -- "$fixture_out"
```
