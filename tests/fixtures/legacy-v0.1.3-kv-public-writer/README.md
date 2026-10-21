# TinyCortex v0.1.3 public KV writer fixture

This SQLite database was generated from TinyCortex source tag
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53`. The tag is source-tag provenance;
TinyCortex declares `publish = false`, so it is not a crates.io release. The
only injected source file was `examples/write_legacy_kv_fixture.rs`, which
uses the tagged public `KvStore::open`, `set_global`, and `set_namespace` APIs.
The tag's tracked `Cargo.toml` and `Cargo.lock` were not changed. No database
rows were hand-authored.

The fixture contains three global and two namespace records. It covers a JSON
null, structured JSON values, a shared key in global and namespace scope, and
legacy namespace normalization from `team alpha/#1` to `team_alpha/_1`. The
integration test at `tests/legacy_kv_import.rs` reads the database read-only,
imports typed rows into a testkit-backed owner store, and checks values,
identity, timestamps, idempotent retry, and source-file immutability.

The checked database SHA-256 is
`7336ae5baa02fb011dc3ab531e5aad7abeaf5c037e6bf087c378c0d9d1dd73ec`.
The tagged manifest and lockfile SHA-256 values are respectively
`92e907723696c7a742d811ca1b8625aa66c2738681a021f64be1e0d30614f188` and
`c9d1c09bf99e10cea1b115e78b822a8a5273778ae1aaf0ca04cff04c92ab5a59`.
Values are fixed by the generator; legacy `updated_at` fields use the writer's
clock and can change when the fixture is regenerated.

## Regeneration

From the TinyCortex repository root, create fresh persistent paths under
`.epic-repositories/` (do not put Cargo output in `/tmp`):

```sh
repo_root=$(git rev-parse --show-toplevel)
source_git="$repo_root/.epic-repositories/legacy-v013-kv-git-rerun"
archive="$repo_root/.epic-repositories/legacy-v013-kv-source-rerun"
fixture_out="$repo_root/.epic-repositories/legacy-v013-kv-output-rerun"
mkdir -p "$source_git" "$archive" "$fixture_out"
git clone --no-checkout https://github.com/senamakel/tinycortex.git "$source_git"
git -C "$source_git" fetch --depth=1 origin 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53
git -C "$source_git" archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp "$repo_root/tests/fixtures/legacy-v0.1.3-kv-public-writer/write_legacy_kv_fixture.rs" "$archive/examples/"
mkdir -p "$archive/target/compiler-scratch"
cd "$archive"
TMPDIR="$archive/target/compiler-scratch" CARGO_BUILD_JOBS=1 RUSTC_WRAPPER='' \
  cargo run --locked --manifest-path "$archive/Cargo.toml" \
  --example write_legacy_kv_fixture -- "$fixture_out"
```

Use new empty `archive` and `fixture_out` directories for each run. The
recorded successful run used the same source and generator, with output at
`.epic-repositories/legacy-v013-kv-output-02`; its Cargo command exited 0.
