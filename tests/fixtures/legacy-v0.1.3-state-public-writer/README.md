# TinyCortex v0.1.3 public-writer state-family fixture

This supplemental database is generated from TinyCortex source tag
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53`. The injected example uses the
tag's public chat-ingest/queue APIs, `TreeFactory`,
`tree::append_leaf_deferred`, `tree::ingest_summary`, public hotness upsert,
public chunk embedding write, and public chunk/summary re-embed skip APIs. It
does not execute SQL writes or modify the source schema. The archived tag has
613 tracked source files; original manifests and lockfile are unchanged. The
only injected file is `examples/write_legacy_state_fixture.rs`.

The artifact is intentionally separate from the existing extended writer
fixture. It covers two pending buffers, one complete hotness row, one chunk
skip, and one summary skip with original timestamps. The destination import
test reads this SQLite database read-only, maps those rows into typed import
records, and verifies owner-store readback and re-embed coverage behavior.

## Regeneration

Extract the exact source tag to a fresh directory, copy the checked-in example
into its `examples/` directory, and run it with a fresh empty output workspace:

```sh
repo_root=$PWD
archive="$repo_root/.epic-repositories/legacy-v013-state-source"
fixture_out="$repo_root/.epic-repositories/legacy-v013-state-output"
git clone --no-checkout https://github.com/senamakel/tinycortex.git "$repo_root/.epic-repositories/legacy-v013-state-git"
git -C "$repo_root/.epic-repositories/legacy-v013-state-git" fetch --depth=1 origin 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53
mkdir -p "$archive" "$fixture_out"
git -C "$repo_root/.epic-repositories/legacy-v013-state-git" archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-state-public-writer/write_legacy_state_fixture.rs "$archive/examples/"
cd "$archive"
cargo run --locked --manifest-path "$archive/Cargo.toml" --example write_legacy_state_fixture -- "$fixture_out"
```

The generated database and content-file hashes, together with the actual
generator command and exit receipt, are recorded below after regeneration.
