# TinyCortex v0.1.3 public-writer state-family fixture

This supplemental database was generated from TinyCortex source tag
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53`. The one injected example uses the
tag's public chat-ingest/queue APIs, `TreeFactory`,
`tree::append_leaf_deferred`, `tree::ingest_summary`, public hotness upsert,
public chunk embedding write, and public chunk/summary re-embed skip APIs. It
does not execute SQL writes or change the source schema. The tag archive has
613 tracked source files; its manifests and lockfile were left unchanged.

This artifact is separate from the existing extended writer fixture. It
contains two pending buffers, one complete hotness row, one chunk skip, and
one summary skip. `tests/legacy_extended_import.rs` reads this SQLite database
read-only, maps those rows into typed import records, and verifies owner-store
readback and re-embed coverage behavior.

Generated artifact hashes:

- `memory_tree/chunks.db`: `4b67734fe554da7d6d21aaa2efff147637cf6f885e05ed335f7c96644b3a2c5c`
- pending chunk content: `c71113cbbc521d8b3d7f91b570ead6461ccc89a4d70e3ccc0e3d7fbfe6ab5613`
- summary child content: `39ccdddd07895936114d50fd625efaabb5fc7e006e92b409901d3450a60ccf2b`
- summary content file: `bff2f5de499c2fedcf7a397b433ae352d9a8467162f2923fdc77cee662cec1d9`

The database contains 2 buffers, 1 hotness row, and 1 row in each re-embed
skip table. The fixture also has 2 chunks, 2 trees, 1 summary, 1 chunk
embedding, 1 entity-index occurrence, and 2 queue jobs created by the public
chat-ingest calls.

## Regeneration

From this repository's root, extract the exact source tag, copy the checked-in
example into it, and run the example with an empty output directory:

```sh
repo_root=$(git rev-parse --show-toplevel)
source_tag=72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53
git_dir=$(mktemp -d)
archive=$(mktemp -d)
fixture_out=$(mktemp -d)
git clone --no-checkout https://github.com/senamakel/tinycortex.git "$git_dir"
git -C "$git_dir" fetch --depth=1 origin "$source_tag"
git -C "$git_dir" archive "$source_tag" | tar -x -C "$archive"
cp "$repo_root/tests/fixtures/legacy-v0.1.3-state-public-writer/write_legacy_state_fixture.rs" "$archive/examples/"
cd "$archive"
cargo run --locked --manifest-path "$archive/Cargo.toml" --example write_legacy_state_fixture -- "$fixture_out"
```

The successful recorded run used the same pinned source and example with
`CFLAGS=-pipe`, an archive-local `TMPDIR`, and output directory
`.epic-repositories/legacy-v013-state-output-02`; its command exited 0. The
first attempt failed during C compilation when the compiler wrote a temporary
assembler file under `/tmp` and hit the environment's per-user quota; it
produced no fixture. The hashes above are from the successful regeneration.
