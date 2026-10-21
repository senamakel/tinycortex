# TinyCortex v0.1.3 vector public-writer fixture

This database was generated with the public `VectorStore::open` and
`VectorStore::insert_with_vector` APIs from the TinyCortex `v0.1.3` source tag,
peeled commit `72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53`. The crate declares
`publish = false`, so this is provenance from the tagged source tree, not a
crates.io release. The archive's original `Cargo.toml` and `Cargo.lock` are
unchanged (Git blob IDs `36b776e3400dc619d79941314c8b76caceb993fb` and
`0cf2edab1b1b4ea83f807ac74e1821ccfd21e71c`). The only injected source file is
the public-API generator, whose SHA-256 is recorded below. No database rows
were hand-authored.

The generated SQLite database contains three vectors across two namespaces
and the historical `embed_provider` / `embed_dims` metadata. `notes` has two
rows; `archive` has one. The alpha vector includes negative zero to exercise
byte-preserving import. The current integration test reads the database
read-only, imports the typed rows into a host-prepared document store, and
checks namespace search, metadata, original packed bytes, timestamps, and the
dimension reopen guard. The source provider name is provenance only: v0.1.3
compared dimensions on reopen, not provider names or embedding signatures.

SHA-256:

| File | SHA-256 |
| --- | --- |
| `vectors.db` | `640795e03fefd9000a3dbf2fb5b0a0a4ea4c990ac92d7c73ff16a91176acead2` |
| `write_vector_fixture.rs` | `GENERATED_AFTER_COPY` |

## Regeneration

Run from the TinyCortex checkout containing this fixture. Keep the source
archive and output in persistent `.epic-repositories/` paths; use a new
`legacy-v013-vector-output-*` directory for each generation because the writer
refuses to replace an existing database.

```sh
repo_root=$PWD
git fetch upstream tag v0.1.3
archive="$repo_root/.epic-repositories/legacy-tinycortex-v013-vector-source"
output="$repo_root/.epic-repositories/legacy-v013-vector-output-next"
mkdir -p "$archive" "$output"
git archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-vector-public-writer/write_vector_fixture.rs \
  "$archive/examples/"
mkdir -p "$archive/target/compiler-scratch"
cd "$archive"
TMPDIR="$archive/target/compiler-scratch" cargo run --locked \
  --example write_vector_fixture -- "$output/vectors.db"
```

The generator is isolated to the archive and output; it does not alter the
tagged production files, manifest, or lockfile. Its database is authentic
source data for host import tests, not proof that an import succeeded by
itself.
