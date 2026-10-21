# TinyCortex v0.1.3 public-writer fixture

This fixture was generated through the public `tinycortex::memory::ingest::ingest_chat`
API and `QueueJobSink` from the TinyCortex `v0.1.3` source tag at
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53` (2026-10-01). The repository
declares `publish = false`, so this is a tagged source version, not a crates.io
release. Its original `Cargo.toml` and `Cargo.lock` were preserved unchanged
(Git blob IDs `36b776e3400dc619d79941314c8b76caceb993fb` and
`0cf2edab1b1b4ea83f807ac74e1821ccfd21e71c`). The archived source had 613
tracked files; only the generator example was injected to run the public
writer. No database rows were hand-authored.

The generated database contains two chunks, two score rows, and two pending
extract jobs for `slack:#migration-fixture`. It contains no trees, summaries,
entity index rows, or entity edges. The two content files are under
`memory_tree/content/chat/slack-migration-fixture/`. Message timestamps are
fixed in the generator; legacy `created_at_ms` values come from the writer's
clock, so regeneration may produce a different database hash while preserving
the same application data.

SHA-256 for the checked-in artifacts:

| File | SHA-256 |
| --- | --- |
| `memory_tree/chunks.db` | `230e8ae019249964c6609e256295cbd9ba2a452ff3d90df015519a2e24b3bb00` |
| `memory_tree/content/chat/slack-migration-fixture/7d19953bab38532ca8a0251f799a18ee.md` | `06479c86e53d2187dffea9d75040bc6f7c20ce7dce8862505355af0a3faafa57` |
| `memory_tree/content/chat/slack-migration-fixture/c541a6f00915b23f9e051c202c0c0147.md` | `b70fe92b921712e506c79f1d3729b96540a201536d964c087bc6d7faf4dccbb0` |

## Regeneration

Run from the TinyCortex checkout containing this fixture. This archives the
tagged source, copies in the generator, and writes to a fresh output directory:

```sh
repo_root=$PWD
archive="$repo_root/.epic-repositories/legacy-tinycortex-v013-source"
mkdir -p "$archive"
git archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-public-writer/write_legacy_storage_fixture.rs \
  "$archive/examples/"
mkdir -p "$archive/target/compiler-scratch"
fixture_out=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-output.XXXXXX")
cd "$archive"
TMPDIR="$archive/target/compiler-scratch" cargo run --locked \
  --example write_legacy_storage_fixture -- "$fixture_out"
```

The generator adds only an example to the archived source; it does not change
the tagged production code, manifest, or lockfile. The database and content
files are authentic source artifacts for host-side import tests. This fixture
does not prove destination import or readback; the host import owner must
perform that operation and validate the imported fields before claiming that
coverage.
