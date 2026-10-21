# TinyCortex v0.1.3 extended public-writer fixture

This separate fixture was generated from the TinyCortex `v0.1.3` source tag at
`72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53` using only public APIs from that
tag: chat ingestion and `QueueJobSink`, `TreeFactory`, `tree::ingest_summary`,
`EntityIndex::index_entity`, and `graph::upsert_edges`. The tag declares
`publish = false`; it is source-tag provenance, not a crates.io release. The
archived source had 613 tracked files. The only injected file was
`examples/write_legacy_extended_fixture.rs`; the original `Cargo.toml` and
`Cargo.lock` were unchanged (Git blob IDs `36b776e3400dc619d79941314c8b76caceb993fb`
and `0cf2edab1b1b4ea83f807ac74e1821ccfd21e71c`). No database rows were
hand-authored.

The database contains one chunk, one score, one Ready extract job, one source
tree, one L1 summary, two summary-node entity occurrences, and one graph edge.
The summary content file is included so import coverage can validate the
content pointer and bytes. The checked-in artifact supplies authentic source
data; it does not by itself prove that the destination importer preserves or
reads these records. Application event times are fixed, while legacy
`created_at_ms`, tree IDs, and summary IDs use the writer's clock/randomness, so
regeneration can change the database and content-file hashes.

SHA-256 for this generated output:

| File | SHA-256 |
| --- | --- |
| `memory_tree/chunks.db` | `865e4bfb81734254f6f5075917e4d7cd9ffc045649e6214b94e37f59b1c5fa0c` |
| `memory_tree/content/chat/slack-extended-migration-fixture/1bc5dcab126259aa4b5a9dd9b96ed3df.md` | `b9fdde82eaf39378f89bddbba29aef68e8d4af6983e207d1019944f2e88a3b27` |
| `memory_tree/content/wiki/summaries/source-slack-extended-migration-fixture/L1/summary-1792544917907-L1-4097db96.md` | `1e5d1463b00fd8ce60456357c4823020ce4657ba5c03419e7c8d0fdc29b87aa1` |

## Regeneration

From the TinyCortex checkout containing this fixture, archive the exact tag
and inject only the checked-in public writer example. Use a fresh output
directory for every run:

```sh
repo_root=$PWD
archive=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-extended-source.XXXXXX")
git archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-extended-public-writer/write_legacy_extended_fixture.rs \
  "$archive/examples/"
mkdir -p "$repo_root/target/logs" "$repo_root/target/compiler-scratch"
fixture_out=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-extended-output.XXXXXX")
cd "$archive"
flock /home/enamakel/work/openhuman/.git/implementing-an-epic/8131-cargo.lock \
  /home/enamakel/work/openhuman/worktrees/epic-8131-2-cortex-chunks/scripts/ci-cancel-aware.sh \
  env TMPDIR="$repo_root/target/compiler-scratch" RUSTC_WRAPPER='' CARGO_BUILD_JOBS=1 \
  cargo run --locked --manifest-path "$archive/Cargo.toml" \
    --example write_legacy_extended_fixture -- "$fixture_out"
```

The recorded generation completed with exit code `0` using the command above
with output `.epic-repositories/legacy-v013-extended-output`. Its log and exit
receipt are `target/logs/review31-legacy-v013-extended-generate-01.log` and
`target/logs/review31-legacy-v013-extended-generate-01.exit`.
