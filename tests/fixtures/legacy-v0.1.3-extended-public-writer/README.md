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

The summary row's semantic `content_sha256` is
`7acfc05fdf4bf82899e58d889bc6dcbd9420d79eaa133e1cf060ac0f81a1a029`; it
hashes the Markdown body after frontmatter, while the table records the hash
of the complete physical file.

## Regeneration

From the TinyCortex checkout containing this fixture, fetch the source commit
and extract it into a fresh directory. Inject only the checked-in public
writer example; its build artifacts stay under that archive's `target/`:

```sh
repo_root=$PWD
mkdir -p "$repo_root/.epic-repositories"
legacy_git=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-git.XXXXXX")
archive=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-extended-source.XXXXXX")
fixture_out=$(mktemp -d "$repo_root/.epic-repositories/legacy-v013-extended-output.XXXXXX")
git clone --no-checkout https://github.com/senamakel/tinycortex.git "$legacy_git"
git -C "$legacy_git" fetch --depth=1 origin 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53
git -C "$legacy_git" archive 72ce1d176aa3dc3a9ece14b9e61ecef9a8ae6b53 | tar -x -C "$archive"
cp tests/fixtures/legacy-v0.1.3-extended-public-writer/write_legacy_extended_fixture.rs \
  "$archive/examples/"
cd "$archive"
cargo run --locked --manifest-path "$archive/Cargo.toml" \
  --example write_legacy_extended_fixture -- "$fixture_out"
```

The original manifest and lockfile remain unchanged; the injected example is
the only source addition. This procedure writes the new database and content
files under `fixture_out`; compare their hashes with the table above because
the writer uses the clock and random IDs for some legacy fields.
