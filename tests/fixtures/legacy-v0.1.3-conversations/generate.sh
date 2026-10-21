#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
legacy_revision="ccb4e76"
legacy_source="$repo_root/../legacy-conversations-v013-writer"
fixture="$repo_root/tests/fixtures/legacy-v0.1.3-conversations"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run_root="$repo_root/../legacy-conversations-v013-runs/$run_id"
workspace="$run_root/workspace"
expected="$run_root/expected.json"

mkdir -p "$legacy_source" "$run_root" "$workspace" \
  "$legacy_source/target/conversations-tmp"
git -C "$repo_root" archive "$legacy_revision" | tar -x -C "$legacy_source"
cp "$fixture/writer.rs" "$legacy_source/examples/legacy_conversations_fixture.rs"

cd "$legacy_source"
flock /home/enamakel/work/openhuman/.git/implementing-an-epic/8131-cargo.lock \
  env TMPDIR="$legacy_source/target/conversations-tmp" \
  CARGO_BUILD_JOBS=1 RUSTC_WRAPPER='' \
  "$repo_root/../../scripts/ci-cancel-aware.sh" cargo run --locked \
    --example legacy_conversations_fixture -- "$workspace" "$expected"

mkdir -p "$fixture/data"
cp -a "$workspace/memory/conversations/." "$fixture/data/"
cp "$expected" "$fixture/expected.json"
(
  cd "$fixture"
  find data expected.json -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)
printf 'Wrote fixture using TinyCortex %s in %s\n' "$legacy_revision" "$run_root"
