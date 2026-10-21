# TinyCortex Memory Viewer

A local, read-only debug UI for inspecting a TinyCortex memory workspace. It is
a small Next.js app that asks TinyCortex owner APIs for a bounded read-only
snapshot from an explicitly configured storage scope. It reads the sync
manifest and entity Markdown files from the workspace; it does not open owner
databases directly.

## What it shows

- **Overview** — owner-scope document/chunk counts and host artifact counts.
- **Skill docs** — every `SkillDocument` sync ingested (secrets/PII scrubbed on
  write by the KV store), filterable by toolkit and searchable, with a detail
  view (content + metadata + raw provider payload).
- **Memory tree** — chunks and summary relationships returned by TinyCortex.
- **Entities** — canonical entities under `memory_tree/content/entities/`.
- **Sync runs** — the last run's per-toolkit results and event stream from
  `sync_manifest.json`.

## Run it

```sh
# 1. Produce a workspace to inspect. Either seed a demo (no API key needed):
TINYCORTEX_WORKSPACE=/tmp/tinycortex-demo \
  cargo run --example seed_memory --features sync

#    …or run a real Composio sync into a workspace:
TINYCORTEX_WORKSPACE=/tmp/tinycortex-demo \
  cargo run --example composio_harness --features sync

# 2. Build the standalone owner inspector.
cargo build --manifest-path viewer/inspector/Cargo.toml --locked

# 3. Point the viewer at the workspace and explicitly configured owner storage.
cd viewer
npm install
TINYCORTEX_WORKSPACE=/tmp/tinycortex-demo \
TINYCORTEX_STORAGE_URL=sqlite:///tmp/tinycortex-demo/owner.db \
TINYCORTEX_STORAGE_SCOPE=local \
TINYCORTEX_INSPECTOR_BIN="$(pwd)/inspector/target/debug/tinycortex-viewer-inspector" \
npm run dev
# → http://localhost:4319
```

The viewer reads the configured owner scope on each request, so re-running a
sync and refreshing the page shows the new state. The inspector receives its
storage URL and scope through environment variables; it never silently falls
back to a local database.

## Storage compatibility

Owner records are read through TinyCortex's inspection snapshot API and its
configured TinyStorage backend. The separate server-side inspector process
keeps backend configuration out of browser code and command-line arguments.
Entity Markdown and `sync_manifest.json` remain workspace artifacts and are
read by the Next.js server.

## Environment

- `TINYCORTEX_WORKSPACE` — path to the workspace to inspect (required).
- `TINYCORTEX_STORAGE_URL` — explicit TinyStorage backend URL (required).
- `TINYCORTEX_STORAGE_SCOPE` — explicit scope to inspect (required).
- `TINYCORTEX_INSPECTOR_BIN` — absolute path to the built inspector executable.

## Stack

Next.js (App Router, server components) and a standalone Rust owner inspector.
No client-side data fetching; all storage reads happen on the server.
