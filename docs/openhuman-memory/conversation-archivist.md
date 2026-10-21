# Conversation and Archivist Spec

OpenHuman modules: `memory_conversations`, `memory_archivist`.

## Responsibility

Conversation storage persists thread and message events in the host-provided
`StreamStore`, with a rebuildable `DocumentStore` projection for thread lists.
Archivist converts selected conversation turns into cleaned tree leaves for
semantic memory.

## Conversation Store

The host prepares the declared conversation index collection and injects its
storage ports through `MemoryConfig`. Conversation streams are authoritative;
the document index is only a rebuildable projection.

A process-wide async lifecycle gate serializes transcript snapshots, mutations,
deletion, and purge across store handles. Delete and rewrite checkpoints retain
their monotonic stream offsets while truncating superseded message payloads.

## Conversation Types

Thread metadata should include:

- id, title, active/deleted state.
- created/updated timestamps.
- labels.
- optional parent thread id.
- optional personality id.
- message counts and last message timestamp for summaries.

Message records should include:

- id.
- content.
- type.
- sender.
- created timestamp.
- extra metadata.

Patch operations must update labels, titles, metadata, deletion/purge state,
and message records without corrupting append-only logs.

## Event Persistence

OpenHuman `memory_conversations::bus` subscribes to inbound channel events and
mirrors messages into the store. TinyCortex exposes the same persistence
subscriber as host-wired traits, so a host event bus can forward Slack,
Telegram, or UI-driven thread events without this crate depending on the
concrete bus implementation.

## Search Helpers

OpenHuman includes an inverted index and tokenizer. TinyCortex should preserve
language-agnostic normalization for thread/message search, including Unicode
normalization and diacritic-insensitive matching where practical.

## Archivist Flow

Archivist turns raw conversation history into a tree leaf:

```text
Vec<Turn>
  -> clean_conversation()
  -> compose_conversation_md()
  -> archive_to_tree()
  -> memory_tree append/seal pipeline
```

`Turn` includes role, content, optional tool-call JSON, and timestamp.

`clean_conversation` must:

- remove `tool_calls_json`.
- drop turns with role `tool`.
- preserve natural-language user/assistant content.

`compose_conversation_md` emits markdown sections like:

```markdown
## user
...

## assistant
...
```

`archive_to_tree` writes the cleaned blob as one leaf. In TinyCortex the chunk
id is `archivist:<sha256(session_id || NUL || markdown)[..32 hex chars]>`;
OpenHuman's configured id length is the compatibility constraint for any host
adapter that reuses the batch archivist.

## Rationale

Tool-call JSON and tool-result output are verbose, provider-specific, and often
irrelevant out of context. They should not distort embeddings or summaries of
human conversation. Clean natural-language transcript is the archival unit.

## Required Invariants

- Conversation streams are transcript persistence, not semantic memory by
  themselves.
- Ingestion into searchable memory is a separate archivist/tree operation.
- Transcript operations are serialized across handles, including purge and
  reads that could otherwise race prefix compaction.
- Tool turns and tool-call JSON are removed before archival.
- Archives should cite source session/thread ids.

## TinyCortex Landing Area

```text
src/memory/conversations/
src/memory/archivist/
```

Current TinyCortex ports the stream-backed conversation store, inverted-index search,
host-wired bus subscriber, archivist clean/compose helpers, per-turn episodic
capture, and tree writer integration. The source-registry conversation reader is
a separate legacy-thread reader over `<workspace>/threads/*.json`.
