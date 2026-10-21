# Authentic TinyCortex v0.1.3 conversation writer fixture

This fixture was produced from TinyCortex release source commit
`ccb4e76adfd9076a2c095e5f85c721d4dc22ab7f` by calling its unchanged public
`ConversationStore::new`, `ensure_thread`, `append_message`, `list_threads`,
and `get_messages` APIs. `writer.rs` is copied into that source archive as an
example; the transcript JSONL files and `expected.json` are generated outputs.
No fixture row is hand-seeded or relabelled as historical data.

The fixture deliberately contains two messages with the same arbitrary source
ID in one thread. The v0.1.3 writer appends both in order, so the typed
destination import must preserve that behavior. It also covers two threads,
parent/personality fields, labels, message metadata, timestamps, and message
ordering. The integration test checks the old message files against the
public-API readback before importing those typed records into host-provided
storage.

`SHA256SUMS` records every fixture payload. To regenerate from a checkout with
the old release commit available, run from the TinyCortex repository root:

```bash
bash tests/fixtures/legacy-v0.1.3-conversations/generate.sh
```

The script archives the recorded source commit, builds the example with its
locked dependencies, writes into a unique persistent run directory outside
`target/`, copies the generated source files, and refreshes the checksums.
