import test from "node:test";
import assert from "node:assert/strict";

import { parseOwnerSnapshot } from "../lib/owner-snapshot.ts";

const valid = {
  skill_documents: [{ namespace: "skilldoc:github", key: "doc-1", value: { title: "Guide" }, updated_at: 12 }],
  chunks: [{
    id: "chunk-1",
    content: "body",
    token_count: 1,
    metadata: { source_kind: "document", source_id: "source-1", owner: "", tags: [], timestamp: 1000 },
  }],
  graph_chunks: [],
  chunk_total: 1,
  trees: [{ id: "tree-1", kind: "source", scope: "source-1", root_id: null }],
  summaries: [{ id: "summary-1", tree_id: "tree-1", level: 1, parent_id: null, child_ids: ["chunk-1"], content: "summary" }],
};

test("owner snapshot validation preserves typed inspection projections", () => {
  assert.deepEqual(parseOwnerSnapshot(valid), valid);
});

test("owner snapshot accepts opaque KV values but rejects malformed persisted rows", () => {
  const snapshot = structuredClone(valid);
  snapshot.skill_documents[0].value = ["legacy opaque value"];
  assert.deepEqual(parseOwnerSnapshot(snapshot).skill_documents[0].value, ["legacy opaque value"]);

  const malformed = structuredClone(valid);
  malformed.chunks[0].metadata.tags = "not-an-array";
  assert.throws(() => parseOwnerSnapshot(malformed), /chunk tags/);
});
