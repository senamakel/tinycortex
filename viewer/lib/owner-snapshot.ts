export type OwnerSnapshot = {
  skill_documents: Array<{
    namespace: string | null;
    key: string;
    value: unknown;
    updated_at: number;
  }>;
  chunks: Array<{
    id: string;
    content: string;
    token_count: number;
    metadata: {
      source_kind: string;
      source_id: string;
      owner: string;
      tags: string[];
      timestamp: number;
    };
  }>;
  chunk_total: number;
  trees: Array<{
    id: string;
    kind: string;
    scope: string;
    root_id: string | null;
  }>;
  summaries: Array<{
    id: string;
    tree_id: string;
    level: number;
    parent_id: string | null;
    child_ids: string[];
    content: string;
  }>;
};

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("inspector returned a malformed snapshot");
  }
  return value as Record<string, unknown>;
}

function array(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`inspector snapshot has invalid ${name}`);
  return value;
}

function string(value: unknown, name: string): string {
  if (typeof value !== "string") throw new Error(`inspector snapshot has invalid ${name}`);
  return value;
}

function nullableString(value: unknown, name: string): string | null {
  if (value === null) return null;
  return string(value, name);
}

function finiteNumber(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`inspector snapshot has invalid ${name}`);
  }
  return value;
}

function strings(value: unknown, name: string): string[] {
  return array(value, name).map((item) => string(item, name));
}

/** Validate owner-serialized JSON before passing it into viewer projections. */
export function parseOwnerSnapshot(value: unknown): OwnerSnapshot {
  const root = object(value);
  const skill_documents = array(root.skill_documents, "skill_documents").map((value) => {
    const row = object(value);
    return {
      namespace: nullableString(row.namespace, "skill document namespace"),
      key: string(row.key, "skill document key"),
      value: row.value,
      updated_at: finiteNumber(row.updated_at, "skill document timestamp"),
    };
  });
  const chunks = array(root.chunks, "chunks").map((value) => {
    const row = object(value);
    const metadata = object(row.metadata);
    return {
      id: string(row.id, "chunk id"),
      content: string(row.content, "chunk content"),
      token_count: finiteNumber(row.token_count, "chunk token count"),
      metadata: {
        source_kind: string(metadata.source_kind, "chunk source kind"),
        source_id: string(metadata.source_id, "chunk source id"),
        owner: string(metadata.owner, "chunk owner"),
        tags: strings(metadata.tags, "chunk tags"),
        timestamp: finiteNumber(metadata.timestamp, "chunk timestamp"),
      },
    };
  });
  const trees = array(root.trees, "trees").map((value) => {
    const row = object(value);
    return {
      id: string(row.id, "tree id"),
      kind: string(row.kind, "tree kind"),
      scope: string(row.scope, "tree scope"),
      root_id: nullableString(row.root_id, "tree root id"),
    };
  });
  const summaries = array(root.summaries, "summaries").map((value) => {
    const row = object(value);
    return {
      id: string(row.id, "summary id"),
      tree_id: string(row.tree_id, "summary tree id"),
      level: finiteNumber(row.level, "summary level"),
      parent_id: nullableString(row.parent_id, "summary parent id"),
      child_ids: strings(row.child_ids, "summary children"),
      content: string(row.content, "summary content"),
    };
  });
  return {
    skill_documents,
    chunks,
    chunk_total: finiteNumber(root.chunk_total, "chunk total"),
    trees,
    summaries,
  };
}
