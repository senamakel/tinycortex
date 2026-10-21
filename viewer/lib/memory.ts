// Server-only projection of TinyCortex owner storage plus host-owned artifacts.

import "server-only";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { readOwnerSnapshot } from "@/lib/inspector";

// ── Workspace resolution ────────────────────────────────────────────────────

export function workspacePath(): string | null {
  const raw = process.env.TINYCORTEX_WORKSPACE?.trim();
  return raw && raw.length > 0 ? raw : null;
}

export type StorePaths = {
  workspace: string;
  manifest: string;
  entitiesDir: string;
};

export function storePaths(workspace: string): StorePaths {
  return {
    workspace,
    manifest: join(workspace, "sync_manifest.json"),
    entitiesDir: join(workspace, "memory_tree", "content", "entities"),
  };
}

// ── Skill documents (what sync ingested) ────────────────────────────────────

export type SkillDoc = {
  toolkit: string;
  documentId: string;
  namespaceSkillId: string;
  connectionId: string;
  title: string;
  content: string;
  metadata: Record<string, unknown>;
  updatedAt: number | null;
};

const SKILLDOC_NS_PREFIX = "skilldoc"; // ':' is sanitized to '_' by the KV store

function rowToDoc(namespace: string | null, value: Record<string, unknown>, updatedAt: number): SkillDoc {
  const toolkit =
    (typeof value.toolkit === "string" && value.toolkit) ||
    (namespace?.startsWith(`${SKILLDOC_NS_PREFIX}:`)
      ? namespace.slice(SKILLDOC_NS_PREFIX.length + 1)
      : "") ||
    "unknown";
  return {
    toolkit,
    documentId: String(value.document_id ?? ""),
    namespaceSkillId: String(value.namespace_skill_id ?? toolkit),
    connectionId: String(value.connection_id ?? ""),
    title: String(value.title ?? "(untitled)"),
    content: String(value.content ?? ""),
    metadata:
      value.metadata && typeof value.metadata === "object"
        ? (value.metadata as Record<string, unknown>)
        : {},
    updatedAt,
  };
}

export async function listSkillDocs(workspace: string): Promise<SkillDoc[]> {
  const snapshot = await readOwnerSnapshot(workspace);
  return snapshot.skill_documents
    .filter((row) => row.namespace?.startsWith(SKILLDOC_NS_PREFIX))
    .map((row) => rowToDoc(row.namespace, row.value, row.updated_at))
    .sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
}

export async function getSkillDoc(workspace: string, toolkit: string, documentId: string): Promise<SkillDoc | null> {
  return (
    (await listSkillDocs(workspace)).find(
      (d) => d.toolkit === toolkit && d.documentId === documentId,
    ) ?? null
  );
}

export type ToolkitCount = { toolkit: string; count: number };

export function skillDocCountsByToolkit(docs: SkillDoc[]): ToolkitCount[] {
  const counts = new Map<string, number>();
  for (const d of docs) counts.set(d.toolkit, (counts.get(d.toolkit) ?? 0) + 1);
  return [...counts.entries()]
    .map(([toolkit, count]) => ({ toolkit, count }))
    .sort((a, b) => b.count - a.count || a.toolkit.localeCompare(b.toolkit));
}

// ── Sync run manifest ───────────────────────────────────────────────────────

export type ToolkitResult = {
  toolkit: string;
  connectionId?: string;
  ingested?: number;
  actions?: number;
  costUsd?: number;
  docsStored?: number;
  taintOk?: boolean;
  cursorAdvanced?: boolean;
  idempotency?: string;
  passed?: boolean;
  error?: string | null;
};

export type SyncEvent = {
  toolkit?: string;
  sourceId?: string;
  source_id?: string;
  connectionId?: string;
  connection_id?: string;
  stage?: string;
  message?: string;
};

export type Manifest = {
  toolkits: ToolkitResult[];
  events: SyncEvent[];
  documentsPersisted?: number;
};

export function readManifest(workspace: string): Manifest | null {
  const path = storePaths(workspace).manifest;
  if (!existsSync(path)) return null;
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    return {
      toolkits: Array.isArray(parsed.toolkits) ? parsed.toolkits : [],
      events: Array.isArray(parsed.events) ? parsed.events : [],
      documentsPersisted:
        typeof parsed.documentsPersisted === "number" ? parsed.documentsPersisted : undefined,
    };
  } catch {
    return null;
  }
}

// ── Memory tree (chunks) — present only after a full ingest ──────────────────

export type Chunk = {
  id: string;
  sourceKind: string | null;
  sourceId: string | null;
  pathScope: string | null;
  owner: string | null;
  tags: string[];
  preview: string;
  tokenCount: number | null;
  timestampMs: number | null;
};

export type ChunkView = { available: boolean; chunks: Chunk[]; total: number };

export async function listChunks(workspace: string, limit = 200): Promise<ChunkView> {
  const snapshot = await readOwnerSnapshot(workspace, limit);
  const chunks: Chunk[] = snapshot.chunks.map((row) => ({
    id: row.id,
    sourceKind: row.metadata.source_kind || null,
    sourceId: row.metadata.source_id || null,
    pathScope: null,
    owner: row.metadata.owner || null,
    tags: row.metadata.tags,
    preview: row.content.slice(0, 240),
    tokenCount: row.token_count,
    timestampMs: row.metadata.timestamp,
  }));
  return { available: true, chunks, total: snapshot.chunk_total };
}

// ── Entities — markdown files under memory_tree/content/entities ─────────────

export type Entity = { kind: string; file: string; frontMatter: Record<string, string>; body: string };

export function listEntities(workspace: string): Entity[] {
  const dir = storePaths(workspace).entitiesDir;
  if (!existsSync(dir)) return [];
  const out: Entity[] = [];
  let kinds: string[];
  try {
    kinds = readdirSync(dir, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name);
  } catch {
    return [];
  }
  for (const kind of kinds) {
    const kindDir = join(dir, kind);
    let files: string[];
    try {
      files = readdirSync(kindDir).filter((f) => f.endsWith(".md"));
    } catch {
      continue;
    }
    for (const file of files) {
      try {
        const { frontMatter, body } = parseFrontMatter(readFileSync(join(kindDir, file), "utf8"));
        out.push({ kind, file, frontMatter, body });
      } catch {
        // skip unreadable entity file
      }
    }
  }
  return out;
}

function parseFrontMatter(raw: string): { frontMatter: Record<string, string>; body: string } {
  const match = raw.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
  if (!match) return { frontMatter: {}, body: raw };
  const frontMatter: Record<string, string> = {};
  for (const line of match[1].split("\n")) {
    const idx = line.indexOf(":");
    if (idx > 0) frontMatter[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
  }
  return { frontMatter, body: match[2] };
}

// ── Memory graph (summary tree hierarchy) ───────────────────────────────────
//
// Nodes: a synthetic root hub (when >1 tree), one `source` node per tree, every
// `summary` node, and the leaf `chunk`s referenced by the lowest summaries.
// Edges are the parent→child hierarchy derived from each summary's child ids
// (which point at child summaries or, at the lowest level, at chunks) plus a
// tree→root-summary link. This mirrors OpenHuman's `tree` graph mode.

export type GraphNodeKind = "root" | "source" | "summary" | "chunk";

export type GraphNode = {
  id: string;
  kind: GraphNodeKind;
  label: string;
  level?: number;
  treeKind?: string;
  childCount?: number;
  detail?: string;
};

export type GraphEdge = { from: string; to: string };

export type MemoryGraphData = {
  available: boolean;
  nodes: GraphNode[];
  edges: GraphEdge[];
  treeCount: number;
  summaryCount: number;
  chunkCount: number;
};

function firstLine(text: string, max = 48): string {
  const line = (text ?? "").split("\n").find((l) => l.trim().length > 0) ?? "";
  const trimmed = line.trim();
  return trimmed.length > max ? `${trimmed.slice(0, max - 1)}…` : trimmed || "(empty)";
}

export async function memoryGraph(workspace: string): Promise<MemoryGraphData> {
  const empty: MemoryGraphData = {
    available: false,
    nodes: [],
    edges: [],
    treeCount: 0,
    summaryCount: 0,
    chunkCount: 0,
  };
  const snapshot = await readOwnerSnapshot(workspace, 10_000);
  const trees = snapshot.trees;
  const summaries = snapshot.summaries;
  if (trees.length === 0 && summaries.length === 0) return empty;

    const summaryIds = new Set(summaries.map((s) => s.id));
    const nodes: GraphNode[] = [];
    const edges: GraphEdge[] = [];
    const multi = trees.length > 1;

    const treeIds = new Set(trees.map((t) => t.id));
    if (multi) nodes.push({ id: "__root__", kind: "root", label: "memory" });
    for (const t of trees) {
      nodes.push({ id: t.id, kind: "source", label: t.scope, treeKind: t.kind });
      if (multi) edges.push({ from: "__root__", to: t.id });
    }

    const childChunkIds = new Set<string>();
    for (const s of summaries) {
      const children = s.child_ids;
      nodes.push({
        id: s.id,
        kind: "summary",
        level: s.level,
        label: firstLine(s.content),
        childCount: children.length,
        detail: s.content,
      });
      // Attach every top-level summary (no parent) to its source node, so a tree
      // that sealed more than one top summary stays one connected component.
      if (!s.parent_id && treeIds.has(s.tree_id)) {
        edges.push({ from: s.tree_id, to: s.id });
      }
      for (const c of children) {
        edges.push({ from: s.id, to: c });
        if (!summaryIds.has(c)) childChunkIds.add(c);
      }
    }

    const found = new Map(snapshot.chunks.map((chunk) => [chunk.id, chunk.content]));
    for (const id of childChunkIds) {
      const preview = found.get(id);
      nodes.push({
        id,
        kind: "chunk",
        label: preview ? firstLine(preview, 40) : id.slice(0, 12),
        detail: preview ?? undefined,
      });
    }

    return {
      available: true,
      nodes,
      edges,
      treeCount: trees.length,
      summaryCount: summaries.length,
      chunkCount: childChunkIds.size,
    };
}

// ── Overview aggregate ──────────────────────────────────────────────────────

export type Overview = {
  workspace: string;
  exists: { skillDocs: boolean; chunks: boolean; manifest: boolean; entities: boolean };
  docCount: number;
  toolkitCounts: ToolkitCount[];
  chunkTotal: number;
  entityCount: number;
  manifest: Manifest | null;
};

export async function overview(workspace: string): Promise<Overview> {
  const paths = storePaths(workspace);
  const [docs, chunks] = await Promise.all([listSkillDocs(workspace), listChunks(workspace, 1)]);
  const entities = listEntities(workspace);
  return {
    workspace,
    exists: {
      skillDocs: docs.length > 0,
      chunks: chunks.total > 0,
      manifest: existsSync(paths.manifest),
      entities: existsSync(paths.entitiesDir),
    },
    docCount: docs.length,
    toolkitCounts: skillDocCountsByToolkit(docs),
    chunkTotal: chunks.total,
    entityCount: entities.length,
    manifest: readManifest(workspace),
  };
}
