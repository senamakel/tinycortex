import "server-only";

import { cache } from "react";
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";

export type OwnerSnapshot = {
  skill_documents: Array<{
    namespace: string | null;
    key: string;
    value: Record<string, unknown>;
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

const MAX_STDOUT_BYTES = 16 * 1024 * 1024;
const MAX_STDERR_BYTES = 8 * 1024;
const INSPECT_TIMEOUT_MS = 30_000;

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
    const storedValue = object(row.value);
    return {
      namespace: nullableString(row.namespace, "skill document namespace"),
      key: string(row.key, "skill document key"),
      value: storedValue,
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

const snapshotForRequest = cache(async (workspace: string): Promise<OwnerSnapshot> => {
  const binary = process.env.TINYCORTEX_INSPECTOR_BIN?.trim();
  const storageUrl = process.env.TINYCORTEX_STORAGE_URL?.trim();
  const scope = process.env.TINYCORTEX_STORAGE_SCOPE?.trim();
  if (!binary || !isAbsolute(binary)) {
    throw new Error("TINYCORTEX_INSPECTOR_BIN must name an absolute inspector executable path");
  }
  if (!storageUrl || !scope) {
    throw new Error("TINYCORTEX_STORAGE_URL and TINYCORTEX_STORAGE_SCOPE are required");
  }

  const env: NodeJS.ProcessEnv = { PATH: process.env.PATH };
  for (const name of [
    "HOME",
    "TMPDIR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
  ]) {
    if (process.env[name]) env[name] = process.env[name];
  }
  env.TINYCORTEX_WORKSPACE = workspace;
  env.TINYCORTEX_STORAGE_URL = storageUrl;
  env.TINYCORTEX_STORAGE_SCOPE = scope;

  const stdout: Buffer[] = [];
  let stdoutBytes = 0;
  let stderr = "";
  const raw = await new Promise<string>((resolve, reject) => {
    const child = spawn(binary, [], { env, stdio: ["ignore", "pipe", "pipe"], shell: false });
    let settled = false;
    const finishError = (error: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    };
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finishError(new Error("owner inspection exceeded its time limit"));
    }, INSPECT_TIMEOUT_MS);
    child.stdout.on("data", (part: Buffer) => {
      stdoutBytes += part.length;
      if (stdoutBytes > MAX_STDOUT_BYTES) {
        child.kill("SIGKILL");
        finishError(new Error("owner inspection exceeded its output limit"));
        return;
      }
      stdout.push(part);
    });
    child.stderr.on("data", (part: Buffer) => {
      if (stderr.length < MAX_STDERR_BYTES) {
        stderr += part.toString("utf8").slice(0, MAX_STDERR_BYTES - stderr.length);
      }
    });
    child.on("error", () => finishError(new Error("could not start the owner inspector")));
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) {
        // Inspector stderr is intentionally not included: storage diagnostics
        // can contain connection details. The operator can inspect it locally.
        reject(new Error("owner inspector could not read the configured scope"));
      } else {
        resolve(Buffer.concat(stdout).toString("utf8"));
      }
    });
  });

  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch {
    throw new Error("owner inspector returned invalid JSON");
  }
  return parseOwnerSnapshot(decoded);
});

export function readOwnerSnapshot(workspace: string): Promise<OwnerSnapshot> {
  return snapshotForRequest(workspace);
}
