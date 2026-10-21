import "server-only";

import { cache } from "react";
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";
import { parseOwnerSnapshot, type OwnerSnapshot } from "@/lib/owner-snapshot";

const MAX_STDOUT_BYTES = 16 * 1024 * 1024;
const INSPECT_TIMEOUT_MS = 30_000;

export function readOwnerSnapshot(workspace: string, chunkLimit = 300): Promise<OwnerSnapshot> {
  if (!Number.isFinite(chunkLimit) || chunkLimit < 1) throw new Error("invalid owner snapshot limit");
  const boundedLimit = Math.max(1, Math.min(10_000, Math.floor(chunkLimit)));
  return snapshotForRequest(workspace, boundedLimit);
}

const snapshotForRequest = cache(async (workspace: string, chunkLimit: number): Promise<OwnerSnapshot> => {
  const binary = process.env.TINYCORTEX_INSPECTOR_BIN?.trim();
  const storageUrl = process.env.TINYCORTEX_STORAGE_URL?.trim();
  const scope = process.env.TINYCORTEX_STORAGE_SCOPE?.trim();
  if (!binary || !isAbsolute(binary)) {
    throw new Error("TINYCORTEX_INSPECTOR_BIN must name an absolute inspector executable path");
  }
  if (!storageUrl || !scope) {
    throw new Error("TINYCORTEX_STORAGE_URL and TINYCORTEX_STORAGE_SCOPE are required");
  }

  const env: NodeJS.ProcessEnv = {
    PATH: process.env.PATH,
    NODE_ENV: process.env.NODE_ENV,
  };
  for (const name of ["HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"]) {
    if (process.env[name]) env[name] = process.env[name];
  }
  env.TINYCORTEX_WORKSPACE = workspace;
  env.TINYCORTEX_STORAGE_URL = storageUrl;
  env.TINYCORTEX_STORAGE_SCOPE = scope;
  env.TINYCORTEX_INSPECTOR_CHUNK_LIMIT = String(chunkLimit);

  const stdout: Buffer[] = [];
  let stdoutBytes = 0;
  const raw = await new Promise<string>((resolve, reject) => {
    const child = spawn(binary, [], { env, stdio: ["ignore", "pipe", "ignore"], shell: false });
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
    child.on("error", () => finishError(new Error("could not start the owner inspector")));
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) reject(new Error("owner inspector could not read the configured scope"));
      else resolve(Buffer.concat(stdout).toString("utf8"));
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
