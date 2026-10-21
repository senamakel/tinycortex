import path from "node:path";
import { fileURLToPath } from "node:url";

const viewerRoot = path.dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const nextConfig = {
  outputFileTracingRoot: viewerRoot,
};

export default nextConfig;
