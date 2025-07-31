import { defineConfig } from "tsup";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/redis/index.ts",
    "src/workflows/index.ts",
    "src/workflows/cloudflare/index.ts",
    "src/workflows/cloudflare/sse/index.ts",
  ],
  splitting: true,
  sourcemap: true,
  clean: true,
  format: "esm",
  external: ["cloudflare:workers", "cloudflare:workflows", "ioredis"],
  dts: true,
});
