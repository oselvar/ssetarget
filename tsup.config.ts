import { defineConfig } from "tsup";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/workflows/index.ts",
    "src/workflows/cloudflare/index.ts",
    "src/workflows/cloudflare/sse/index.ts",
  ],
  splitting: false,
  sourcemap: true,
  clean: true,
  format: "esm",
  external: ["cloudflare:workers", "cloudflare:workflows"],
  dts: true,
});
