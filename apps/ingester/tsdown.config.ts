import { defineConfig } from "tsdown";

export default defineConfig({
  entry: "./src/ingester.ts",
  format: "esm",
  outDir: "./dist",
  clean: true,
  deps: {
    alwaysBundle: [/@effect-monorepo\/.*/],
    neverBundle: ["bun"],
  },
});
