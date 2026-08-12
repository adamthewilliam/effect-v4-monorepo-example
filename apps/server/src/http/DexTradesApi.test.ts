import { describe, expect, it } from "@effect-monorepo/testing/bun-effect";
import { OpenApi } from "effect/unstable/httpapi";
import { DexTradesApi } from "./DexTradesApi";

describe("DexTradesApi contract", () => {
  it("declares the global internal error response for every endpoint", () => {
    const spec = OpenApi.fromApi(DexTradesApi);

    expect(responseStatuses(spec, "/")).toEqual(["200", "400", "500"]);
    expect(responseStatuses(spec, "/healthz")).toEqual(["200", "400", "500"]);
    expect(responseStatuses(spec, "/readyz")).toEqual(["200", "400", "500", "503"]);
    expect(responseStatuses(spec, "/leaderboard")).toEqual(["200", "400", "500", "503"]);
  });
});

function responseStatuses(spec: ReturnType<typeof OpenApi.fromApi>, path: string): string[] {
  const responses = spec.paths[path]?.get?.responses;
  return Object.keys(responses ?? {}).sort();
}
