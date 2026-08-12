import { Effect, Layer } from "effect";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiGroup,
  OpenApi,
} from "effect/unstable/httpapi";
import {
  HealthResponse,
  LeaderboardResponse,
  ReadinessResponse,
  RootResponse,
} from "../contracts/DexTradeResponses";
import {
  InternalServerErrorResponse,
  InvalidRequestErrorResponse,
  ReadinessErrorResponses,
  toInternalServerErrorResponse,
  toReadinessErrorResponse,
} from "../contracts/ServerErrors";
import { DexTradeService } from "../services/DexTradeService";
import { InvalidRequestMiddleware, InvalidRequestMiddlewareLive } from "./InvalidRequestMiddleware";
import type { LeaderboardRow } from "@effect-monorepo/db/repositories/DexTradeRepository";

export const SystemGroup = HttpApiGroup.make("system", { topLevel: true })
  .add(
    HttpApiEndpoint.get("root", "/", {
      success: RootResponse,
    })
      .annotate(OpenApi.Summary, "Get API status")
      .annotate(OpenApi.Description, "Returns the API identity and current status."),
    HttpApiEndpoint.get("healthz", "/healthz", {
      success: HealthResponse,
    })
      .annotate(OpenApi.Summary, "Check process liveness")
      .annotate(OpenApi.Description, "Returns ok when the HTTP service is alive."),
    HttpApiEndpoint.get("readyz", "/readyz", {
      success: ReadinessResponse,
      error: ReadinessErrorResponses,
    })
      .annotate(OpenApi.Summary, "Check dependency readiness")
      .annotate(OpenApi.Description, "Checks whether the API can reach required dependencies."),
  )
  .annotate(OpenApi.Description, "System endpoints for service identity and health checks.");

export const DexTradesGroup = HttpApiGroup.make("dex-trades", { topLevel: true })
  .add(
    HttpApiEndpoint.get("leaderboard", "/leaderboard", {
      success: LeaderboardResponse,
      error: InternalServerErrorResponse,
    }),
  )
  .annotate(OpenApi.Description, "DEX endpoints for operations on DEX trades.");

export const DexTradesApi = HttpApi.make("dex-trades")
  .add(SystemGroup)
  .add(DexTradesGroup)
  .middleware(InvalidRequestMiddleware)
  .annotate(OpenApi.Title, "DEX Trades API")
  .annotate(OpenApi.Version, "0.1.0")
  .annotate(OpenApi.Description, "HTTP API for the DEX trade ingestion service.")
  .annotate(OpenApi.Servers, [
    {
      url: "http://localhost:3000",
      description: "Local development",
    },
  ])
  .annotate(HttpApi.AdditionalSchemas, [InternalServerErrorResponse, InvalidRequestErrorResponse]);

const SystemRoutesBaseLive = HttpApiBuilder.group(
  DexTradesApi,
  "system",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* DexTradeService;

    return handlers
      .handle("root", () => Effect.succeed({ service: "dex-trades", status: "ok" } as const))
      .handle("healthz", () => Effect.succeed({ status: "ok" } as const))
      .handle("readyz", () =>
        service
          .readiness()
          .pipe(Effect.as({ status: "ready" } as const), Effect.mapError(toReadinessErrorResponse)),
      );
  }),
);

export const SystemRoutesLive = SystemRoutesBaseLive.pipe(
  Layer.provide(InvalidRequestMiddlewareLive),
);

const mapToLeaderboardResponse = (leaderboard: LeaderboardRow[]) => {
  return LeaderboardResponse.make({
    leaderboard: leaderboard.map((row) => ({
      rank: row.rank,
      signer: row.signer,
      totalPnlUsd: row.totalPnlUsd,
      tradeCount: row.tradeCount,
    })),
  });
};

const DexTradesRoutesBaseLive = HttpApiBuilder.group(
  DexTradesApi,
  "dex-trades",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* DexTradeService;

    return handlers.handle("leaderboard", () =>
      service
        .leaderboard()
        .pipe(Effect.map(mapToLeaderboardResponse), Effect.mapError(toInternalServerErrorResponse)),
    );
  }),
);

export const DexTradesRoutesLive = DexTradesRoutesBaseLive.pipe(
  Layer.provide(InvalidRequestMiddlewareLive),
);

export const ApiRoutesLive = Layer.provide(
  HttpApiBuilder.layer(DexTradesApi, { openapiPath: "/openapi.json" }),
  Layer.mergeAll(SystemRoutesLive, DexTradesRoutesLive),
);
