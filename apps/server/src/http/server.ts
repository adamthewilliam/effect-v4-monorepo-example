import { BunHttpServer } from "@effect/platform-bun";
import { DexTradeRepository, makeDbLayer } from "@effect-monorepo/db";
import { ServerEnv } from "@effect-monorepo/env/server";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { HttpApiScalar } from "effect/unstable/httpapi";
import { DexTradeService } from "../services/DexTradeService";
import { ApiRoutesLive, DexTradesApi } from "./DexTradesApi";
import { GlobalUnexpectedErrorMiddlewareLive } from "./GlobalUnexpectedErrorMiddleware";

const ServerDbLive = Layer.unwrap(
  ServerEnv.pipe(Effect.map((env) => makeDbLayer(env.DATABASE_URL))),
);

const DexTradeLive = DexTradeService.layer.pipe(
  Layer.provideMerge(DexTradeRepository.layer),
  Layer.provideMerge(ServerDbLive),
  Layer.provideMerge(ServerEnv.layer),
);

const CorsLive = Layer.unwrap(
  ServerEnv.pipe(
    Effect.map((env) =>
      HttpRouter.cors({
        allowedOrigins: [env.CORS_ORIGIN],
        allowedMethods: ["GET", "OPTIONS"],
      }),
    ),
  ),
);

const DocsLive = HttpApiScalar.layer(DexTradesApi, {
  path: "/docs",
  scalar: {
    hideTestRequestButton: false,
    showOperationId: true,
  },
});

const ApiLive = Layer.mergeAll(
  ApiRoutesLive,
  DocsLive,
  CorsLive,
  GlobalUnexpectedErrorMiddlewareLive,
);

const BunServerLive = Layer.unwrap(
  ServerEnv.pipe(Effect.map((env) => BunHttpServer.layer({ port: env.PORT }))),
);

export const HttpLive = HttpRouter.serve(ApiLive).pipe(
  Layer.provideMerge(BunServerLive),
  Layer.provideMerge(DexTradeLive),
);
