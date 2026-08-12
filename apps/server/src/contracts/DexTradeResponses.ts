import { SignerAddress } from "@effect-monorepo/db/schema/index";
import { Schema } from "effect";

export const RootResponse = Schema.Struct({
  service: Schema.Literal("dex-trades"),
  status: Schema.Literal("ok"),
}).annotate({
  identifier: "RootResponse",
  description: "Service identity response.",
});

export const HealthResponse = Schema.Struct({
  status: Schema.Literal("ok"),
}).annotate({
  identifier: "HealthResponse",
  description: "Process liveness response.",
});

export const ReadinessResponse = Schema.Struct({
  status: Schema.Literal("ready"),
}).annotate({
  identifier: "ReadinessResponse",
  description: "Dependency readiness response.",
});

export const LeaderboardResponse = Schema.Struct({
  leaderboard: Schema.Array(
    Schema.Struct({
      rank: Schema.Number,
      signer: SignerAddress,
      totalPnlUsd: Schema.Number,
      tradeCount: Schema.Number,
    }),
  ),
}).annotate({
  identifier: "LeaderboardResponse",
  description: "Leaderboard response.",
});
