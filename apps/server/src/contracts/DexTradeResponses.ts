import { PnlUsdDecimal, SignerAddress } from "@effect-monorepo/db";
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
      rank: Schema.Int,
      signer: SignerAddress,
      totalPnlUsd: PnlUsdDecimal,
      tradeCount: Schema.Int,
    }),
  ),
}).annotate({
  identifier: "LeaderboardResponse",
  description: "Leaderboard response.",
});
