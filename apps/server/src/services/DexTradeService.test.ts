import {
  DbQueryError,
  DbReadinessError,
  AggregatorName,
  DexTradeId,
  DexTradeRepository,
  PnlUsdDecimal,
  SignerAddress,
  TokenAmountDecimal,
  UsdAmountDecimal,
  type DexTrade,
} from "@effect-monorepo/db";
import { describe, expect, it } from "@effect-monorepo/testing/bun-effect";
import { Effect, Layer, Logger } from "effect";
import { DexTradeService } from "./DexTradeService";

describe("DexTradeService", () => {
  it.effect("reports readiness when the repository is reachable", () =>
    DexTradeService.use((service) => service.readiness).pipe(Effect.provide(testLayer())),
  );

  it.effect("maps repository readiness failures to DatabaseUnavailable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        DexTradeService.use((service) => service.readiness).pipe(
          Effect.provide(
            testLayer({
              readiness: Effect.fail(
                new DbReadinessError({
                  message: "Database readiness check failed",
                  operation: "readiness",
                  cause: new Error("database offline"),
                  kind: "transient",
                }),
              ),
            }),
          ),
        ),
      );

      if (error._tag !== "DatabaseUnavailable") {
        throw new Error(`Expected DatabaseUnavailable, got ${error._tag}`);
      }

      expect(error.message).toBe("Database unavailable");
      expect(error.operation).toBe("readiness");
    }),
  );

  it.effect("maps retryable leaderboard failures to DatabaseUnavailable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        DexTradeService.use((service) => service.leaderboard).pipe(
          Effect.provide(
            testLayer({
              leaderboard: Effect.fail(
                new DbQueryError({
                  message: "Database query failed",
                  operation: "leaderboard",
                  cause: new Error("database offline"),
                  kind: "transient",
                }),
              ),
            }),
          ),
        ),
      );

      expect(error._tag).toBe("DatabaseUnavailable");
      if (error._tag !== "DatabaseUnavailable") {
        throw new Error(`Expected DatabaseUnavailable, got ${error._tag}`);
      }
      expect(error.operation).toBe("leaderboard");
      expect(error.cause).toBeInstanceOf(Error);
    }),
  );

  it.effect("keeps permanent leaderboard failures as query failures", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        DexTradeService.use((service) => service.leaderboard).pipe(
          Effect.provide(
            testLayer({
              leaderboard: Effect.fail(
                new DbQueryError({
                  message: "Database query failed",
                  operation: "leaderboard",
                  cause: new Error("invalid query"),
                  kind: "fatal",
                }),
              ),
            }),
          ),
        ),
      );

      expect(error._tag).toBe("DatabaseQueryFailed");
      if (error._tag !== "DatabaseQueryFailed") {
        throw new Error(`Expected DatabaseQueryFailed, got ${error._tag}`);
      }
      expect(error.operation).toBe("leaderboard");
      expect(error.cause).toBeInstanceOf(Error);
    }),
  );
});

function testLayer(overrides: Partial<DexTradeRepository["Service"]> = {}) {
  const base: DexTradeRepository["Service"] = {
    upsert: () => Effect.succeed(fixtureTrade()),
    leaderboard: Effect.succeed([]),
    readiness: Effect.void,
  };

  return Layer.mergeAll(
    DexTradeService.layer.pipe(
      Layer.provide(
        Layer.succeed(DexTradeRepository, DexTradeRepository.of({ ...base, ...overrides })),
      ),
    ),
    Logger.layer([]),
  );
}

function fixtureTrade(): DexTrade {
  const now = new Date("2025-07-18T20:20:30.000Z");

  return {
    uniqueId: DexTradeId.make("4d0dac4c-a2c4-41a1-86d1-b58f0bbc11e0"),
    blockTimestamp: new Date("2025-07-18T20:20:24.265Z"),
    signer: SignerAddress.make("AMtrZihSfHJHk6bCruZTsDtaB8nAHNYCcptFDyKZCWLU"),
    tokenSoldAmount: TokenAmountDecimal.make("72.43187983"),
    usdSoldAmount: UsdAmountDecimal.make("1246.98"),
    tokenBoughtAmount: TokenAmountDecimal.make("71.3058433"),
    usdBoughtAmount: UsdAmountDecimal.make("1207.83"),
    aggregator: AggregatorName.make("orca"),
    txFeeUsd: UsdAmountDecimal.make("3.24"),
    pnlUsd: PnlUsdDecimal.make("0"),
    createdAt: now,
    updatedAt: now,
  };
}
