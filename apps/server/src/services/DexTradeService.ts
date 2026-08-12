import {
  DbQueryError,
  DbReadinessError,
  DexTradeRepository,
  LeaderboardRow,
} from "@effect-monorepo/db";
import { Context, Effect, Layer } from "effect";
import { DatabaseQueryFailedError, DatabaseUnavailableError } from "./DexTradeErrors";

export class DexTradeService extends Context.Service<
  DexTradeService,
  {
    readonly readiness: Effect.Effect<void, DatabaseUnavailableError>;
    readonly leaderboard: Effect.Effect<
      LeaderboardRow[],
      DatabaseUnavailableError | DatabaseQueryFailedError
    >;
  }
>()("@effect-monorepo/server/DexTradeService") {
  static readonly layer: Layer.Layer<DexTradeService, never, DexTradeRepository> = Layer.effect(
    DexTradeService,
    Effect.gen(function* () {
      const trades = yield* DexTradeRepository;

      const readiness = trades.readiness.pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Database readiness check failed", {
            operation: error.operation,
            kind: error.kind,
            cause: error.cause,
          }),
        ),
        Effect.mapError(toDatabaseUnavailable),
      );

      const leaderboard = trades.leaderboard.pipe(
        Effect.tapError((error) =>
          Effect.logError("Database leaderboard query failed", {
            operation: error.operation,
            kind: error.kind,
            cause: error.cause,
          }),
        ),
        Effect.mapError(toDatabaseQueryFailure),
      );

      return DexTradeService.of({ readiness, leaderboard });
    }),
  );
}

const toDatabaseUnavailable = (error: DbReadinessError): DatabaseUnavailableError =>
  new DatabaseUnavailableError({
    message: "Database unavailable",
    operation: error.operation,
    cause: error.cause,
  });

const toDatabaseQueryFailure = (
  error: DbQueryError,
): DatabaseUnavailableError | DatabaseQueryFailedError =>
  error.kind === "transient"
    ? new DatabaseUnavailableError({
        message: "Database unavailable",
        operation: error.operation,
        cause: error.cause,
      })
    : new DatabaseQueryFailedError({
        message: "Database query failed",
        operation: error.operation,
        cause: error.cause,
      });
