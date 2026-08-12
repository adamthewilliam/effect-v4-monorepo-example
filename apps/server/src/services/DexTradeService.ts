import {
  DbQueryError,
  DbReadinessError,
  DexTradeRepository,
  LeaderboardRow,
} from "@effect-monorepo/db";
import { Context, Effect, Layer } from "effect";
import { DatabaseUnavailableError } from "./DexTradeErrors";

export class DexTradeService extends Context.Service<
  DexTradeService,
  {
    readonly readiness: () => Effect.Effect<void, DatabaseUnavailableError>;
    readonly leaderboard: () => Effect.Effect<LeaderboardRow[], DatabaseUnavailableError>;
  }
>()("@effect-monorepo/server/DexTradeService") {
  static readonly layer: Layer.Layer<DexTradeService, never, DexTradeRepository> = Layer.effect(
    DexTradeService,
    Effect.gen(function* () {
      const trades = yield* DexTradeRepository;

      const readiness = Effect.fn("DexTradeService.readiness")(function* () {
        return yield* trades.readiness().pipe(Effect.mapError(toDatabaseUnavailable));
      });

      const leaderboard = Effect.fn("DexTradeService.leaderboard")(function* () {
        return yield* trades.leaderboard().pipe(Effect.mapError(toDatabaseUnavailable));
      });

      return DexTradeService.of({ readiness, leaderboard });
    }),
  );
}

const toDatabaseUnavailable = (_error: DbQueryError | DbReadinessError): DatabaseUnavailableError =>
  new DatabaseUnavailableError({ message: "Database unavailable" });
