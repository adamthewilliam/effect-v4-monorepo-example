import { count, sql } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import { DbClient } from "../DbClient";
import { DbPersistError, DbReadinessError } from "../DbErrors";
import {
  dexTrades,
  SignerAddress,
  type DexTrade,
  type DexTradeId,
  type NewDexTrade,
} from "../schema";

const dexTradesTable = "dex_trades";

const mapReadinessError = (cause: unknown): DbReadinessError =>
  new DbReadinessError({
    message: "Database readiness check failed",
    cause,
    retryable: true,
  });

const mapPersistError =
  (uniqueId: DexTradeId) =>
  (cause: unknown): DbPersistError =>
    new DbPersistError({
      message: "Failed to persist DEX trade",
      table: dexTradesTable,
      uniqueId,
      cause,
    });

export const LeaderboardRow = Schema.Struct({
  rank: Schema.Number,
  signer: SignerAddress,
  totalPnlUsd: Schema.Number,
  tradeCount: Schema.Number,
});
export type LeaderboardRow = typeof LeaderboardRow.Type;

export class DexTradeRepository extends Context.Service<
  DexTradeRepository,
  {
    readonly upsert: (trade: NewDexTrade) => Effect.Effect<DexTrade, DbPersistError>;
    readonly leaderboard: () => Effect.Effect<LeaderboardRow[], DbReadinessError>;
    readonly readiness: () => Effect.Effect<void, DbReadinessError>;
  }
>()("@effect-monorepo/db/DexTradeRepository") {
  static readonly layer: Layer.Layer<DexTradeRepository, never, DbClient> = Layer.effect(
    DexTradeRepository,
    Effect.gen(function* () {
      const database = yield* DbClient;

      const upsert = Effect.fn("DexTradeRepository.upsert")(function* (trade: NewDexTrade) {
        const row = yield* database
          .insert(dexTrades)
          .values(trade)
          .onConflictDoUpdate({
            target: dexTrades.uniqueId,
            set: {
              blockTimestamp: trade.blockTimestamp,
              signer: trade.signer,
              tokenSoldAmount: trade.tokenSoldAmount,
              usdSoldAmount: trade.usdSoldAmount,
              tokenBoughtAmount: trade.tokenBoughtAmount,
              usdBoughtAmount: trade.usdBoughtAmount,
              aggregator: trade.aggregator,
              txFeeUsd: trade.txFeeUsd,
              updatedAt: sql`now()`,
            },
          })
          .returning()
          .pipe(Effect.mapError(mapPersistError(trade.uniqueId)));

        if (row[0] === undefined) {
          return yield* new DbPersistError({
            message: "Failed to persist DEX trade",
            table: dexTradesTable,
            uniqueId: trade.uniqueId,
            cause: new Error("Insert returned no row"),
          });
        }

        return row[0];
      });

      const leaderboard = Effect.fn("DexTradeRepository.leaderboard")(function* () {
        const rows = yield* database
          .select({
            rank: sql<number>`row_number() over (order by sum(${dexTrades.pnlUsd}) desc)`,
            signer: dexTrades.signer,
            totalPnlUsd: sql<number>`sum(${dexTrades.pnlUsd})`,
            tradeCount: count(),
          })
          .from(dexTrades)
          .groupBy(dexTrades.signer)
          .orderBy(sql<number>`sum(${dexTrades.pnlUsd}) desc`)
          .limit(10)
          .pipe(Effect.mapError(mapReadinessError));

        return rows.map((row) =>
          LeaderboardRow.make({
            rank: Number(row.rank),
            signer: row.signer,
            totalPnlUsd: Number(row.totalPnlUsd),
            tradeCount: Number(row.tradeCount),
          }),
        );
      });

      const readiness = Effect.fn("DexTradeRepository.readiness")(function* () {
        yield* database.execute(sql`select 1`).pipe(Effect.mapError(mapReadinessError));
      });

      return DexTradeRepository.of({
        upsert,
        leaderboard,
        readiness,
      });
    }),
  );
}
