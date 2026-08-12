import { count, sql } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import * as SqlError from "effect/unstable/sql/SqlError";
import { DbClient } from "../DbClient";
import {
  DbPersistError,
  DbQueryError,
  DbReadinessError,
  type DbFailureKind as DbFailureKindType,
} from "../DbErrors";
import {
  dexTrades,
  PnlUsdDecimal,
  SignerAddress,
  type DexTrade,
  type DexTradeId,
  type NewDexTrade,
} from "../schema";

const dexTradesTable = "dex_trades";

const mapReadinessError = (cause: unknown): DbReadinessError =>
  new DbReadinessError({
    message: "Database readiness check failed",
    operation: "readiness",
    cause,
    kind: classifyDatabaseError(cause),
  });

const mapQueryError =
  (operation: string) =>
  (cause: unknown): DbQueryError =>
    new DbQueryError({
      message: `Database query failed: ${operation}`,
      table: dexTradesTable,
      operation,
      cause,
      kind: classifyDatabaseError(cause),
    });

const mapPersistError =
  (uniqueId: DexTradeId) =>
  (cause: unknown): DbPersistError =>
    new DbPersistError({
      message: "Failed to persist DEX trade",
      operation: "upsert",
      table: dexTradesTable,
      uniqueId,
      cause,
      kind: classifyDatabaseError(cause),
    });

const classifyDatabaseError = (cause: unknown): DbFailureKindType => {
  if (!SqlError.isSqlError(cause)) {
    return "fatal";
  }

  switch (cause.reason._tag) {
    case "ConnectionError":
    case "DeadlockError":
    case "LockTimeoutError":
    case "SerializationError":
    case "StatementTimeoutError":
      return "transient";
    case "ConstraintError":
    case "UniqueViolation":
      return "constraint";
    case "AuthenticationError":
    case "AuthorizationError":
    case "SqlSyntaxError":
    case "UnknownError":
      return "fatal";
    default:
      return assertNever(cause.reason);
  }
};

export const LeaderboardRow = Schema.Struct({
  rank: Schema.Int,
  signer: SignerAddress,
  totalPnlUsd: PnlUsdDecimal,
  tradeCount: Schema.Int,
});
export type LeaderboardRow = typeof LeaderboardRow.Type;

export class DexTradeRepository extends Context.Service<
  DexTradeRepository,
  {
    readonly upsert: (trade: NewDexTrade) => Effect.Effect<DexTrade, DbPersistError>;
    readonly leaderboard: Effect.Effect<LeaderboardRow[], DbQueryError>;
    readonly readiness: Effect.Effect<void, DbReadinessError>;
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
            operation: "upsert",
            kind: "fatal",
          });
        }

        return row[0];
      });

      const leaderboard = Effect.gen(function* () {
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
          .pipe(Effect.mapError(mapQueryError("leaderboard")));

        return rows.map((row) =>
          LeaderboardRow.make({
            rank: Number(row.rank),
            signer: row.signer,
            totalPnlUsd: PnlUsdDecimal.make(String(row.totalPnlUsd)),
            tradeCount: Number(row.tradeCount),
          }),
        );
      });

      const readiness = database.execute(sql`select 1`).pipe(Effect.mapError(mapReadinessError));

      return DexTradeRepository.of({
        upsert,
        leaderboard,
        readiness,
      });
    }),
  );
}

function assertNever(value: never): never {
  throw new Error(`Unhandled SQL error reason: ${JSON.stringify(value)}`);
}
