import {
  DbPersistError,
  DexTradeRepository,
  PnlUsdDecimal,
  type DexTrade,
  type NewDexTrade,
} from "@effect-monorepo/db";
import { BigDecimal, Context, Effect, Layer } from "effect";
import {
  EmptyKafkaMessageError,
  actionForDbFailure,
  IngesterPersistError,
  type IngesterError,
  type KafkaSource,
} from "../contracts/IngesterErrors";
import { parseDexTradeMessage } from "../contracts/DexTradeMessage";

export class DexTradeIngesterService extends Context.Service<
  DexTradeIngesterService,
  {
    readonly ingestMessage: (
      rawMessage: string | undefined,
      source: KafkaSource,
    ) => Effect.Effect<DexTrade, IngesterError>;
  }
>()("@effect-monorepo/ingester/DexTradeIngesterService") {
  static readonly layer: Layer.Layer<DexTradeIngesterService, never, DexTradeRepository> =
    Layer.effect(
      DexTradeIngesterService,
      Effect.gen(function* () {
        const trades = yield* DexTradeRepository;

        const ingestMessage = Effect.fn("DexTradeIngesterService.ingestMessage")(function* (
          rawMessage: string | undefined,
          source: KafkaSource,
        ) {
          if (!rawMessage) {
            return yield* new EmptyKafkaMessageError({
              message: "Kafka message value is empty",
              source,
            });
          }

          const parsedTrade = yield* parseDexTradeMessage(rawMessage, source);
          const trade: NewDexTrade = {
            ...parsedTrade,
            pnlUsd: calculatePnlUsd(parsedTrade),
          };

          const stored = yield* trades.upsert(trade).pipe(
            Effect.catchTag(
              "DbPersistError",
              (error: DbPersistError) =>
                new IngesterPersistError({
                  message: error.message,
                  source,
                  uniqueId: trade.uniqueId,
                  cause: error.cause,
                  action: actionForDbFailure(error.kind),
                }),
            ),
          );

          yield* Effect.logInfo("Stored DEX trade", {
            uniqueId: stored.uniqueId,
            aggregator: stored.aggregator,
            offset: source.offset,
          });

          return stored;
        });

        return DexTradeIngesterService.of({ ingestMessage });
      }),
    );
}

const calculatePnlUsd = (trade: NewDexTrade): PnlUsdDecimal => {
  const pnl = BigDecimal.subtract(
    BigDecimal.subtract(
      BigDecimal.fromStringUnsafe(trade.usdSoldAmount),
      BigDecimal.fromStringUnsafe(trade.usdBoughtAmount),
    ),
    BigDecimal.fromStringUnsafe(trade.txFeeUsd),
  );

  return PnlUsdDecimal.make(
    BigDecimal.format(BigDecimal.round(pnl, { scale: 2, mode: "half-from-zero" })),
  );
};
