import { DbPersistError, DexTradeRepository, type DexTrade } from "@effect-monorepo/db";
import { Context, Effect, Layer } from "effect";
import {
  EmptyKafkaMessageError,
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

          const trade = yield* parseDexTradeMessage(rawMessage, source);
          trade.pnlUsd = (
            Number(trade.usdSoldAmount) -
            Number(trade.usdBoughtAmount) -
            Number(trade.txFeeUsd)
          ).toFixed(2);

          const stored = yield* trades.upsert(trade).pipe(
            Effect.catchTag(
              "DbPersistError",
              (error: DbPersistError) =>
                new IngesterPersistError({
                  message: error.message,
                  source,
                  uniqueId: trade.uniqueId,
                  cause: error.cause,
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
