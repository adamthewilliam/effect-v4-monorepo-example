import {
  AggregatorName,
  DexTradeId,
  SignerAddress,
  TokenAmountDecimal,
  UsdAmountDecimal,
  type NewDexTrade,
} from "@effect-monorepo/db";
import { DateTime, Effect, Schema } from "effect";
import {
  InvalidDexTradeMessageError,
  InvalidKafkaJsonError,
  type KafkaSource,
  type ParseDexTradeMessageError,
} from "./IngesterErrors";

const TokenAmountInput = TokenAmountDecimal.pipe(
  Schema.check(
    Schema.isPattern(/^(?:[1-9]\d*(?:\.\d+)?|0+\.\d*[1-9]\d*)$/, {
      message: "Expected a positive decimal string",
    }),
  ),
);
type TokenAmountInput = typeof TokenAmountInput.Type;

const UsdAmountInput = UsdAmountDecimal;
type UsdAmountInput = typeof UsdAmountInput.Type;

export const dexTradeMessageSchema = Schema.Struct({
  unique_id: DexTradeId,
  block_timestamp: Schema.DateTimeUtcFromString,
  signer: SignerAddress,
  token_sold_amount: TokenAmountInput,
  usd_sold_amount: UsdAmountInput,
  token_bought_amount: TokenAmountInput,
  usd_bought_amount: UsdAmountInput,
  aggregator: AggregatorName,
  tx_fee_usd: UsdAmountInput,
});

export type DexTradeMessage = typeof dexTradeMessageSchema.Type;

export type { KafkaSource };

export const parseDexTradeMessage = Effect.fn("parseDexTradeMessage")(function* (
  rawMessage: string,
  source: KafkaSource,
): Effect.fn.Return<NewDexTrade, ParseDexTradeMessageError> {
  const parsedJson = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(
    rawMessage,
  ).pipe(
    Effect.catchTag(
      "SchemaError",
      (cause) =>
        new InvalidKafkaJsonError({
          message: "Kafka message is not valid JSON",
          source,
          cause,
        }),
    ),
  );

  const message = yield* Schema.decodeUnknownEffect(dexTradeMessageSchema)(parsedJson).pipe(
    Effect.catchTag(
      "SchemaError",
      (details) =>
        new InvalidDexTradeMessageError({
          message: "Kafka message does not match the DEX trade schema",
          source,
          details,
        }),
    ),
  );

  return {
    uniqueId: message.unique_id,
    blockTimestamp: DateTime.toDateUtc(message.block_timestamp),
    signer: message.signer,
    tokenSoldAmount: message.token_sold_amount,
    usdSoldAmount: message.usd_sold_amount,
    tokenBoughtAmount: message.token_bought_amount,
    usdBoughtAmount: message.usd_bought_amount,
    aggregator: message.aggregator,
    txFeeUsd: message.tx_fee_usd,
  };
});
