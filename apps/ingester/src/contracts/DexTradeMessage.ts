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

const TokenAmountInput = Schema.Finite.check(Schema.isGreaterThan(0)).pipe(
  Schema.brand("@effect-monorepo/TokenAmountInput"),
);
type TokenAmountInput = typeof TokenAmountInput.Type;

const UsdAmountInput = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("@effect-monorepo/UsdAmountInput"),
);
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
    tokenSoldAmount: tokenAmountDecimalFromInput(message.token_sold_amount),
    usdSoldAmount: usdAmountDecimalFromInput(message.usd_sold_amount),
    tokenBoughtAmount: tokenAmountDecimalFromInput(message.token_bought_amount),
    usdBoughtAmount: usdAmountDecimalFromInput(message.usd_bought_amount),
    aggregator: message.aggregator,
    txFeeUsd: usdAmountDecimalFromInput(message.tx_fee_usd),
  };
});

const tokenAmountDecimalFromInput = (amount: TokenAmountInput): TokenAmountDecimal =>
  TokenAmountDecimal.make(String(amount));

const usdAmountDecimalFromInput = (amount: UsdAmountInput): UsdAmountDecimal =>
  UsdAmountDecimal.make(String(amount));
