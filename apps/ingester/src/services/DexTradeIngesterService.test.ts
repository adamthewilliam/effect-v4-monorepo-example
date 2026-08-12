import {
  AggregatorName,
  DbPersistError,
  DexTradeId,
  DexTradeRepository,
  SignerAddress,
  TokenAmountDecimal,
  UsdAmountDecimal,
  type DexTrade,
  type NewDexTrade,
} from "@effect-monorepo/db";
import { describe, expect, it } from "@effect-monorepo/testing/bun-effect";
import { Effect, Layer, Logger } from "effect";

import {
  KafkaOffset,
  KafkaPartition,
  KafkaSource,
  KafkaTopic,
  shouldCommitOffset,
} from "../contracts/IngesterErrors";
import { DexTradeIngesterService } from "./DexTradeIngesterService";

describe("DexTradeIngesterService", () => {
  it.effect("persists a valid Kafka message", () =>
    Effect.gen(function* () {
      const storedTrade = fixtureTrade();
      let persistedTrade: NewDexTrade | undefined;
      const result = yield* serviceEffect({
        upsert: (trade) => {
          persistedTrade = trade;
          return Effect.succeed(storedTrade);
        },
      });

      expect(result.uniqueId).toBe(storedTrade.uniqueId);
      expect(persistedTrade?.blockTimestamp.toISOString()).toBe("2025-07-18T20:20:24.265Z");
    }),
  );

  it.effect("rejects an empty Kafka message as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(serviceEffectFromRawMessage(undefined));

      expect(error._tag).toBe("EmptyKafkaMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects invalid trade IDs as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            unique_id: "not-a-uuid",
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects blank signer values as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            signer: "   ",
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects untrimmed aggregator values as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            aggregator: " orca",
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects decimal string amounts as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            token_sold_amount: "72.43187983",
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects zero token quantities as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            token_bought_amount: 0,
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects negative fees as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            tx_fee_usd: -0.01,
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects non-finite JSON number literals as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffectFromRawMessage(
          JSON.stringify(fixturePayload()).replace("72.43187983", "1e309"),
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("rejects invalid block timestamps as non-retryable", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect(
          {},
          {
            ...fixturePayload(),
            block_timestamp: "not-a-date",
          },
        ),
      );

      expect(error._tag).toBe("InvalidDexTradeMessage");
      expect(shouldCommitOffset(error)).toBe(true);
    }),
  );

  it.effect("maps repository persistence failures without committing the offset", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        serviceEffect({
          upsert: (trade) =>
            Effect.fail(
              new DbPersistError({
                message: "Failed to persist DEX trade",
                table: "dex_trades",
                uniqueId: trade.uniqueId,
                cause: new Error("database offline"),
              }),
            ),
        }),
      );

      expect(error._tag).toBe("IngesterPersistFailed");
      expect(shouldCommitOffset(error)).toBe(false);
      expect(error.source.offset).toBe(KafkaOffset.make("42"));
    }),
  );
});

function serviceEffect(
  overrides: Partial<DexTradeRepository["Service"]> = {},
  payload: unknown = fixturePayload(),
) {
  return serviceEffectFromRawMessage(JSON.stringify(payload), overrides);
}

function serviceEffectFromRawMessage(
  rawMessage: string | undefined,
  overrides: Partial<DexTradeRepository["Service"]> = {},
) {
  return DexTradeIngesterService.use((service) =>
    service.ingestMessage(rawMessage, fixtureSource()),
  ).pipe(Effect.provide(testLayer(overrides)));
}

function testLayer(overrides: Partial<DexTradeRepository["Service"]> = {}) {
  const base: DexTradeRepository["Service"] = {
    upsert: () => Effect.succeed(fixtureTrade()),
    leaderboard: () => Effect.succeed([]),
    readiness: () => Effect.void,
  };

  return Layer.mergeAll(
    DexTradeIngesterService.layer.pipe(
      Layer.provide(
        Layer.succeed(DexTradeRepository, DexTradeRepository.of({ ...base, ...overrides })),
      ),
    ),
    Logger.layer([]),
  );
}

function fixtureSource() {
  return new KafkaSource({
    topic: KafkaTopic.make("dex.trades"),
    partition: KafkaPartition.make(0),
    offset: KafkaOffset.make("42"),
  });
}

function fixturePayload() {
  return {
    unique_id: "4d0dac4c-a2c4-41a1-86d1-b58f0bbc11e0",
    block_timestamp: "2025-07-18T20:20:24.265Z",
    signer: "AMtrZihSfHJHk6bCruZTsDtaB8nAHNYCcptFDyKZCWLU",
    token_sold_amount: 72.43187983,
    usd_sold_amount: 1246.98,
    token_bought_amount: 71.3058433,
    usd_bought_amount: 1207.83,
    aggregator: "orca",
    tx_fee_usd: 3.24,
  };
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
    pnlUsd: "0",
    createdAt: now,
    updatedAt: now,
  };
}
