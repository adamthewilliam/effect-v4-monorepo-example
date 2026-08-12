import {
  AggregatorName,
  DbPersistError,
  DexTradeRepository,
  DexTradeId,
  SignerAddress,
  TokenAmountDecimal,
  UsdAmountDecimal,
  type DexTrade,
  type NewDexTrade,
} from "@effect-monorepo/db";
import { describe, expect, it } from "@effect-monorepo/testing/bun-effect";
import { Effect, Layer, Logger } from "effect";
import type { Consumer, EachBatchPayload, KafkaMessage } from "kafkajs";

import { KafkaOffset } from "../contracts/IngesterErrors";
import { DexTradeIngesterService } from "../services/DexTradeIngesterService";
import { nextOffset, processDexTradeBatch } from "./KafkaBatchProcessor";

describe("nextOffset", () => {
  it("increments a Kafka offset string", () => {
    expect(nextOffset(KafkaOffset.make("42"))).toBe(KafkaOffset.make("43"));
  });
});

describe("processDexTradeBatch", () => {
  it.effect("commits offset after a successful message", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      const committedOffsets: Array<{ topic: string; partition: number; offset: string }> = [];

      yield* batchEffect(
        {
          messages: [message("42", JSON.stringify(fixturePayload()))],
        },
        {
          resolveOffset: (offset) => resolvedOffsets.push(offset),
          consumer: {
            commitOffsets: async (
              offsets: Array<{ topic: string; partition: number; offset: string }>,
            ) => {
              committedOffsets.push(...offsets);
            },
          } as unknown as Consumer,
        },
      );

      expect(resolvedOffsets).toEqual(["42"]);
      expect(committedOffsets).toEqual([{ topic: "dex.trades", partition: 0, offset: "43" }]);
    }),
  );

  it.effect("fails a successful batch when committing processed offsets fails", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      let commitAttempts = 0;

      const error = yield* Effect.flip(
        batchEffect(
          {
            messages: [message("42", JSON.stringify(fixturePayload()))],
          },
          {
            resolveOffset: (offset) => resolvedOffsets.push(offset),
            consumer: {
              commitOffsets: async () => {
                commitAttempts += 1;
                throw new Error("Kafka commit failed");
              },
            } as unknown as Consumer,
          },
        ),
      );

      expect(error._tag).toBe("RetryableIngesterBatch");
      expect(error.message).toBe("Failed to commit Kafka offsets");
      if (!("source" in error)) {
        throw new Error("Expected a retryable ingester batch error");
      }
      expect(error.source.offset).toBe(KafkaOffset.make("42"));
      expect(resolvedOffsets).toEqual(["42"]);
      expect(commitAttempts).toBe(1);
    }),
  );

  it.effect("skips poison messages by resolving their offsets", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];

      yield* batchEffect(
        {
          messages: [message("42", undefined)],
        },
        {
          resolveOffset: (offset) => resolvedOffsets.push(offset),
          consumer: {
            commitOffsets: async () => undefined,
          } as unknown as Consumer,
        },
      );

      expect(resolvedOffsets).toEqual(["42"]);
    }),
  );

  it.effect("skips malformed JSON messages by committing their offsets", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      const committedOffsets: Array<{ topic: string; partition: number; offset: string }> = [];

      yield* batchEffect(
        {
          messages: [message("42", "{not-json")],
        },
        {
          resolveOffset: (offset) => resolvedOffsets.push(offset),
          consumer: {
            commitOffsets: async (
              offsets: Array<{ topic: string; partition: number; offset: string }>,
            ) => {
              committedOffsets.push(...offsets);
            },
          } as unknown as Consumer,
        },
      );

      expect(resolvedOffsets).toEqual(["42"]);
      expect(committedOffsets).toEqual([{ topic: "dex.trades", partition: 0, offset: "43" }]);
    }),
  );

  it.effect("skips invalid DEX trade messages by committing their offsets", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      const committedOffsets: Array<{ topic: string; partition: number; offset: string }> = [];

      yield* batchEffect(
        {
          messages: [
            message(
              "42",
              JSON.stringify({
                ...fixturePayload(),
                token_sold_amount: "72.43187983",
              }),
            ),
          ],
        },
        {
          resolveOffset: (offset) => resolvedOffsets.push(offset),
          consumer: {
            commitOffsets: async (
              offsets: Array<{ topic: string; partition: number; offset: string }>,
            ) => {
              committedOffsets.push(...offsets);
            },
          } as unknown as Consumer,
        },
      );

      expect(resolvedOffsets).toEqual(["42"]);
      expect(committedOffsets).toEqual([{ topic: "dex.trades", partition: 0, offset: "43" }]);
    }),
  );

  it.effect("fails the batch without resolving a persistence error", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      const committedOffsets: string[] = [];

      const error = yield* Effect.flip(
        batchEffect(
          {
            messages: [
              message("41", JSON.stringify(fixturePayload())),
              message("42", JSON.stringify(fixturePayload())),
            ],
          },
          {
            resolveOffset: (offset) => resolvedOffsets.push(offset),
            consumer: {
              commitOffsets: async (
                offsets: Array<{ topic: string; partition: number; offset: string }>,
              ) => {
                committedOffsets.push(offsets[0]?.offset ?? "");
              },
            } as unknown as Consumer,
          },
          {
            upsert: (() => {
              let calls = 0;
              return (trade: NewDexTrade) => {
                calls += 1;
                if (calls === 1) {
                  return Effect.succeed(fixtureTrade());
                }

                return Effect.fail(
                  new DbPersistError({
                    message: "Failed to persist DEX trade",
                    table: "dex_trades",
                    uniqueId: trade.uniqueId,
                    cause: new Error("database offline"),
                  }),
                );
              };
            })(),
          },
        ),
      );

      expect(error._tag).toBe("RetryableIngesterBatch");
      expect(resolvedOffsets).toEqual(["41"]);
      expect(committedOffsets).toEqual(["42"]);
    }),
  );

  it.effect("still reports the persistence error when the best-effort commit fails", () =>
    Effect.gen(function* () {
      const resolvedOffsets: string[] = [];
      let commitAttempts = 0;

      const error = yield* Effect.flip(
        batchEffect(
          {
            messages: [
              message("41", JSON.stringify(fixturePayload())),
              message("42", JSON.stringify(fixturePayload())),
            ],
          },
          {
            resolveOffset: (offset) => resolvedOffsets.push(offset),
            consumer: {
              commitOffsets: async () => {
                commitAttempts += 1;
                throw new Error("Kafka commit failed");
              },
            } as unknown as Consumer,
          },
          {
            upsert: (() => {
              let calls = 0;
              return (trade: NewDexTrade) => {
                calls += 1;
                if (calls === 1) {
                  return Effect.succeed(fixtureTrade());
                }

                return Effect.fail(
                  new DbPersistError({
                    message: "Failed to persist DEX trade",
                    table: "dex_trades",
                    uniqueId: trade.uniqueId,
                    cause: new Error("database offline"),
                  }),
                );
              };
            })(),
          },
        ),
      );

      expect(error._tag).toBe("RetryableIngesterBatch");
      expect(error.message).toBe("Failed to persist DEX trade");
      if (!("source" in error)) {
        throw new Error("Expected a retryable ingester batch error");
      }
      expect(error.source.offset).toBe(KafkaOffset.make("42"));
      expect(resolvedOffsets).toEqual(["41"]);
      expect(commitAttempts).toBe(1);
    }),
  );
});

function batchEffect(
  batch: { messages: EachBatchPayload["batch"]["messages"] },
  hooks: {
    resolveOffset: (offset: string) => void;
    consumer: Consumer;
  },
  repositoryOverrides: Partial<DexTradeRepository["Service"]> = {},
) {
  const payload = fixtureBatchPayload(batch.messages, hooks.resolveOffset);

  return processDexTradeBatch(
    { consumer: hooks.consumer, isShuttingDown: () => false },
    payload,
  ).pipe(Effect.provide(testLayer(repositoryOverrides)));
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

function fixtureBatchPayload(
  messages: EachBatchPayload["batch"]["messages"],
  resolveOffset: (offset: string) => void,
): EachBatchPayload {
  return {
    batch: {
      topic: "dex.trades",
      partition: 0,
      highWatermark: "100",
      messages,
      isEmpty: () => messages.length === 0,
      firstOffset: () => messages[0]?.offset ?? "",
      lastOffset: () => messages[messages.length - 1]?.offset ?? "",
      offsetLag: () => "0",
      offsetLagLow: () => "0",
    },
    resolveOffset,
    heartbeat: async () => undefined,
    isRunning: () => true,
    isStale: () => false,
    pause: () => () => undefined,
    commitOffsetsIfNecessary: async () => undefined,
    uncommittedOffsets: () => ({ topics: [] }),
  } as EachBatchPayload;
}

function message(offset: string, value: string | undefined): KafkaMessage {
  return {
    key: null,
    value: value === undefined ? null : Buffer.from(value),
    timestamp: "0",
    attributes: 0,
    offset,
    size: 0,
  };
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
