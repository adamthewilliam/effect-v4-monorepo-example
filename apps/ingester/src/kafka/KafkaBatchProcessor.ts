import { Effect } from "effect";
import type { Consumer, EachBatchPayload } from "kafkajs";
import type { IngesterError } from "../contracts/IngesterErrors";
import {
  KafkaOffset,
  KafkaPartition,
  KafkaSource,
  KafkaTopic,
  RetryableIngesterBatchError,
  shouldCommitOffset,
} from "../contracts/IngesterErrors";
import { DexTradeIngesterService } from "../services/DexTradeIngesterService";

export function nextOffset(offset: KafkaOffset): KafkaOffset {
  return KafkaOffset.make((BigInt(offset) + 1n).toString());
}

export type BatchDeps = {
  readonly consumer: Consumer;
  readonly isShuttingDown: () => boolean;
};

export const processDexTradeBatch = Effect.fn("KafkaBatchProcessor.processBatch")(function* (
  deps: BatchDeps,
  { batch, heartbeat, isRunning, isStale, resolveOffset }: EachBatchPayload,
) {
  let lastResolvedOffset: KafkaOffset | undefined;

  const markOffsetProcessed = Effect.fn("KafkaBatchProcessor.markOffsetProcessed")(function* (
    offset: KafkaOffset,
  ) {
    yield* Effect.sync(() => resolveOffset(offset));
    lastResolvedOffset = offset;
    yield* Effect.tryPromise(() => heartbeat());
  });

  const commitProcessedOffsets = Effect.fn("KafkaBatchProcessor.commitProcessedOffsets")(
    function* () {
      const resolvedOffset = lastResolvedOffset;

      if (resolvedOffset === undefined) {
        return;
      }

      const commitOffset = nextOffset(resolvedOffset);
      const source = new KafkaSource({
        topic: KafkaTopic.make(batch.topic),
        partition: KafkaPartition.make(batch.partition),
        offset: resolvedOffset,
      });

      yield* Effect.tryPromise({
        try: () =>
          deps.consumer.commitOffsets([
            {
              topic: batch.topic,
              partition: batch.partition,
              offset: commitOffset,
            },
          ]),
        catch: (cause) =>
          new RetryableIngesterBatchError({
            message: "Failed to commit Kafka offsets",
            source,
            cause,
            retryable: true,
          }),
      }).pipe(
        Effect.tapError((error) =>
          Effect.logError(error.message, {
            source: error.source,
            commitOffset,
            cause: error.cause,
          }),
        ),
      );
    },
  );

  const commitProcessedOffsetsBestEffort = Effect.fn(
    "KafkaBatchProcessor.commitProcessedOffsetsBestEffort",
  )(function* () {
    yield* commitProcessedOffsets().pipe(Effect.ignore);
  });

  const failBatchAfterBestEffortCommit = Effect.fn(
    "KafkaBatchProcessor.failBatchAfterBestEffortCommit",
  )(function* (error: IngesterError) {
    // DB failures leave the current message unresolved; this only preserves earlier progress.
    yield* commitProcessedOffsetsBestEffort();

    return yield* new RetryableIngesterBatchError({
      message: error.message,
      source: error.source,
      cause: error,
      retryable: true,
    });
  });

  const applyOffsetPolicy = Effect.fn("KafkaBatchProcessor.applyOffsetPolicy")(function* (
    error: IngesterError,
  ) {
    yield* logIngesterError(error);

    if (shouldCommitOffset(error)) {
      return yield* markOffsetProcessed(error.source.offset);
    }

    return yield* failBatchAfterBestEffortCommit(error);
  });

  for (const message of batch.messages) {
    if (deps.isShuttingDown() || !isRunning() || isStale()) {
      break;
    }

    const source = new KafkaSource({
      topic: KafkaTopic.make(batch.topic),
      partition: KafkaPartition.make(batch.partition),
      offset: KafkaOffset.make(message.offset),
    });
    const rawMessage = message.value?.toString();

    yield* DexTradeIngesterService.use((service) => service.ingestMessage(rawMessage, source)).pipe(
      Effect.matchEffect({
        onFailure: applyOffsetPolicy,
        onSuccess: () => markOffsetProcessed(source.offset),
      }),
    );
  }

  // Normal path: commit once after the batch for the latest resolved offset.
  yield* commitProcessedOffsets();
});

const logIngesterError = Effect.fn("KafkaBatchProcessor.logIngesterError")(function* (
  error: IngesterError,
) {
  switch (error._tag) {
    case "EmptyKafkaMessage":
    case "InvalidKafkaJson":
    case "InvalidDexTradeMessage":
      return yield* Effect.logWarning("Skipping non-retryable DEX trade message", error);
    case "IngesterPersistFailed":
      return yield* Effect.logError("Retryable DEX trade ingestion failure", error);
    default:
      return assertNever(error);
  }
});

function assertNever(value: never): never {
  throw new Error(`Unhandled ingester error: ${JSON.stringify(value)}`);
}
