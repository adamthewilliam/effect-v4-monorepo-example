import { Effect } from "effect";
import type { Consumer, EachBatchPayload } from "kafkajs";
import type { IngesterError } from "../contracts/IngesterErrors";
import {
  actionForError,
  FatalIngesterBatchError,
  KafkaOffset,
  KafkaPartition,
  KafkaSource,
  KafkaTopic,
  RetryableIngesterBatchError,
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
    const source = sourceForOffset(batch, offset);

    yield* Effect.try({
      try: () => resolveOffset(offset),
      catch: (cause) =>
        new RetryableIngesterBatchError({
          message: "Failed to resolve Kafka offset",
          source,
          cause,
          action: "retry",
        }),
    });
    lastResolvedOffset = offset;
    yield* Effect.tryPromise({
      try: () => heartbeat(),
      catch: (cause) =>
        new RetryableIngesterBatchError({
          message: "Failed to heartbeat Kafka batch",
          source,
          cause,
          action: "retry",
        }),
    });
  });

  const commitProcessedOffsets = Effect.fn("KafkaBatchProcessor.commitProcessedOffsets")(
    function* () {
      const resolvedOffset = lastResolvedOffset;

      if (resolvedOffset === undefined) {
        return;
      }

      const commitOffset = nextOffset(resolvedOffset);
      const source = sourceForOffset(batch, resolvedOffset);

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
            action: "retry",
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
  )(function* (error: IngesterError, action: "retry" | "halt") {
    // DB failures leave the current message unresolved; this only preserves earlier progress.
    yield* commitProcessedOffsetsBestEffort();

    if (action === "retry") {
      return yield* new RetryableIngesterBatchError({
        message: error.message,
        source: error.source,
        cause: error,
        action,
      });
    }

    return yield* new FatalIngesterBatchError({
      message: error.message,
      source: error.source,
      cause: error,
      action,
    });
  });

  const applyOffsetPolicy = Effect.fn("KafkaBatchProcessor.applyOffsetPolicy")(function* (
    error: IngesterError,
  ) {
    yield* logIngesterError(error);

    const action = actionForError(error);

    switch (action) {
      case "skip":
        return yield* markOffsetProcessed(error.source.offset);
      case "retry":
      case "halt":
        return yield* failBatchAfterBestEffortCommit(error, action);
      default:
        return assertNever(action);
    }
  });

  for (const message of batch.messages) {
    if (deps.isShuttingDown() || !isRunning() || isStale()) {
      break;
    }

    const source = sourceForOffset(batch, KafkaOffset.make(message.offset));
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
      switch (error.action) {
        case "retry":
          return yield* Effect.logError("Retryable DEX trade ingestion failure", error);
        case "skip":
          return yield* Effect.logWarning("Skipping DEX trade ingestion failure", error);
        case "halt":
          return yield* Effect.logError("Fatal DEX trade ingestion failure", error);
        default:
          return assertNever(error.action);
      }
    default:
      return assertNever(error);
  }
});

function assertNever(value: never): never {
  throw new Error(`Unhandled ingester error: ${JSON.stringify(value)}`);
}

function sourceForOffset(batch: EachBatchPayload["batch"], offset: KafkaOffset): KafkaSource {
  return KafkaSource.make({
    topic: KafkaTopic.make(batch.topic),
    partition: KafkaPartition.make(batch.partition),
    offset,
  });
}
