import { DexTradeId } from "@effect-monorepo/db";
import { Schema } from "effect";

export const KafkaTopic = Schema.NonEmptyString.pipe(Schema.brand("@effect-monorepo/KafkaTopic"));
export type KafkaTopic = typeof KafkaTopic.Type;

export const KafkaPartition = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
  Schema.brand("@effect-monorepo/KafkaPartition"),
);
export type KafkaPartition = typeof KafkaPartition.Type;

export const KafkaOffset = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^(0|[1-9]\d*)$/)),
  Schema.brand("@effect-monorepo/KafkaOffset"),
);
export type KafkaOffset = typeof KafkaOffset.Type;

export class KafkaSource extends Schema.Class<KafkaSource>("KafkaSource")({
  topic: KafkaTopic,
  partition: KafkaPartition,
  offset: KafkaOffset,
}) {}

export class EmptyKafkaMessageError extends Schema.TaggedErrorClass<EmptyKafkaMessageError>()(
  "EmptyKafkaMessage",
  {
    message: Schema.String,
    source: KafkaSource,
  },
) {}

export class InvalidKafkaJsonError extends Schema.TaggedErrorClass<InvalidKafkaJsonError>()(
  "InvalidKafkaJson",
  {
    message: Schema.String,
    source: KafkaSource,
    cause: Schema.Defect(),
  },
) {}

export class InvalidDexTradeMessageError extends Schema.TaggedErrorClass<InvalidDexTradeMessageError>()(
  "InvalidDexTradeMessage",
  {
    message: Schema.String,
    source: KafkaSource,
    details: Schema.Defect(),
  },
) {}

export class IngesterPersistError extends Schema.TaggedErrorClass<IngesterPersistError>()(
  "IngesterPersistFailed",
  {
    message: Schema.String,
    source: KafkaSource,
    uniqueId: Schema.optional(DexTradeId),
    cause: Schema.Defect(),
  },
) {}

export class RetryableIngesterBatchError extends Schema.TaggedErrorClass<RetryableIngesterBatchError>()(
  "RetryableIngesterBatch",
  {
    message: Schema.String,
    source: KafkaSource,
    cause: Schema.Defect(),
    retryable: Schema.Literal(true),
  },
) {}

export type ParseDexTradeMessageError = InvalidKafkaJsonError | InvalidDexTradeMessageError;

export type IngesterError =
  | EmptyKafkaMessageError
  | InvalidKafkaJsonError
  | InvalidDexTradeMessageError
  | IngesterPersistError;

export function shouldCommitOffset(error: IngesterError) {
  switch (error._tag) {
    case "EmptyKafkaMessage":
    case "InvalidKafkaJson":
    case "InvalidDexTradeMessage":
      return true;
    case "IngesterPersistFailed":
      return false;
    default:
      return assertNever(error);
  }
}

function assertNever(value: never): never {
  throw new Error(`Unhandled ingester error: ${JSON.stringify(value)}`);
}
