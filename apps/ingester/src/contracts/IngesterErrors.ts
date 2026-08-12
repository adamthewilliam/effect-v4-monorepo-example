import { DexTradeId, type DbFailureKind } from "@effect-monorepo/db";
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

export const KafkaSource = Schema.Struct({
  topic: KafkaTopic,
  partition: KafkaPartition,
  offset: KafkaOffset,
});
export interface KafkaSource extends Schema.Schema.Type<typeof KafkaSource> {}

export const PersistenceAction = Schema.Literals(["retry", "skip", "halt"] as const);
export type PersistenceAction = typeof PersistenceAction.Type;

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
    uniqueId: Schema.optionalKey(DexTradeId),
    cause: Schema.Defect(),
    action: PersistenceAction,
  },
) {}

export class RetryableIngesterBatchError extends Schema.TaggedErrorClass<RetryableIngesterBatchError>()(
  "RetryableIngesterBatch",
  {
    message: Schema.String,
    source: KafkaSource,
    cause: Schema.Defect(),
    action: Schema.Literal("retry"),
  },
) {}

export class FatalIngesterBatchError extends Schema.TaggedErrorClass<FatalIngesterBatchError>()(
  "FatalIngesterBatch",
  {
    message: Schema.String,
    source: KafkaSource,
    cause: Schema.Defect(),
    action: Schema.Literal("halt"),
  },
) {}

export class KafkaConsumerError extends Schema.TaggedErrorClass<KafkaConsumerError>()(
  "KafkaConsumerFailed",
  {
    message: Schema.String,
    operation: Schema.String,
    cause: Schema.Defect(),
    action: Schema.Literal("halt"),
  },
) {}

export type ParseDexTradeMessageError = InvalidKafkaJsonError | InvalidDexTradeMessageError;

export type IngesterError =
  | EmptyKafkaMessageError
  | InvalidKafkaJsonError
  | InvalidDexTradeMessageError
  | IngesterPersistError;

export function actionForError(error: IngesterError): PersistenceAction {
  switch (error._tag) {
    case "EmptyKafkaMessage":
    case "InvalidKafkaJson":
    case "InvalidDexTradeMessage":
      return "skip";
    case "IngesterPersistFailed":
      return error.action;
    default:
      return assertNever(error);
  }
}

export function actionForDbFailure(kind: DbFailureKind): PersistenceAction {
  switch (kind) {
    case "transient":
      return "retry";
    case "constraint":
      return "skip";
    case "fatal":
      return "halt";
    default:
      return assertNever(kind);
  }
}

function assertNever(value: never): never {
  throw new Error(`Unhandled ingester error: ${JSON.stringify(value)}`);
}
