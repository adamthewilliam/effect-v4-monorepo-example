import { Schema } from "effect";
import { DexTradeId } from "./schema";

export class DbPersistError extends Schema.TaggedErrorClass<DbPersistError>()("DbPersistError", {
  message: Schema.String,
  table: Schema.optional(Schema.String),
  uniqueId: Schema.optional(DexTradeId),
  cause: Schema.Defect(),
}) {}

export class DbQueryError extends Schema.TaggedErrorClass<DbQueryError>()("DbQueryError", {
  message: Schema.String,
  table: Schema.optional(Schema.String),
  operation: Schema.String,
  cause: Schema.Defect(),
  retryable: Schema.Boolean,
}) {}

export class DbReadinessError extends Schema.TaggedErrorClass<DbReadinessError>()(
  "DbReadinessError",
  {
    message: Schema.String,
    cause: Schema.Defect(),
    retryable: Schema.Boolean,
  },
) {}
