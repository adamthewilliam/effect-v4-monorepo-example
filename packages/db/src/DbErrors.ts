import { Schema } from "effect";
import { DexTradeId } from "./schema";

export const DbFailureKind = Schema.Literals(["transient", "constraint", "fatal"] as const);
export type DbFailureKind = typeof DbFailureKind.Type;

export class DbPersistError extends Schema.TaggedErrorClass<DbPersistError>()("DbPersistError", {
  message: Schema.String,
  operation: Schema.String,
  table: Schema.optionalKey(Schema.String),
  uniqueId: Schema.optionalKey(DexTradeId),
  cause: Schema.Defect(),
  kind: DbFailureKind,
}) {}

export class DbQueryError extends Schema.TaggedErrorClass<DbQueryError>()("DbQueryError", {
  message: Schema.String,
  table: Schema.optionalKey(Schema.String),
  operation: Schema.String,
  cause: Schema.Defect(),
  kind: DbFailureKind,
}) {}

export class DbReadinessError extends Schema.TaggedErrorClass<DbReadinessError>()(
  "DbReadinessError",
  {
    message: Schema.String,
    operation: Schema.String,
    cause: Schema.Defect(),
    kind: DbFailureKind,
  },
) {}
