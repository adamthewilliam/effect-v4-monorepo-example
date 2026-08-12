import { Schema } from "effect";

export class DatabaseUnavailableError extends Schema.TaggedErrorClass<DatabaseUnavailableError>()(
  "DatabaseUnavailable",
  {
    message: Schema.String,
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class DatabaseQueryFailedError extends Schema.TaggedErrorClass<DatabaseQueryFailedError>()(
  "DatabaseQueryFailed",
  {
    message: Schema.String,
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}
