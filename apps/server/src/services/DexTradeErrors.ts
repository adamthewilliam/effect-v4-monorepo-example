import { Schema } from "effect";

export class DatabaseUnavailableError extends Schema.TaggedErrorClass<DatabaseUnavailableError>()(
  "DatabaseUnavailable",
  {
    message: Schema.String,
  },
) {}
