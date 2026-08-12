import { PgClient } from "@effect/sql-pg";
import { Context, Effect, Layer, Redacted } from "effect";
import * as PgDrizzle from "drizzle-orm/effect-postgres";
import { types } from "pg";

const passthroughPgTypeIds = [
  1082, // date
  1114, // timestamp
  1115, // timestamp[]
  1182, // date[]
  1184, // timestamptz
  1185, // timestamptz[]
  1186, // interval
  1187, // interval[]
  1231, // numeric[]
];

export const createDb = PgDrizzle.makeWithDefaults();

export type Db = Effect.Success<typeof createDb>;

export class DbClient extends Context.Service<DbClient, Db>()("@effect-monorepo/db/DbClient") {}

export function makeDbLayer(databaseUrl: Redacted.Redacted<string> | string) {
  return Layer.effect(DbClient, createDb).pipe(
    Layer.provide(
      PgClient.layer({
        url: Redacted.isRedacted(databaseUrl) ? databaseUrl : Redacted.make(databaseUrl),
        types: {
          getTypeParser: (typeId, format) => {
            if (passthroughPgTypeIds.includes(typeId)) {
              return (value: string) => value;
            }

            return types.getTypeParser(typeId, format);
          },
        },
      }),
    ),
  );
}
