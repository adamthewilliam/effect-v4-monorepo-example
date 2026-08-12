import { Config, Context, Effect, Layer, Schema } from "effect";

export class IngesterEnv extends Context.Service<IngesterEnv>()(
  "@effect-monorepo/env/IngesterEnv",
  {
    make: Effect.gen(function* () {
      return {
        DATABASE_URL: yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "DATABASE_URL"),
        KAFKA_BROKERS: yield* Config.nonEmptyString("KAFKA_BROKERS").pipe(
          Config.withDefault("localhost:29092"),
        ),
        KAFKA_CLIENT_ID: yield* Config.nonEmptyString("KAFKA_CLIENT_ID").pipe(
          Config.withDefault("dex-trades-ingester"),
        ),
        KAFKA_GROUP_ID: yield* Config.nonEmptyString("KAFKA_GROUP_ID").pipe(
          Config.withDefault("dex-trades-ingester"),
        ),
        KAFKA_TOPIC: yield* Config.nonEmptyString("KAFKA_TOPIC").pipe(
          Config.withDefault("dex.trades"),
        ),
        // Start from latest by default to avoid replaying old mock trades during local runs.
        KAFKA_FROM_BEGINNING: yield* Config.boolean("KAFKA_FROM_BEGINNING").pipe(
          Config.withDefault(false),
        ),
      } as const;
    }),
  },
) {
  static readonly layer = Layer.effect(this, this.make);
}
