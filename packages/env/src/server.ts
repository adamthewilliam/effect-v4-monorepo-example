import { Config, Context, Effect, Layer, Schema } from "effect";

export class ServerEnv extends Context.Service<ServerEnv>()("@effect-monorepo/env/ServerEnv", {
  make: Effect.gen(function* () {
    return {
      DATABASE_URL: yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "DATABASE_URL"),
      PORT: yield* Config.port("PORT").pipe(Config.withDefault(3000)),
      CORS_ORIGIN: yield* Config.url("CORS_ORIGIN").pipe(
        Config.map((url) => url.origin),
        Config.withDefault("http://localhost:3000"),
      ),
    } as const;
  }),
}) {
  static readonly layer = Layer.effect(this, this.make);
}
