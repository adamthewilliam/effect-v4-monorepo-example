# Effect Monorepo

A small, runnable north star for structuring TypeScript services with Effect.

The example is a DEX trade pipeline: a Bun HTTP API reads from Postgres, while
an Effect-based ingester consumes Kafka events and persists validated domain
data. The point is the shape of the monorepo and its boundaries, not the
trading domain.

## Repository layout

- `apps/server` — Effect HttpApi, health routes, OpenAPI, and Scalar documentation.
- `apps/ingester` — Kafka consumer and batch processing.
- `packages/db` — schema, migrations, database client, and repositories.
- `packages/env` — typed runtime configuration.
- `packages/testing` — shared Bun and Effect test helpers.
- `packages/config` — shared TypeScript configuration.

## Quick start

```bash
bun install
cp apps/server/.env.example apps/server/.env
cp apps/ingester/.env.example apps/ingester/.env
bun run docker:up
bun run db:migrate
```

Run the API and ingester in separate terminals:

```bash
bun run dev:server
bun run dev:ingester
```

The API is available at `http://localhost:3000`.

## Documentation

- [Vision](docs/vision.md)
- [Technical guide](docs/technical.md)
- [Architecture](docs/architecture.md)

## Validation

```bash
bun run check-types
bun run test
bun run build
bun run check
```
