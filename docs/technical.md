# Technical Guide

The runnable workspace is the repository root.

## Stack

- Bun and TypeScript
- Effect 4 beta
- Effect HttpApi on Bun
- KafkaJS for event consumption
- PostgreSQL with Drizzle and Effect SQL
- Turborepo for workspace tasks
- Oxlint and Oxfmt for static checks and formatting

## Workspace packages

### Applications

`apps/server` owns the HTTP boundary. It defines the API contract, maps
service failures to HTTP responses, and composes the live layers needed by the
server.

`apps/ingester` owns Kafka consumption. It validates incoming messages,
processes batches, and delegates persistence to the database repository.

### Libraries

`packages/db` owns the database client, schema, migrations, and repository
interfaces. Database details do not leak into the HTTP or Kafka adapters.

`packages/env` exposes typed configuration as Effect services. Application
code depends on these services instead of reading `process.env` directly.

`packages/testing` contains shared test layers and Bun-specific helpers.

`packages/config` contains compiler and workspace defaults shared by all
packages.

## Local development

From the repository root:

```bash
bun install
cp apps/server/.env.example apps/server/.env
cp apps/ingester/.env.example apps/ingester/.env
bun run docker:up
bun run db:migrate
```

The Compose file provides Kafka and PostgreSQL. The ingester expects records on
the `dex.trades` topic; a producer can be supplied independently.

Useful commands:

```bash
bun run dev:server
bun run dev:ingester
bun run check-types
bun run test
bun run build
bun run check
bun run docker:logs
bun run docker:down
```

## Effect conventions

- Use `Effect.gen` and `Effect.fn` for workflows and service methods.
- Model boundary data with `Schema`.
- Keep expected failures typed and close to the service that owns them.
- Construct live dependencies with `Layer`.
- Keep external I/O at adapters and keep domain services independently
  testable.
- Use explicit test layers and synchronization primitives instead of timing
  assumptions.
