# Technical Guide

The runnable workspace is the repository root.

## Stack

- Bun and TypeScript
- Effect 4 beta
- Effect HttpApi on Bun
- Effect TSGO for native TypeScript 7 Effect diagnostics and refactors
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

`bun install` runs `effect-tsgo patch --typescript --oxlint`, which enables the
Effect language service in TypeScript-Go and the Effect-aware Oxlint bridge.
The shared `packages/config/tsconfig.base.json` contains the language-service
plugin configuration, and the workspace VS Code settings select the native
TypeScript-Go service.

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

The default test suite uses explicit repository and Kafka adapters, so it is
fast and deterministic. It does not require Postgres or Kafka. The Compose
stack provides the manual integration smoke path for validating migrations,
database queries, and message ingestion together.

## Effect conventions

- Use `Effect.gen` and `Effect.fn` for workflows and service methods.
- Model boundary data with `Schema`.
- Keep expected failures typed and close to the service that owns them.
- Construct live dependencies with `Layer`.
- Keep external I/O at adapters and keep domain services independently
  testable.
- Preserve monetary and token amounts as decimal strings; use `BigDecimal`
  for arithmetic instead of converting through JavaScript `number`.
- Normalize database failures to `transient`, `constraint`, or `fatal` in the
  repository. Callers choose their own policy: HTTP maps transient failures to
  `503`, while ingestion maps failures to `retry`, `skip`, or `halt`.
- Keep database causes and operation metadata for internal diagnostics; never
  expose them in HTTP error response bodies.
- Use explicit test layers and synchronization primitives instead of timing
  assumptions.
