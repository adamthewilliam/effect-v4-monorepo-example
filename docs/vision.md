# Vision

This repository is a reference point for Effect TypeScript monorepos that are
small enough to understand and complete enough to run.

## The north star

Every package should have one clear responsibility, every boundary should have
an explicit contract, and every runtime dependency should be visible in the
composition of the application.

The example favors:

- typed domain values over unvalidated strings;
- services and layers over hidden global state;
- thin transport and ingestion adapters;
- deterministic tests with explicit dependencies;
- local infrastructure that can be started and stopped easily;
- boring, inspectable code over framework ceremony.

## What this is meant to teach

The DEX trade pipeline provides a concrete setting for showing how an Effect
monorepo can separate:

1. transport concerns from application services;
2. domain schemas from persistence details;
3. runtime configuration from business logic;
4. infrastructure wiring from reusable modules;
5. a clear path from unit-level service tests to integration smoke tests.

The domain can change without changing the architectural rules. A future
example could process payments, events, files, or telemetry and keep the same
package boundaries.

## What does not belong here

This is not a catalog of every Effect feature, a distributed-systems
framework, or a claim that one layout fits every team. New abstractions should
earn their place by making a boundary clearer, a failure more explicit, or a
test more deterministic.
