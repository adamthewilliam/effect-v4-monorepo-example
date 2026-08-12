import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { Effect, Layer, type Scope } from "effect";
import { TestClock, TestConsole } from "effect/testing";

export { afterAll, afterEach, beforeAll, beforeEach, describe, expect };

type TestOptions = number | { timeout?: number; retry?: number; repeats?: number };
type EffectTestServices = TestClock.TestClock | TestConsole.TestConsole;
type EffectTestFn<A, E, R> = () => Effect.Effect<A, E, R>;

const TestEnv = Layer.mergeAll(TestConsole.layer, TestClock.layer());

export const runEffect = <A, E>(effect: Effect.Effect<A, E, EffectTestServices>) =>
  Effect.runPromise(effect.pipe(Effect.provide(TestEnv)));

export const runScopedEffect = <A, E>(
  effect: Effect.Effect<A, E, EffectTestServices | Scope.Scope>,
) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(TestEnv)));

type EffectTester<R> = {
  <A, E>(name: string, fn: EffectTestFn<A, E, R>, options?: TestOptions): void;
  skip: <A, E>(name: string, fn: EffectTestFn<A, E, R>, options?: TestOptions) => void;
  only: <A, E>(name: string, fn: EffectTestFn<A, E, R>, options?: TestOptions) => void;
};

const makeEffectTest =
  <R>(runner: typeof test, run: <A, E>(effect: Effect.Effect<A, E, R>) => Promise<A>) =>
  <A, E>(name: string, fn: EffectTestFn<A, E, R>, options?: TestOptions) => {
    runner(name, () => run(fn()), options);
  };

export const effect: EffectTester<EffectTestServices> = Object.assign(
  makeEffectTest(test, runEffect),
  {
    skip: makeEffectTest(test.skip, runEffect),
    only: makeEffectTest(test.only, runEffect),
  },
);

export const scoped: EffectTester<EffectTestServices | Scope.Scope> = Object.assign(
  makeEffectTest(test, runScopedEffect),
  {
    skip: makeEffectTest(test.skip, runScopedEffect),
    only: makeEffectTest(test.only, runScopedEffect),
  },
);

export const it = Object.assign(test, { effect, scoped });
