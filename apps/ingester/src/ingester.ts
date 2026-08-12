import { DexTradeRepository, makeDbLayer } from "@effect-monorepo/db";
import { IngesterEnv } from "@effect-monorepo/env/ingester";
import { Effect, Layer, ManagedRuntime } from "effect";
import { Kafka } from "kafkajs";

import { processDexTradeBatch } from "./kafka/KafkaBatchProcessor";
import { DexTradeIngesterService } from "./services/DexTradeIngesterService";

const env = await Effect.runPromise(IngesterEnv.make);
const ingesterDbLayer = makeDbLayer(env.DATABASE_URL);
const ingesterLayer = DexTradeIngesterService.layer.pipe(
  Layer.provideMerge(DexTradeRepository.layer),
  Layer.provideMerge(ingesterDbLayer),
);
const ingesterRuntime = ManagedRuntime.make(ingesterLayer);
const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers: env.KAFKA_BROKERS.split(",").map((broker) => broker.trim()),
});

const kafkaConsumer = kafka.consumer({ groupId: env.KAFKA_GROUP_ID });

let shuttingDown = false;
let activeBatch: Promise<void> | undefined;

registerKafkaInstrumentation(kafkaConsumer);
registerShutdownHandlers();

await startConsumer();

async function startConsumer() {
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({
    topic: env.KAFKA_TOPIC,
    fromBeginning: env.KAFKA_FROM_BEGINNING,
  });

  await ingesterRuntime.runPromise(
    Effect.logInfo(
      `Consuming ${env.KAFKA_TOPIC} from ${env.KAFKA_BROKERS} as group ${env.KAFKA_GROUP_ID}`,
      { fromBeginning: env.KAFKA_FROM_BEGINNING },
    ),
  );

  await kafkaConsumer.run({
    // Manual batch commits let us skip poison messages without advancing past DB failures.
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: (payload) => {
      // KafkaJS calls us with a Promise API; ManagedRuntime is the thin Effect bridge.
      const batchRun = ingesterRuntime
        .runPromise(
          processDexTradeBatch(
            { consumer: kafkaConsumer, isShuttingDown: () => shuttingDown },
            payload,
          ),
        )
        .finally(() => {
          if (activeBatch === batchRun) {
            activeBatch = undefined;
          }
        });

      activeBatch = batchRun;
      return batchRun;
    },
  });
}

async function shutdown(signal: string) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  await ingesterRuntime.runPromise(Effect.logInfo(`Received ${signal}; stopping Kafka ingester`));

  let exitCode = 0;

  try {
    if (activeBatch !== undefined) {
      await activeBatch.catch(() => undefined);
    }

    await kafkaConsumer.stop();
    await kafkaConsumer.disconnect();
  } catch (error) {
    exitCode = 1;
    await ingesterRuntime
      .runPromise(Effect.logError("Kafka ingester shutdown failed", { error }))
      .catch(() => undefined);
  } finally {
    await ingesterRuntime.dispose().catch(() => undefined);
    process.exit(exitCode);
  }
}

function registerShutdownHandlers() {
  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

function registerKafkaInstrumentation(consumer: typeof kafkaConsumer) {
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    void ingesterRuntime.runPromise(
      Effect.logError("Kafka consumer crashed", {
        groupId: payload.groupId,
        restart: payload.restart,
        error: payload.error,
      }),
    );
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    void ingesterRuntime.runPromise(
      Effect.logInfo("Kafka consumer joined group", {
        groupId: payload.groupId,
        memberId: payload.memberId,
        leaderId: payload.leaderId,
        isLeader: payload.isLeader,
        memberAssignment: payload.memberAssignment,
      }),
    );
  });

  consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => {
    void ingesterRuntime.runPromise(
      Effect.logInfo("Kafka offsets committed", {
        groupId: payload.groupId,
        memberId: payload.memberId,
        topics: payload.topics,
      }),
    );
  });
}
