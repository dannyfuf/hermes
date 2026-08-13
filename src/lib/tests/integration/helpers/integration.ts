import { assertEquals } from "@std/assert";
import { Queue } from "bullmq";
import { Hermes } from "../../../../main.ts";
import type {
  BackendAdapter,
  HermesHooks,
  HermesInstance,
  LoggerSink,
  QueueStats,
  WorkerConfig,
} from "../../../../main.ts";
import {
  BullMQBackend,
  type BullMQBackendOptions,
} from "../../../backends/bullmq.ts";
import { type RedisTestConfig, redisTestConfig } from "./env_local.ts";

export type SinkRecord = Record<string, unknown>;

export const redis: RedisTestConfig = await redisTestConfig();

async function redisIsReachable(config: RedisTestConfig): Promise<boolean> {
  try {
    const connection = await Deno.connect({
      hostname: config.host,
      port: config.port,
    });
    connection.close();
    return true;
  } catch {
    return false;
  }
}

export const redisAvailable: boolean = await redisIsReachable(redis);
export const slowTestsEnabled: boolean =
  Deno.env.get("HERMES_SLOW_TESTS") === "1";

export function integrationTest(
  name: string,
  fn: () => Promise<void>,
  options: { slow?: boolean } = {},
): void {
  Deno.test({
    name,
    ignore: !redisAvailable || (options.slow === true && !slowTestsEnabled),
    sanitizeOps: false,
    sanitizeResources: false,
    fn,
  });
}

export async function readSink(path: string): Promise<SinkRecord[]> {
  try {
    const contents = await Deno.readTextFile(path);
    return contents.trim().split("\n").filter(Boolean).map((line) =>
      JSON.parse(line) as SinkRecord
    );
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) return [];
    throw error;
  }
}

export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

export async function waitForElapsed(durationMs: number): Promise<void> {
  const startedAt = Date.now();
  await waitFor(
    () => Date.now() - startedAt >= durationMs,
    `Did not observe ${durationMs}ms elapse`,
    durationMs + 1_000,
  );
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timeoutId);
  }
}

export function queueConnection(): RedisTestConfig {
  return { ...redis };
}

export class IntegrationScope {
  readonly queueName: string;
  readonly secondaryQueueName: string;
  readonly sinkPath: string;
  private readonly tempDir: string;
  private readonly queueNames = new Set<string>();
  private readonly backends: BackendAdapter[] = [];
  private readonly instances: HermesInstance[] = [];

  private constructor(testId: string, tempDir: string) {
    const uniqueId = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
    this.queueName = `test_hermes_${testId}_${uniqueId}`;
    this.secondaryQueueName = `${this.queueName}_secondary`;
    this.tempDir = tempDir;
    this.sinkPath = `${tempDir}/sink.ndjson`;
    this.queueNames.add(this.queueName);
    this.queueNames.add(this.secondaryQueueName);
    Deno.env.set("HERMES_INTEGRATION_QUEUE", this.queueName);
    Deno.env.set(
      "HERMES_INTEGRATION_SECONDARY_QUEUE",
      this.secondaryQueueName,
    );
    Deno.env.set("HERMES_INTEGRATION_SINK", this.sinkPath);
  }

  static async create(testId: string): Promise<IntegrationScope> {
    return new IntegrationScope(testId, await Deno.makeTempDir());
  }

  backend(
    options: Omit<BullMQBackendOptions, "connection"> = {},
  ): BackendAdapter {
    const backend = BullMQBackend({
      connection: queueConnection(),
      ...options,
    });
    this.backends.push(backend);
    return backend;
  }

  hermes(
    manifest: string,
    backend: BackendAdapter,
    worker: WorkerConfig = {},
    extras: { hooks?: HermesHooks; logger?: LoggerSink } = {},
  ): HermesInstance {
    const instance = Hermes({
      manifest,
      backend,
      worker: { gracefulShutdownTimeout: 250, ...worker },
      ...extras,
    });
    this.instances.push(instance);
    return instance;
  }

  inspector(queueName = this.queueName): Queue {
    this.queueNames.add(queueName);
    return new Queue(queueName, { connection: queueConnection() });
  }

  async obliterateQueues(): Promise<void> {
    for (const queueName of this.queueNames) {
      const queue = this.inspector(queueName);
      try {
        await queue.obliterate({ force: true });
      } finally {
        await queue.close();
      }
    }
  }

  async cleanup(): Promise<void> {
    let cleanupError: unknown;

    for (const instance of this.instances.toReversed()) {
      try {
        await instance.stop();
      } catch (error) {
        cleanupError ??= error;
      }
    }
    for (const backend of this.backends.toReversed()) {
      try {
        await backend.close({ force: true });
      } catch (error) {
        cleanupError ??= error;
      }
    }
    try {
      await this.obliterateQueues();
    } catch (error) {
      cleanupError ??= error;
    }
    try {
      await Deno.remove(this.tempDir, { recursive: true });
    } catch (error) {
      cleanupError ??= error;
    }

    if (cleanupError) throw cleanupError;
  }
}

export async function runIntegration(
  testId: string,
  test: (scope: IntegrationScope) => Promise<void>,
): Promise<void> {
  const scope = await IntegrationScope.create(testId);
  try {
    await test(scope);
  } finally {
    await scope.cleanup();
  }
}

export async function onlyQueueStats(
  hermes: HermesInstance,
): Promise<QueueStats> {
  const stats = await hermes.stats();
  assertEquals(stats.length, 1);
  return stats[0];
}
