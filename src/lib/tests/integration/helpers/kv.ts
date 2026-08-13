import { Hermes } from "../../../../main.ts";
import type {
  BackendAdapter,
  HermesHooks,
  HermesInstance,
  LoggerSink,
  WorkerConfig,
} from "../../../../main.ts";
import { DenoKvBackend } from "../../../backends/deno_kv.ts";
import { clearBackend } from "../../../backend_registry.ts";
import { clearHooks, clearLoggerSink } from "../../../hooks_registry.ts";

/**
 * Deno KV twin of IntegrationScope: temp-dir KV store shared by every
 * backend the scope creates (so restart/durability tests see the same
 * store), env-driven queue names for the shared fixture jobs, and cleanup
 * that closes everything and clears the module-global registries.
 */
export class KvScope {
  readonly queueName: string;
  readonly secondaryQueueName: string;
  readonly sinkPath: string;
  readonly kvPath: string;
  private readonly tempDir: string;
  private readonly backends: BackendAdapter[] = [];
  private readonly instances: HermesInstance[] = [];

  private constructor(testId: string, tempDir: string) {
    const uniqueId = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
    this.queueName = `test_kv_${testId}_${uniqueId}`;
    this.secondaryQueueName = `${this.queueName}_secondary`;
    this.tempDir = tempDir;
    this.sinkPath = `${tempDir}/sink.ndjson`;
    this.kvPath = `${tempDir}/queue.kv`;
    Deno.env.set("HERMES_INTEGRATION_QUEUE", this.queueName);
    Deno.env.set(
      "HERMES_INTEGRATION_SECONDARY_QUEUE",
      this.secondaryQueueName,
    );
    Deno.env.set("HERMES_INTEGRATION_SINK", this.sinkPath);
  }

  static async create(testId: string): Promise<KvScope> {
    return new KvScope(testId, await Deno.makeTempDir());
  }

  backend(): BackendAdapter {
    const backend = DenoKvBackend({ path: this.kvPath });
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
    clearBackend();
    clearHooks();
    clearLoggerSink();
    try {
      await Deno.remove(this.tempDir, { recursive: true });
    } catch (error) {
      cleanupError ??= error;
    }

    if (cleanupError) throw cleanupError;
  }
}

export async function runKv(
  testId: string,
  test: (scope: KvScope) => Promise<void>,
): Promise<void> {
  const scope = await KvScope.create(testId);
  try {
    await test(scope);
  } finally {
    await scope.cleanup();
  }
}

/** Deno KV needs no external infrastructure, so these never self-skip.
 * Sanitizers are off for parity with the BullMQ harness: hung-job and
 * force-close tests intentionally leave settled-later promises behind. */
export function kvTest(name: string, fn: () => Promise<void>): void {
  Deno.test({
    name,
    sanitizeOps: false,
    sanitizeResources: false,
    fn,
  });
}
