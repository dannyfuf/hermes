import { assertEquals, assertRejects } from "@std/assert";
import type {
  BackendAdapter,
  EnqueueOptions,
  RecurringJobConfig,
  RecurringJobValidationBackend,
} from "../../backend.ts";
import type { JobPayload } from "../../types.ts";

async function withTimeout<T>(
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

async function waitForKvValue<T>(
  kv: Deno.Kv,
  key: Deno.KvKey,
  timeoutMs: number,
  message: string,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const entry = await kv.get<T>(key);
    if (entry.value !== null) return entry.value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(message);
}

/** Create an isolated DenoKvBackend using a temp directory */
async function createIsolatedBackend(): Promise<
  { backend: BackendAdapter; kvPath: string; cleanup: () => Promise<void> }
> {
  const tempDir = await Deno.makeTempDir();
  const kvPath = `${tempDir}/test.kv`;

  const { DenoKvBackend } = await import("../../backends/deno_kv.ts");
  const backend = DenoKvBackend({ path: kvPath });

  const cleanup = async () => {
    await backend.close();
    try {
      await Deno.remove(tempDir, { recursive: true });
    } catch { /* best effort */ }
  };

  return { backend, kvPath, cleanup };
}

Deno.test({
  name: "DenoKvBackend: enqueue and listen round-trip",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      const receivedPayloads: JobPayload[] = [];

      await backend.listen(async (payload: JobPayload) => {
        receivedPayloads.push(payload);
        await Promise.resolve();
      });

      const payload: JobPayload = {
        jobName: "test_kv_job",
        queueName: "default",
        jobBody: { key: "value" },
      };

      await backend.enqueue(payload);

      // Give Deno KV time to deliver the message
      await new Promise((resolve) => setTimeout(resolve, 1000));

      assertEquals(receivedPayloads.length, 1);
      assertEquals(receivedPayloads[0].jobName, "test_kv_job");
      assertEquals(receivedPayloads[0].queueName, "default");
      assertEquals(receivedPayloads[0].jobBody, { key: "value" });
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: close() cleans up the KV handle",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      // Trigger KV initialization by enqueueing
      await backend.enqueue({
        jobName: "cleanup_test",
        queueName: "default",
        jobBody: null,
      });

      // Should not throw
      await backend.close();

      // Calling close() again should also not throw (idempotent)
      await backend.close();
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: graceful close waits for an in-flight handler",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    let resolveStarted: () => void = () => {};
    let releaseHandler: () => void = () => {};
    const started = new Promise<void>((resolve) => resolveStarted = resolve);
    const release = new Promise<void>((resolve) => releaseHandler = resolve);

    try {
      await backend.listen(async () => {
        resolveStarted();
        await release;
      });
      await backend.enqueue({
        jobName: "graceful_close_job",
        queueName: "default",
        jobBody: null,
      });
      await withTimeout(started, 2_000, "KV handler did not start");

      let closeSettled = false;
      const closePromise = backend.close().then(() => {
        closeSettled = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 25));
      assertEquals(closeSettled, false);

      releaseHandler();
      await withTimeout(
        closePromise,
        2_000,
        "Graceful KV close did not finish after handler release",
      );
      assertEquals(closeSettled, true);
    } finally {
      releaseHandler();
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: force close does not wait for an in-flight handler",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    let resolveStarted: () => void = () => {};
    let releaseHandler: () => void = () => {};
    const started = new Promise<void>((resolve) => resolveStarted = resolve);
    const release = new Promise<void>((resolve) => releaseHandler = resolve);

    try {
      await backend.listen(async () => {
        resolveStarted();
        await release;
      });
      await backend.enqueue({
        jobName: "force_close_job",
        queueName: "default",
        jobBody: null,
      });
      await withTimeout(started, 2_000, "KV handler did not start");

      await withTimeout(
        backend.close({ force: true }),
        250,
        "Force KV close waited for the active handler",
      );
    } finally {
      releaseHandler();
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: enqueue supports delay option",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      const receivedPayloads: JobPayload[] = [];

      await backend.listen(async (payload: JobPayload) => {
        receivedPayloads.push(payload);
        await Promise.resolve();
      });

      const payload: JobPayload = {
        jobName: "delayed_job",
        queueName: "default",
        jobBody: { delayed: true },
      };

      const options: EnqueueOptions = { delay: 500 };
      await backend.enqueue(payload, options);

      // Message should NOT be received immediately (within 200ms)
      await new Promise((resolve) => setTimeout(resolve, 200));
      assertEquals(receivedPayloads.length, 0);

      // Wait enough time for the delay to pass and message to be delivered
      await new Promise((resolve) => setTimeout(resolve, 1500));
      assertEquals(receivedPayloads.length, 1);
      assertEquals(receivedPayloads[0].jobBody, { delayed: true });
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: listen dispatches jobs from configured queues",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      const receivedPayloads: JobPayload[] = [];

      await backend.listen(async (payload: JobPayload) => {
        receivedPayloads.push(payload);
        await Promise.resolve();
      }, { queueNames: ["custom"] });

      await backend.enqueue({
        jobName: "any_job",
        queueName: "custom",
        jobBody: null,
      });

      await new Promise((resolve) => setTimeout(resolve, 1000));
      assertEquals(receivedPayloads.length, 1);
      assertEquals(receivedPayloads[0].queueName, "custom");
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: listen with empty queueNames accepts every queue",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      const receivedPayloads: JobPayload[] = [];

      await backend.listen(async (payload: JobPayload) => {
        receivedPayloads.push(payload);
        await Promise.resolve();
      }, { queueNames: [] });

      await backend.enqueue({
        jobName: "unfiltered_job",
        queueName: "any_queue",
        jobBody: null,
      });

      await withTimeout(
        (async () => {
          while (receivedPayloads.length === 0) {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
        })(),
        2_000,
        "KV handler did not receive an unfiltered job",
      );
      assertEquals(receivedPayloads.length, 1);
      assertEquals(receivedPayloads[0].queueName, "any_queue");
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name:
    "DenoKvBackend: filtered jobs are logged, retried, and become undelivered",
  async fn() {
    const { backend, kvPath, cleanup } = await createIsolatedBackend();
    const producerKv = await Deno.openKv(kvPath);
    const undeliveredKey: Deno.KvKey = [
      "undelivered",
      crypto.randomUUID(),
    ];
    const payload: JobPayload = {
      jobName: "filtered_job",
      queueName: "unowned_queue",
      jobBody: null,
    };
    const logLines: string[] = [];
    const originalConsoleLog = console.log;
    let handlerCalls = 0;

    try {
      console.log = (...data: unknown[]) => {
        logLines.push(data.map(String).join(" "));
      };
      await backend.listen(async () => {
        handlerCalls += 1;
        await Promise.resolve();
      }, { queueNames: ["owned_queue"] });

      await producerKv.enqueue(payload, {
        backoffSchedule: [10],
        keysIfUndelivered: [undeliveredKey],
      });

      const undeliveredPayload = await waitForKvValue<JobPayload>(
        producerKv,
        undeliveredKey,
        2_000,
        "Filtered job did not reach Deno KV undelivered handling",
      );
      assertEquals(undeliveredPayload, payload);
      assertEquals(handlerCalls, 0);

      const skippedEvents = logLines.flatMap((line) => {
        try {
          return [JSON.parse(line) as Record<string, unknown>];
        } catch {
          return [];
        }
      }).filter((event) => event.event === "job_skipped");
      assertEquals(skippedEvents.length >= 1, true);
      assertEquals(skippedEvents[0].jobName, payload.jobName);
      assertEquals(skippedEvents[0].queueName, payload.queueName);
      assertEquals(skippedEvents[0].reason, "queue filtering");
    } finally {
      console.log = originalConsoleLog;
      producerKv.close();
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: registers recurrence with a cron-safe name",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    const jobName = `namespace:job/${crypto.randomUUID().slice(0, 8)}`;

    try {
      await backend.registerRecurringJob!({
        jobName,
        queueName: "default",
        every: "1m",
      });
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: colliding sanitized prefixes get distinct cron names",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    const suffix = crypto.randomUUID().slice(0, 8);
    const configs: RecurringJobConfig[] = [
      {
        jobName: `namespace-${suffix}:a:b`,
        queueName: "default",
        every: "1m",
      },
      {
        jobName: `namespace-${suffix}:a/b`,
        queueName: "default",
        every: "1m",
      },
    ];
    const validationBackend = backend as RecurringJobValidationBackend;

    try {
      await validationBackend.validateRecurringJobs(configs);
      await backend.registerRecurringJob!(configs[0]);
      await backend.registerRecurringJob!(configs[1]);
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: caps cron names derived from long raw job names",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    const config: RecurringJobConfig = {
      jobName: `long-${crypto.randomUUID()}-${"x".repeat(100)}`,
      queueName: "default",
      every: "1m",
    };
    const validationBackend = backend as RecurringJobValidationBackend;

    try {
      await validationBackend.validateRecurringJobs([config]);
      await backend.registerRecurringJob!(config);
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: rejects duplicate derived cron names in pre-validation",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();
    const commonPrefix = "x".repeat(60);
    const configs: RecurringJobConfig[] = [
      {
        jobName: `${commonPrefix}23348`,
        queueName: "default",
        every: "1m",
      },
      {
        jobName: `${commonPrefix}251842`,
        queueName: "default",
        every: "1m",
      },
    ];
    const validationBackend = backend as RecurringJobValidationBackend;

    try {
      await assertRejects(
        () => validationBackend.validateRecurringJobs(configs),
        Error,
        "Duplicate derived Deno cron name",
      );
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: registerRecurringJob throws for unsupported interval",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      await assertRejects(
        () =>
          backend.registerRecurringJob!({
            jobName: "seconds_job",
            queueName: "default",
            every: "90s",
          }),
        Error,
        "Deno KV recurrence supports",
      );
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: registerRecurringJob throws without every or cron",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      await assertRejects(
        () =>
          backend.registerRecurringJob!({
            jobName: "no_schedule_job",
            queueName: "default",
          }),
        Error,
        "Recurring job must have either 'every' or 'cron'",
      );
    } finally {
      await cleanup();
    }
  },
});
