// BullMQ integration tests - require a running Redis instance on localhost:6379
// These tests are skipped if Redis is not available.

import { assertEquals } from "@std/assert";
import type { JobPayload } from "../../types.ts";

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

async function checkRedis(): Promise<boolean> {
  try {
    const conn = await Deno.connect({ hostname: "127.0.0.1", port: 6379 });
    conn.close();
    return true;
  } catch {
    return false;
  }
}

const redisAvailable = await checkRedis();

// Use unique queue names per test run to avoid cross-run pollution
const testRunId = crypto.randomUUID().slice(0, 8);

Deno.test({
  name: "BullMQBackend: enqueue and process a job",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_basic`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    const processed: JobPayload[] = [];

    const done = new Promise<void>((resolve) => {
      backend.listen(async (payload: JobPayload) => {
        processed.push(payload);
        resolve();
        await Promise.resolve();
      }, { queueNames: [queueName] });
    });

    await backend.enqueue({
      jobName: "bullmq_test_job",
      queueName: queueName,
      jobBody: { test: true },
    });

    // Wait for the job to be processed (with timeout)
    let timeoutId: number | undefined;
    await Promise.race([
      done,
      new Promise<void>((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Timeout waiting for job processing")),
          10000,
        ) as unknown as number;
      }),
    ]);

    clearTimeout(timeoutId);

    assertEquals(processed.length, 1);
    assertEquals(processed[0].jobName, "bullmq_test_job");
    assertEquals(processed[0].jobBody, { test: true });

    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: enqueues to correct queue based on queueName",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queue1 = `test_bullmq_${testRunId}_q1`;
    const queue2 = `test_bullmq_${testRunId}_q2`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    const processedQ1: JobPayload[] = [];
    const processedQ2: JobPayload[] = [];
    let resolveQ1: () => void;
    let resolveQ2: () => void;
    const doneQ1 = new Promise<void>((r) => resolveQ1 = r);
    const doneQ2 = new Promise<void>((r) => resolveQ2 = r);

    // Listen on both queues with a single backend but track separately
    // We need to use listen() which creates workers for both queues
    backend.listen(async (payload: JobPayload) => {
      if (payload.queueName === queue1) {
        processedQ1.push(payload);
        resolveQ1();
      } else if (payload.queueName === queue2) {
        processedQ2.push(payload);
        resolveQ2();
      }
      await Promise.resolve();
    }, { queueNames: [queue1, queue2] });

    // Enqueue one job to each queue
    await backend.enqueue({
      jobName: "job_for_q1",
      queueName: queue1,
      jobBody: { queue: 1 },
    });

    await backend.enqueue({
      jobName: "job_for_q2",
      queueName: queue2,
      jobBody: { queue: 2 },
    });

    // Wait for both to be processed
    let timeoutId: number | undefined;
    await Promise.race([
      Promise.all([doneQ1, doneQ2]),
      new Promise<void>((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Timeout waiting for multi-queue processing")),
          10000,
        ) as unknown as number;
      }),
    ]);
    clearTimeout(timeoutId);

    assertEquals(processedQ1.length, 1);
    assertEquals(processedQ1[0].jobName, "job_for_q1");
    assertEquals(processedQ2.length, 1);
    assertEquals(processedQ2[0].jobName, "job_for_q2");

    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: close() shuts down queues and workers",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_close`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    // Initialize a queue by enqueueing
    await backend.enqueue({
      jobName: "close_test",
      queueName: queueName,
      jobBody: null,
    });

    // Start a worker
    await backend.listen(async (_payload: JobPayload) => {
      await Promise.resolve();
    }, { queueNames: [queueName] });

    // close() should shut down both queues and workers without throwing
    await backend.close();

    // Calling close() again should be safe (idempotent)
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: replaces an existing worker without leaking it",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_duplicate_listen`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    let firstHandlerCalls = 0;
    let secondHandlerCalls = 0;

    await backend.listen(async () => {
      firstHandlerCalls += 1;
      await Promise.resolve();
    }, { queueNames: [queueName] });
    await backend.listen(async () => {
      secondHandlerCalls += 1;
      await Promise.resolve();
    }, { queueNames: [queueName] });
    await backend.enqueue({
      jobName: "duplicate_listen_job",
      queueName,
      jobBody: null,
    });

    await waitFor(
      () => secondHandlerCalls === 1,
      "Replacement worker did not process the job",
    );
    assertEquals(firstHandlerCalls, 0);
    assertEquals(secondHandlerCalls, 1);
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: registers and executes recurring job with every",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_recurring_every`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    const executions: JobPayload[] = [];
    let resolveTwo: () => void;
    const twoExecutions = new Promise<void>((r) => resolveTwo = r);

    await backend.listen(async (payload: JobPayload) => {
      executions.push(payload);
      if (executions.length >= 2) resolveTwo();
      await Promise.resolve();
    }, { queueNames: [queueName] });

    await backend.registerRecurringJob!({
      jobName: "recurring_every_test",
      queueName,
      every: "1s",
      jobBody: { test: true },
    });

    let timeoutId: number | undefined;
    await Promise.race([
      twoExecutions,
      new Promise<void>((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Timeout waiting for recurring job executions")),
          10000,
        ) as unknown as number;
      }),
    ]);
    clearTimeout(timeoutId);

    assertEquals(executions.length >= 2, true);
    assertEquals(executions[0].jobName, "recurring_every_test");

    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: registers recurring job with cron expression",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_recurring_cron`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    // Register a cron-based recurring job (should not throw)
    await backend.registerRecurringJob!({
      jobName: "recurring_cron_test",
      queueName,
      cron: "0 * * * *",
    });

    // Upsert again should also not throw (idempotent)
    await backend.registerRecurringJob!({
      jobName: "recurring_cron_test",
      queueName,
      cron: "0 * * * *",
    });

    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: force close returns with a hung job in flight",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_force_close`;
    let resolveStarted: () => void;
    const started = new Promise<void>((resolve) => resolveStarted = resolve);
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    await backend.listen(async () => {
      resolveStarted();
      await new Promise(() => {});
    }, { queueNames: [queueName] });
    await backend.enqueue({
      jobName: "hung_close_job",
      queueName,
      jobBody: null,
    });
    await started;

    const gracefulClose = backend.close();
    await new Promise((resolve) => setTimeout(resolve, 25));

    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        backend.close({ force: true }),
        new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () => reject(new Error("Force close did not return promptly")),
            1_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
    }
    void gracefulClose;
  },
});

Deno.test({
  name: "BullMQBackend: uses defaultQueueName when payload has no queueName",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const defaultQueue = `test_bullmq_${testRunId}_default`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
      defaultQueueName: defaultQueue,
    });

    const processed: JobPayload[] = [];
    const done = new Promise<void>((resolve) => {
      backend.listen(async (payload: JobPayload) => {
        processed.push(payload);
        resolve();
        await Promise.resolve();
      }, { queueNames: [defaultQueue] });
    });

    // Enqueue with empty queueName — should route to defaultQueueName
    await backend.enqueue({
      jobName: "default_queue_job",
      queueName: "",
      jobBody: { default: true },
    });

    let timeoutId: number | undefined;
    await Promise.race([
      done,
      new Promise<void>((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Timeout")),
          10000,
        ) as unknown as number;
      }),
    ]);
    clearTimeout(timeoutId);

    assertEquals(processed.length, 1);
    assertEquals(processed[0].jobName, "default_queue_job");

    await backend.close();
  },
});
