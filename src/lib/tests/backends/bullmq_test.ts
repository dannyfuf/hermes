// BullMQ integration tests - require a running Redis instance on localhost:6379
// These tests are skipped if Redis is not available.

import { assert, assertEquals, assertRejects } from "@std/assert";
import { Job } from "../../job.ts";
import { resolveJobTimeouts, Worker as HermesWorker } from "../../worker.ts";
import type { JobContext, JobPayload } from "../../types.ts";

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
  name: "BullMQBackend: rejects invalid concurrency before creating workers",
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    await assertRejects(
      () => backend.listen(() => Promise.resolve(), { concurrency: 0 }),
      Error,
      "Invalid BullMQ concurrency",
    );
    await backend.close({ force: true });
  },
});

Deno.test({
  name: "BullMQBackend: all connection-creating operations reject after close",
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    const backendState = backend as typeof backend & {
      queues: Map<string, unknown>;
      workers: Map<string, unknown>;
    };

    await backend.close({ force: true });

    await assertRejects(
      () =>
        backend.enqueue({
          jobName: "closed_enqueue",
          queueName: "closed_queue",
          jobBody: null,
        }),
      Error,
      "BullMQ backend is closed.",
    );
    await assertRejects(
      () =>
        backend.registerRecurringJob!({
          jobName: "closed_recurring",
          queueName: "closed_queue",
          every: "1m",
        }),
      Error,
      "BullMQ backend is closed.",
    );
    await assertRejects(
      () => backend.getQueueStats!("closed_queue"),
      Error,
      "BullMQ backend is closed.",
    );
    await assertRejects(
      () => backend.listen(() => Promise.resolve()),
      Error,
      "BullMQ backend is closed.",
    );

    assertEquals(backendState.queues.size, 0);
    assertEquals(backendState.workers.size, 0);
  },
});

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

    await assertRejects(
      () => backend.getQueueStats!(queueName),
      Error,
      "backend is closed",
    );
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
          () =>
            reject(new Error("Timeout waiting for recurring job executions")),
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
    const { Queue } = await import("bullmq");
    const queueName = `test_bullmq_${testRunId}_recurring_cron`;

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });

    // Register a cron-based recurring job (should not throw)
    await backend.registerRecurringJob!({
      jobName: "recurring_cron_test",
      queueName,
      cron: "0 * * * *",
      priority: 3,
    });

    // Upsert again should also not throw (idempotent)
    await backend.registerRecurringJob!({
      jobName: "recurring_cron_test",
      queueName,
      cron: "0 * * * *",
      priority: 3,
    });

    const inspector = new Queue(queueName, {
      connection: { host: "localhost", port: 6379 },
    });
    const scheduler = await inspector.getJobScheduler(
      "hermes:recurring_cron_test",
    );
    assertEquals(scheduler?.template?.opts?.priority, 3);
    await inspector.close();

    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: timed-out job fails and frees the worker slot",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_timeout`;
    let resolveSecond: () => void;
    const secondProcessed = new Promise<void>((resolve) =>
      resolveSecond = resolve
    );

    class TimeoutJob extends Job {
      readonly jobName = "timeout_job";
      readonly queueName = queueName;
      override readonly timeout = 50;

      perform(jobBody: unknown, _context?: JobContext): Promise<unknown> {
        if ((jobBody as { hang?: boolean }).hang) {
          return new Promise(() => {});
        }
        resolveSecond();
        return Promise.resolve();
      }
    }

    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    // deno-lint-ignore no-explicit-any
    const jobsMap = new Map<string, any>([["timeout_job", TimeoutJob]]);
    await HermesWorker.start({
      jobsMap,
      backend,
      queueNames: [queueName],
      timeoutByJobName: resolveJobTimeouts(jobsMap),
    });

    await backend.enqueue({
      jobName: "timeout_job",
      queueName,
      jobBody: { hang: true },
    });
    await backend.enqueue({
      jobName: "timeout_job",
      queueName,
      jobBody: { hang: false },
    });

    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        secondProcessed,
        new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () => reject(new Error("Timeout waiting for the next job")),
            10_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
    }
    await waitFor(
      async () => (await backend.getQueueStats!(queueName)).counts.failed === 1,
      "Timed-out job did not reach the failed state",
    );

    const stats = await backend.getQueueStats!(queueName);
    assertEquals(stats.counts.failed, 1);
    assertEquals(stats.counts.completed, 1);
    await backend.close();
  },
});

Deno.test({
  name:
    "BullMQBackend: force close releases a graceful close waiting on a hung job",
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
        Promise.all([
          backend.close({ force: true }),
          backend.close({ force: true }),
          gracefulClose,
        ]),
        new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () =>
              reject(
                new Error(
                  "Force and graceful closes did not return promptly",
                ),
              ),
            1_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
    }
  },
});

Deno.test({
  name: "BullMQBackend: reports queue counts and oldest active job age",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_stats`;
    let resolveStarted: () => void;
    let releaseJob: () => void = () => {};
    const started = new Promise<void>((resolve) => resolveStarted = resolve);
    const release = new Promise<void>((resolve) => releaseJob = resolve);
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    await backend.listen(async () => {
      resolveStarted();
      await release;
    }, { queueNames: [queueName] });
    await backend.enqueue({
      jobName: "stats_job",
      queueName,
      jobBody: null,
    });
    await started;

    const activeStats = await backend.getQueueStats!(queueName);
    assertEquals(activeStats.counts.active, 1);
    assertEquals(typeof activeStats.oldestActiveJobAgeMs, "number");
    assert(activeStats.oldestActiveJobAgeMs! >= 0);

    releaseJob();
    await waitFor(
      async () =>
        (await backend.getQueueStats!(queueName)).counts.completed === 1,
      "Stats job did not complete",
    );
    const completedStats = await backend.getQueueStats!(queueName);
    assertEquals(completedStats.counts.completed, 1);
    assertEquals(completedStats.counts.prioritized, 0);
    assertEquals(completedStats.oldestActiveJobAgeMs, undefined);
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: includes prioritized jobs in waiting backlog stats",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_prioritized_stats`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    await backend.enqueue({
      jobName: "ordinary_waiting_job",
      queueName,
      jobBody: null,
    });
    await backend.enqueue({
      jobName: "prioritized_waiting_job",
      queueName,
      jobBody: null,
    }, { priority: 1 });

    const stats = await backend.getQueueStats!(queueName);
    assertEquals(stats.counts.waiting, 2);
    assertEquals(stats.counts.prioritized, 1);
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: lower priority values run first",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const queueName = `test_bullmq_${testRunId}_priority`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    await backend.enqueue({
      jobName: "low_priority",
      queueName,
      jobBody: null,
    }, { priority: 10 });
    await backend.enqueue({
      jobName: "high_priority",
      queueName,
      jobBody: null,
    }, { priority: 1 });

    const order: string[] = [];
    let resolveProcessed: () => void;
    const processed = new Promise<void>((resolve) =>
      resolveProcessed = resolve
    );
    await backend.listen(async (payload) => {
      order.push(payload.jobName);
      if (order.length === 2) resolveProcessed();
      await Promise.resolve();
    }, { queueNames: [queueName], concurrency: 1 });
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        processed,
        new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () => reject(new Error("Priority jobs were not processed")),
            10_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(timeoutId);
    }

    assertEquals(order, ["high_priority", "low_priority"]);
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: defaultJobOptions retries then fails a job",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const { Queue } = await import("bullmq");
    const queueName = `test_bullmq_${testRunId}_retries`;
    let attemptsSeen = 0;
    const logs: Record<string, unknown>[] = [];
    const originalLog = console.log;
    console.log = (message: string) => {
      try {
        logs.push(JSON.parse(message));
      } catch { /* ignore non-JSON */ }
    };
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: "fixed", delay: 25 },
      },
    });

    try {
      await backend.listen((_payload) => {
        attemptsSeen += 1;
        return Promise.reject(new Error("retry me"));
      }, { queueNames: [queueName] });
      await backend.enqueue({
        jobName: "retry_job",
        queueName,
        jobBody: null,
      });

      await waitFor(
        async () =>
          (await backend.getQueueStats!(queueName)).counts.failed === 1,
        "Retried job did not reach the failed state",
      );
      const inspector = new Queue(queueName, {
        connection: { host: "localhost", port: 6379 },
      });
      const [failedJob] = await inspector.getFailed(0, 0);
      await inspector.close();

      assertEquals(attemptsSeen, 3);
      assertEquals(failedJob.attemptsMade, 3);
      assert(
        logs.some((entry) =>
          entry.event === "worker_job_failed" && entry.attemptsMade === 3
        ),
      );
    } finally {
      await backend.close();
      console.log = originalLog;
    }
  },
});

Deno.test({
  name:
    "BullMQBackend: applies bounded retention defaults to jobs and schedulers",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const { Queue } = await import("bullmq");
    const queueName = `test_bullmq_${testRunId}_retention`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
    });
    await backend.enqueue({
      jobName: "retention_job",
      queueName,
      jobBody: null,
    });
    await backend.registerRecurringJob!({
      jobName: "retention_scheduler_job",
      queueName,
      every: "1h",
    });

    const inspector = new Queue(queueName, {
      connection: { host: "localhost", port: 6379 },
    });
    const job = await inspector.getJob("1");
    assertEquals(job?.opts.removeOnComplete, { count: 1000 });
    assertEquals(job?.opts.removeOnFail, { count: 5000 });

    const schedulers = await inspector.getJobSchedulers();
    const scheduler = schedulers.find((entry) =>
      entry.key === "hermes:retention_scheduler_job"
    );
    assertEquals(scheduler?.template?.opts?.removeOnComplete, { count: 1000 });
    assertEquals(scheduler?.template?.opts?.removeOnFail, { count: 5000 });
    await inspector.close();
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: explicit undefined keeps bounded retention defaults",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const { Queue } = await import("bullmq");
    const queueName = `test_bullmq_${testRunId}_undefined_retention`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
      defaultJobOptions: {
        removeOnComplete: undefined,
        removeOnFail: undefined,
      },
    });
    await backend.enqueue({
      jobName: "undefined_retention_job",
      queueName,
      jobBody: null,
    });
    await backend.registerRecurringJob!({
      jobName: "undefined_retention_scheduler",
      queueName,
      every: "1h",
    });

    const inspector = new Queue(queueName, {
      connection: { host: "localhost", port: 6379 },
    });
    const job = await inspector.getJob("1");
    const scheduler = await inspector.getJobScheduler(
      "hermes:undefined_retention_scheduler",
    );
    assertEquals(job?.opts.removeOnComplete, { count: 1000 });
    assertEquals(job?.opts.removeOnFail, { count: 5000 });
    assertEquals(scheduler?.template?.opts?.removeOnComplete, { count: 1000 });
    assertEquals(scheduler?.template?.opts?.removeOnFail, { count: 5000 });
    await inspector.close();
    await backend.close();
  },
});

Deno.test({
  name: "BullMQBackend: false and zero override retention defaults",
  ignore: !redisAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const { BullMQBackend } = await import("../../backends/bullmq.ts");
    const { Queue } = await import("bullmq");
    const queueName = `test_bullmq_${testRunId}_explicit_retention`;
    const backend = BullMQBackend({
      connection: { host: "localhost", port: 6379 },
      defaultJobOptions: {
        removeOnComplete: false,
        removeOnFail: 0,
      },
    });
    await backend.enqueue({
      jobName: "explicit_retention_job",
      queueName,
      jobBody: null,
    });
    await backend.registerRecurringJob!({
      jobName: "explicit_retention_scheduler",
      queueName,
      every: "1h",
    });

    const inspector = new Queue(queueName, {
      connection: { host: "localhost", port: 6379 },
    });
    const job = await inspector.getJob("1");
    const scheduler = await inspector.getJobScheduler(
      "hermes:explicit_retention_scheduler",
    );
    assertEquals(job?.opts.removeOnComplete, false);
    assertEquals(job?.opts.removeOnFail, 0);
    assertEquals(scheduler?.template?.opts?.removeOnComplete, false);
    assertEquals(scheduler?.template?.opts?.removeOnFail, 0);
    await inspector.close();
    await backend.close();
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
