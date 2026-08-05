import { assertEquals, assertRejects } from "@std/assert";
import { clearBackend } from "../backend_registry.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import type {
  BackendAdapter,
  QueueStats,
  RecurringJobConfig,
} from "../backend.ts";

class HangingCloseBackend extends MockBackend {
  override close(options?: { force?: boolean }): Promise<void> {
    this.closeOptions.push(options);
    if (options?.force) return Promise.resolve();
    return new Promise(() => {});
  }
}

class StatsBackend extends MockBackend {
  requestedStats: string[] = [];

  getQueueStats(queueName: string): Promise<QueueStats> {
    this.requestedStats.push(queueName);
    return Promise.resolve({
      queueName,
      counts: {
        waiting: 1,
        active: 0,
        delayed: 0,
        failed: 0,
        completed: 2,
      },
    });
  }
}

class DeferredRecurringBackend extends MockBackend {
  readonly registrationStarted: Promise<void>;
  private resolveRegistrationStarted: () => void = () => {};
  private readonly registrationRelease: Promise<void>;
  private resolveRegistrationRelease: () => void = () => {};

  constructor() {
    super();
    this.registrationStarted = new Promise((resolve) => {
      this.resolveRegistrationStarted = resolve;
    });
    this.registrationRelease = new Promise((resolve) => {
      this.resolveRegistrationRelease = resolve;
    });
  }

  override async registerRecurringJob(
    config: RecurringJobConfig,
  ): Promise<void> {
    await super.registerRecurringJob(config);
    this.resolveRegistrationStarted();
    await this.registrationRelease;
  }

  releaseRegistration(): void {
    this.resolveRegistrationRelease();
  }
}

Deno.test("Hermes", async (t) => {
  await t.step("configure() sets global backend", async () => {
    clearBackend();
    const { configure } = await import("../hermes.ts");
    const backend = new MockBackend();
    configure({ backend });

    const { getBackend } = await import("../backend_registry.ts");
    assertEquals(getBackend(), backend as BackendAdapter);
    clearBackend();
  });

  await t.step("Hermes() factory sets backend on construction", async () => {
    clearBackend();
    const { Hermes } = await import("../hermes.ts");
    const backend = new MockBackend();

    const _hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });

    const { getBackend } = await import("../backend_registry.ts");
    assertEquals(getBackend(), backend as BackendAdapter);
    clearBackend();
  });

  await t.step(
    "Hermes instance has start, stop, and stats methods",
    async () => {
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();

      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
        backend,
      });

      assertEquals(typeof hermes.start, "function");
      assertEquals(typeof hermes.stop, "function");
      assertEquals(typeof hermes.stats, "function");
      clearBackend();
    },
  );

  await t.step("stop() calls backend.close()", async () => {
    const { Hermes } = await import("../hermes.ts");
    let closeCalled = false;
    const backend = new MockBackend();
    const originalClose = backend.close.bind(backend);
    backend.close = () => {
      closeCalled = true;
      return originalClose();
    };

    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });

    await hermes.stop();
    assertEquals(closeCalled, true);
    clearBackend();
  });

  await t.step(
    "stop() force-closes after the graceful timeout and is idempotent",
    async () => {
      const { Hermes } = await import("../hermes.ts");
      const backend = new HangingCloseBackend();
      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
        backend,
        worker: { gracefulShutdownTimeout: 10 },
      });
      const logEntries: Record<string, unknown>[] = [];
      const originalLog = console.log;
      console.log = (message: string) => {
        logEntries.push(JSON.parse(message));
      };

      try {
        await Promise.all([hermes.stop(), hermes.stop()]);
      } finally {
        console.log = originalLog;
      }

      assertEquals(backend.closeOptions, [undefined, { force: true }]);
      assertEquals(logEntries.map((entry) => entry.event), [
        "worker_stopping",
        "worker_force_closed",
        "worker_stopped",
      ]);
      assertEquals(logEntries[1].gracefulShutdownTimeoutMs, 10);
      clearBackend();
    },
  );

  await t.step(
    "start() loads manifest, builds job registry, and starts worker",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();

      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
        backend,
        worker: { concurrency: 4 },
      });

      await hermes.start();

      // After start(), the backend should have listen() called (worker started)
      assertEquals(backend.isListening, true);

      // Queue names should have been extracted from the registered jobs
      // valid_manifest.ts exports TestJob (default) and CustomQueueJob (priority)
      const queueNames = backend.listenOptions?.queueNames ?? [];
      assertEquals(queueNames.includes("default"), true);
      assertEquals(queueNames.includes("priority"), true);
      assertEquals(backend.listenOptions?.concurrency, 4);

      await hermes.stop();
    },
  );

  await t.step("start() rejects an invalid default job timeout", async () => {
    clearBackend();
    const { Hermes } = await import("../hermes.ts");
    const backend = new MockBackend();
    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/recurring_manifest.ts",
      backend,
      worker: { defaultJobTimeout: "eventually" },
    });

    await assertRejects(
      () => hermes.start(),
      Error,
      'Invalid timeout for job "recurring_every_job"',
    );
    assertEquals(backend.isListening, false);
    assertEquals(backend.registeredRecurringJobs, []);
    clearBackend();
  });

  await t.step(
    "start() rejects job timeouts above the timer ceiling",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();
      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/recurring_manifest.ts",
        backend,
        worker: { defaultJobTimeout: 2_147_483_648 },
      });

      await assertRejects(
        () => hermes.start(),
        Error,
        "~24.8 days",
      );
      assertEquals(backend.isListening, false);
      assertEquals(backend.registeredRecurringJobs, []);
      clearBackend();
    },
  );

  await t.step(
    "start() rejects invalid graceful shutdown timeouts before recurrence registration",
    async () => {
      const invalidTimeouts = [0, -1, 1.5, 2_147_483_648, Number.NaN];

      for (const gracefulShutdownTimeout of invalidTimeouts) {
        clearBackend();
        const { Hermes } = await import("../hermes.ts");
        const backend = new MockBackend();
        const hermes = Hermes({
          manifest: "./src/lib/tests/helpers/fixtures/recurring_manifest.ts",
          backend,
          worker: { gracefulShutdownTimeout },
        });

        await assertRejects(
          () => hermes.start(),
          Error,
          "Invalid worker.gracefulShutdownTimeout",
        );
        assertEquals(backend.isListening, false);
        assertEquals(backend.registeredRecurringJobs, []);
      }
      clearBackend();
    },
  );

  await t.step(
    "a second concurrent or duplicate start() fails fast",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();
      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
        backend,
      });

      const firstStart = hermes.start();
      await assertRejects(
        () => hermes.start(),
        Error,
        "already started",
      );
      await firstStart;
      await assertRejects(
        () => hermes.start(),
        Error,
        "already started",
      );
      await hermes.stop();
      clearBackend();
    },
  );

  await t.step(
    "stop() during start() prevents the worker from listening",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new DeferredRecurringBackend();
      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/recurring_manifest.ts",
        backend,
      });

      const startPromise = hermes.start();
      await backend.registrationStarted;
      const stopPromise = hermes.stop();
      backend.releaseRegistration();

      await Promise.all([startPromise, stopPromise]);
      assertEquals(backend.isListening, false);
      assertEquals(backend.closeOptions, [undefined]);
      clearBackend();
    },
  );

  await t.step(
    "start() passes recurring job priority to the backend",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();
      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/recurring_manifest.ts",
        backend,
      });
      await hermes.start();

      assertEquals(backend.registeredRecurringJobs[0].priority, 4);
      await hermes.stop();
      clearBackend();
    },
  );

  await t.step("stats() reads every manifest queue after start()", async () => {
    clearBackend();
    const { Hermes } = await import("../hermes.ts");
    const backend = new StatsBackend();
    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });
    await hermes.start();

    const stats = await hermes.stats();

    assertEquals(stats.map((entry) => entry.queueName), [
      "default",
      "priority",
    ]);
    assertEquals(backend.requestedStats, ["default", "priority"]);
    await hermes.stop();
    clearBackend();
  });

  await t.step("stats() rejects unsupported backends", async () => {
    clearBackend();
    const { Hermes } = await import("../hermes.ts");
    const backend = new MockBackend();
    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });
    await hermes.start();

    await assertRejects(
      () => hermes.stats(),
      Error,
      "does not support queue stats",
    );
    await hermes.stop();
    clearBackend();
  });

  await t.step("stats() rejects after stop() completes", async () => {
    clearBackend();
    const { Hermes } = await import("../hermes.ts");
    const backend = new StatsBackend();
    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });
    await hermes.start();
    await hermes.stop();

    await assertRejects(
      () => hermes.stats(),
      Error,
      "only available after start()",
    );
    clearBackend();
  });

  await t.step(
    "configure() enables enqueue-only without starting worker",
    async () => {
      clearBackend();
      const { configure } = await import("../hermes.ts");
      const backend = new MockBackend();
      configure({ backend });

      // Import a test job and enqueue through it
      const { TestJob } = await import("./helpers/test_jobs.ts");
      const job = new TestJob();
      await job.performLater({ message: "enqueue-only" });

      assertEquals(backend.enqueued.length, 1);
      assertEquals(backend.enqueued[0].payload.jobName, "test_job");
      assertEquals(backend.enqueued[0].payload.jobBody, {
        message: "enqueue-only",
      });

      // Worker should NOT be listening (we only called configure, not start)
      assertEquals(backend.isListening, false);

      clearBackend();
    },
  );
});
