import { assertEquals, assertRejects } from "@std/assert";
import { clearBackend } from "../backend_registry.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import type { BackendAdapter } from "../backend.ts";

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

  await t.step("Hermes instance has start and stop methods", async () => {
    const { Hermes } = await import("../hermes.ts");
    const backend = new MockBackend();

    const hermes = Hermes({
      manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
      backend,
    });

    assertEquals(typeof hermes.start, "function");
    assertEquals(typeof hermes.stop, "function");
    clearBackend();
  });

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
    "start() loads manifest, builds job registry, and starts worker",
    async () => {
      clearBackend();
      const { Hermes } = await import("../hermes.ts");
      const backend = new MockBackend();

      const hermes = Hermes({
        manifest: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
        backend,
      });

      await hermes.start();

      // After start(), the backend should have listen() called (worker started)
      assertEquals(backend.isListening, true);

      // Queue names should have been extracted from the registered jobs
      // valid_manifest.ts exports TestJob (default) and CustomQueueJob (priority)
      const queueNames = backend.listenOptions?.queueNames ?? [];
      assertEquals(queueNames.includes("default"), true);
      assertEquals(queueNames.includes("priority"), true);

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
