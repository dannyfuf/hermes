import { assertEquals, assertRejects } from "@std/assert";
import { Worker } from "../worker.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import {
  clearPerformCalls,
  FailingJob,
  performCalls,
  TestJob,
} from "./helpers/test_jobs.ts";
import type { JobPayload } from "../types.ts";

Deno.test("Worker", async (t) => {
  await t.step(
    "dispatches to correct job class based on jobName",
    async () => {
      clearPerformCalls();
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([
        ["test_job", TestJob],
      ]);

      await Worker.start({ jobsMap, backend });

      const payload: JobPayload = {
        jobName: "test_job",
        queueName: "default",
        jobBody: { data: "hello" },
      };

      await backend.process(payload);

      // Verify perform() was actually called with the correct jobBody
      assertEquals(performCalls.length, 1);
      assertEquals(performCalls[0].jobName, "test_job");
      assertEquals(performCalls[0].jobBody, { data: "hello" });
    },
  );

  await t.step("logs unknown job and skips (does not throw)", async () => {
    const backend = new MockBackend();
    // deno-lint-ignore no-explicit-any
    const jobsMap = new Map<string, any>();

    await Worker.start({ jobsMap, backend });

    const payload: JobPayload = {
      jobName: "nonexistent_job",
      queueName: "default",
      jobBody: {},
    };

    // Should not throw for unknown jobs
    await backend.process(payload);
  });

  await t.step(
    "catches and re-throws errors from perform()",
    async () => {
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([
        ["failing_job", FailingJob],
      ]);

      await Worker.start({ jobsMap, backend });

      const payload: JobPayload = {
        jobName: "failing_job",
        queueName: "default",
        jobBody: {},
      };

      await assertRejects(
        () => backend.process(payload),
        Error,
        "Job failed intentionally",
      );
    },
  );

  await t.step("passes queueNames to backend.listen()", async () => {
    const backend = new MockBackend();

    // deno-lint-ignore no-explicit-any
    const jobsMap = new Map<string, any>();
    await Worker.start({
      jobsMap,
      backend,
      queueNames: ["default", "priority"],
    });

    assertEquals(backend.listenOptions?.queueNames, ["default", "priority"]);
  });

  await t.step(
    "measures duration and logs for succeeded jobs",
    async () => {
      clearPerformCalls();
      const backend = new MockBackend();

      // Capture Logger output by intercepting console.log
      const logEntries: Record<string, unknown>[] = [];
      const originalLog = console.log;
      console.log = (msg: string) => {
        try {
          logEntries.push(JSON.parse(msg));
        } catch { /* ignore non-JSON */ }
      };

      try {
        // deno-lint-ignore no-explicit-any
        const jobsMap = new Map<string, any>([
          ["test_job", TestJob],
        ]);
        await Worker.start({ jobsMap, backend });

        const payload: JobPayload = {
          jobName: "test_job",
          queueName: "default",
          jobBody: { data: "timing" },
        };

        await backend.process(payload);

        const succeededLog = logEntries.find((e) =>
          e.event === "job_succeeded"
        );
        assertEquals(succeededLog !== undefined, true);
        assertEquals(succeededLog!.jobName, "test_job");
        assertEquals(succeededLog!.queueName, "default");
        assertEquals(typeof succeededLog!.durationMs, "number");
        assertEquals((succeededLog!.durationMs as number) >= 0, true);
      } finally {
        console.log = originalLog;
      }
    },
  );

  await t.step(
    "measures duration and logs for failed jobs",
    async () => {
      const backend = new MockBackend();

      const logEntries: Record<string, unknown>[] = [];
      const originalLog = console.log;
      console.log = (msg: string) => {
        try {
          logEntries.push(JSON.parse(msg));
        } catch { /* ignore non-JSON */ }
      };

      try {
        // deno-lint-ignore no-explicit-any
        const jobsMap = new Map<string, any>([
          ["failing_job", FailingJob],
        ]);
        await Worker.start({ jobsMap, backend });

        const payload: JobPayload = {
          jobName: "failing_job",
          queueName: "default",
          jobBody: {},
        };

        await backend.process(payload).catch(() => {
          // expected to throw
        });

        const failedLog = logEntries.find((e) => e.event === "job_failed");
        assertEquals(failedLog !== undefined, true);
        assertEquals(failedLog!.jobName, "failing_job");
        assertEquals(failedLog!.queueName, "default");
        assertEquals(failedLog!.error, "Job failed intentionally");
        assertEquals(typeof failedLog!.durationMs, "number");
        assertEquals((failedLog!.durationMs as number) >= 0, true);
      } finally {
        console.log = originalLog;
      }
    },
  );
});
