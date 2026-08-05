import { assert, assertEquals, assertRejects } from "@std/assert";
import { resolveJobTimeouts, Worker } from "../worker.ts";
import { Job } from "../job.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import {
  clearPerformCalls,
  FailingJob,
  performCalls,
  TestJob,
} from "./helpers/test_jobs.ts";
import type { JobContext, JobPayload } from "../types.ts";

let observedSignal: AbortSignal | undefined;

function observedSignalAborted(): boolean | undefined {
  return observedSignal?.aborted;
}

class HangingJob extends Job {
  readonly jobName = "hanging_job";
  readonly queueName = "default";
  override readonly timeout = 15;

  perform(_jobBody: unknown, context?: JobContext): Promise<unknown> {
    observedSignal = context?.signal;
    return new Promise(() => {});
  }
}

class DefaultTimeoutJob extends Job {
  readonly jobName = "default_timeout_job";
  readonly queueName = "default";

  perform(_jobBody: unknown, context?: JobContext): Promise<unknown> {
    observedSignal = context?.signal;
    return new Promise(() => {});
  }
}

class SuccessfulTimedJob extends Job {
  readonly jobName = "successful_timed_job";
  readonly queueName = "default";
  override readonly timeout = 10;

  async perform(
    _jobBody: unknown,
    context?: JobContext,
  ): Promise<unknown> {
    observedSignal = context?.signal;
    return await Promise.resolve("done");
  }
}

class InvalidTimeoutJob extends Job {
  readonly jobName = "invalid_timeout_job";
  readonly queueName = "default";
  override readonly timeout = "soon";

  async perform(_jobBody: unknown): Promise<unknown> {
    return await Promise.resolve();
  }
}

class SynchronousThrowingTimedJob extends Job {
  readonly jobName = "synchronous_throwing_timed_job";
  readonly queueName = "default";
  override readonly timeout = 60_000;

  perform(_jobBody: unknown): Promise<unknown> {
    throw new Error("synchronous timed failure");
  }
}

class AsyncRejectingTimedJob extends Job {
  readonly jobName = "async_rejecting_timed_job";
  readonly queueName = "default";
  override readonly timeout = 60_000;

  async perform(_jobBody: unknown): Promise<unknown> {
    await Promise.resolve();
    throw new Error("asynchronous timed failure");
  }
}

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

      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

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

    await Worker.start({
      jobsMap,
      backend,
      timeoutByJobName: resolveJobTimeouts(jobsMap),
    });

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

      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

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
      timeoutByJobName: resolveJobTimeouts(jobsMap),
    });

    assertEquals(backend.listenOptions?.queueNames, ["default", "priority"]);
  });

  await t.step(
    "rejects timed-out jobs with JobTimeoutError and aborts the signal",
    async () => {
      observedSignal = undefined;
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([["hanging_job", HangingJob]]);
      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      const error = await assertRejects(() =>
        backend.process({
          jobName: "hanging_job",
          queueName: "default",
          jobBody: null,
        })
      );

      assert(error instanceof Error);
      assertEquals(error.name, "JobTimeoutError");
      assert(error.message.includes("hanging_job"));
      assert(error.message.includes("15"));
      assertEquals(observedSignalAborted(), true);
    },
  );

  await t.step("applies defaultJobTimeout when the job has none", async () => {
    observedSignal = undefined;
    const backend = new MockBackend();
    // deno-lint-ignore no-explicit-any
    const jobsMap = new Map<string, any>([[
      "default_timeout_job",
      DefaultTimeoutJob,
    ]]);
    await Worker.start({
      jobsMap,
      backend,
      timeoutByJobName: resolveJobTimeouts(jobsMap, "1s"),
    });

    const error = await assertRejects(() =>
      backend.process({
        jobName: "default_timeout_job",
        queueName: "default",
        jobBody: null,
      })
    );
    assert(error instanceof Error);
    assertEquals(error.name, "JobTimeoutError");
    assertEquals(observedSignalAborted(), true);
  });

  await t.step("clears the timeout after a successful job", async () => {
    observedSignal = undefined;
    const backend = new MockBackend();
    // deno-lint-ignore no-explicit-any
    const jobsMap = new Map<string, any>([[
      "successful_timed_job",
      SuccessfulTimedJob,
    ]]);
    await Worker.start({
      jobsMap,
      backend,
      timeoutByJobName: resolveJobTimeouts(jobsMap),
    });

    await backend.process({
      jobName: "successful_timed_job",
      queueName: "default",
      jobBody: null,
    });
    await new Promise((resolve) => setTimeout(resolve, 30));

    assertEquals(observedSignalAborted(), false);
  });

  await t.step(
    "rejects invalid timeout strings during resolution",
    async () => {
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([[
        "invalid_timeout_job",
        InvalidTimeoutJob,
      ]]);

      await assertRejects(
        async () => {
          resolveJobTimeouts(jobsMap);
          await Promise.resolve();
        },
        Error,
        'Invalid timeout for job "invalid_timeout_job"',
      );
      assertEquals(backend.isListening, false);
    },
  );

  await t.step(
    "clears the timeout when a timed job throws synchronously",
    async () => {
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([[
        "synchronous_throwing_timed_job",
        SynchronousThrowingTimedJob,
      ]]);
      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      await assertRejects(
        () =>
          backend.process({
            jobName: "synchronous_throwing_timed_job",
            queueName: "default",
            jobBody: null,
          }),
        Error,
        "synchronous timed failure",
      );
    },
  );

  await t.step(
    "clears the timeout when a timed job rejects asynchronously",
    async () => {
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([[
        "async_rejecting_timed_job",
        AsyncRejectingTimedJob,
      ]]);
      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      await assertRejects(
        () =>
          backend.process({
            jobName: "async_rejecting_timed_job",
            queueName: "default",
            jobBody: null,
          }),
        Error,
        "asynchronous timed failure",
      );
    },
  );

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
        await Worker.start({
          jobsMap,
          backend,
          timeoutByJobName: resolveJobTimeouts(jobsMap),
        });

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
        await Worker.start({
          jobsMap,
          backend,
          timeoutByJobName: resolveJobTimeouts(jobsMap),
        });

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
