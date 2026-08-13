import { assert, assertEquals, assertRejects } from "@std/assert";
import { resolveJobTimeouts, Worker } from "../worker.ts";
import { Job } from "../job.ts";
import { clearBackend } from "../backend_registry.ts";
import { clearHooks, setHooks } from "../hooks_registry.ts";
import { Hermes } from "../hermes.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import type { HermesHooks, JobPayload } from "../types.ts";

/** Counts perform() executions so single-execution invariants are provable. */
let performRuns = 0;
/** External settlement control for jobs that hang until released. */
let releasePerform: (value?: unknown) => void = () => {};

class SucceedingJob extends Job {
  readonly jobName = "succeeding_job";
  readonly queueName = "default";

  async perform(): Promise<unknown> {
    performRuns++;
    return await Promise.resolve("done");
  }
}

class RejectingJob extends Job {
  readonly jobName = "rejecting_job";
  readonly queueName = "default";

  async perform(): Promise<unknown> {
    performRuns++;
    await Promise.resolve();
    throw new Error("perform failed");
  }
}

class ControlledJob extends Job {
  readonly jobName = "controlled_job";
  readonly queueName = "default";

  perform(): Promise<unknown> {
    performRuns++;
    return new Promise((resolve) => {
      releasePerform = resolve;
    });
  }
}

class ControlledTimedJob extends Job {
  readonly jobName = "controlled_timed_job";
  readonly queueName = "default";
  override readonly timeout = 20;

  perform(): Promise<unknown> {
    performRuns++;
    return new Promise((resolve) => {
      releasePerform = resolve;
    });
  }
}

const payloadFor = (jobName: string): JobPayload => ({
  jobName,
  queueName: "default",
  jobBody: null,
});

function captureLogs(): {
  entries: Record<string, unknown>[];
  hookErrors: () => Record<string, unknown>[];
  restore: () => void;
} {
  const entries: Record<string, unknown>[] = [];
  const originalLog = console.log;
  console.log = (msg: string) => {
    try {
      entries.push(JSON.parse(msg));
    } catch { /* ignore non-JSON */ }
  };
  return {
    entries,
    hookErrors: () => entries.filter((e) => e.event === "hook_error"),
    restore: () => {
      console.log = originalLog;
    },
  };
}

async function startWorker(
  // deno-lint-ignore no-explicit-any
  jobClasses: (new () => any)[],
  aroundPerform: HermesHooks["aroundPerform"],
): Promise<{ backend: MockBackend; worker: Worker }> {
  performRuns = 0;
  clearHooks();
  if (aroundPerform) setHooks({ aroundPerform });
  const backend = new MockBackend();
  // deno-lint-ignore no-explicit-any
  const jobsMap = new Map<string, any>(
    jobClasses.map((cls) => [new cls().jobName, cls]),
  );
  const worker = await Worker.start({
    jobsMap,
    backend,
    timeoutByJobName: resolveJobTimeouts(jobsMap),
  });
  return { backend, worker };
}

Deno.test("aroundPerform outcome matrix (design §5.2)", async (t) => {
  await t.step(
    "row 1: well-behaved wrapper, next resolves = success",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [SucceedingJob],
          async (_payload, next) => {
            await next();
          },
        );

        await backend.process(payloadFor("succeeding_job"));

        assertEquals(performRuns, 1);
        assertEquals(logs.hookErrors(), []);
        assert(logs.entries.some((e) => e.event === "job_succeeded"));
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 2: wrapper awaits and rethrows, next rejects = fails with next's error",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [RejectingJob],
          async (_payload, next) => {
            await next();
          },
        );

        await assertRejects(
          () => backend.process(payloadFor("rejecting_job")),
          Error,
          "perform failed",
        );

        assertEquals(performRuns, 1);
        assertEquals(logs.hookErrors(), []);
        assert(logs.entries.some((e) => e.event === "job_failed"));
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 2 variant: a substitute error thrown by the wrapper never replaces next's error",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [RejectingJob],
          async (_payload, next) => {
            try {
              await next();
            } catch {
              throw new Error("wrapper substitute error");
            }
          },
        );

        // The backend still sees next()'s error, not the wrapper's.
        await assertRejects(
          () => backend.process(payloadFor("rejecting_job")),
          Error,
          "perform failed",
        );
        assertEquals(logs.hookErrors(), []);
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 3: wrapper swallows next's rejection = fails anyway + hook_error",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [RejectingJob],
          async (_payload, next) => {
            try {
              await next();
            } catch {
              // swallow: this must not fake a success
            }
          },
        );

        await assertRejects(
          () => backend.process(payloadFor("rejecting_job")),
          Error,
          "perform failed",
        );

        const hookErrors = logs.hookErrors();
        assertEquals(hookErrors.length, 1);
        assertEquals(hookErrors[0].hook, "aroundPerform");
        assertEquals(hookErrors[0].jobName, "rejecting_job");
        assertEquals(hookErrors[0].queueName, "default");
        assertEquals(
          hookErrors[0].error,
          "aroundPerform resolved after next() rejected",
        );
        assert(logs.entries.some((e) => e.event === "job_failed"));
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 4: wrapper throws its own error after next resolved = job still succeeds + hook_error",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [SucceedingJob],
          async (_payload, next) => {
            await next();
            throw new Error("buggy reporter");
          },
        );

        // No rethrow to the backend: no phantom retry for work already done.
        await backend.process(payloadFor("succeeding_job"));

        assertEquals(performRuns, 1);
        const hookErrors = logs.hookErrors();
        assertEquals(hookErrors.length, 1);
        assertEquals(hookErrors[0].error, "buggy reporter");
        assert(logs.entries.some((e) => e.event === "job_succeeded"));
        assertEquals(
          logs.entries.some((e) => e.event === "job_failed"),
          false,
        );
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 5: wrapper throws before calling next = fails with wrapper's error, perform never ran",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [SucceedingJob],
          // deno-lint-ignore require-await
          async () => {
            throw new Error("wrapper setup failed");
          },
        );

        await assertRejects(
          () => backend.process(payloadFor("succeeding_job")),
          Error,
          "wrapper setup failed",
        );

        assertEquals(performRuns, 0);
        const hookErrors = logs.hookErrors();
        assertEquals(hookErrors.length, 1);
        assertEquals(hookErrors[0].error, "wrapper setup failed");
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 6: wrapper resolves without calling next = fails with the canonical message",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [SucceedingJob],
          async () => {},
        );

        await assertRejects(
          () => backend.process(payloadFor("succeeding_job")),
          Error,
          "aroundPerform completed without invoking next()",
        );

        assertEquals(performRuns, 0);
        const hookErrors = logs.hookErrors();
        assertEquals(hookErrors.length, 1);
        assertEquals(
          hookErrors[0].error,
          "aroundPerform completed without invoking next()",
        );
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 7: fire-and-forget wrapper = Hermes still awaits next's settlement",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker(
          [ControlledJob],
          // deno-lint-ignore require-await
          async (_payload, next) => {
            void next(); // resolves while next is still pending
          },
        );

        let settled = false;
        const processing = backend
          .process(payloadFor("controlled_job"))
          .then(() => {
            settled = true;
          });

        await new Promise((resolve) => setTimeout(resolve, 20));
        assertEquals(settled, false); // outcome waits for next, not the wrapper

        releasePerform("late result");
        await processing;
        assertEquals(settled, true);
        assertEquals(performRuns, 1);
        assert(logs.entries.some((e) => e.event === "job_succeeded"));
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 8: next() is memoized — a second call returns the same promise, single execution",
    async () => {
      const logs = captureLogs();
      try {
        let first: Promise<unknown> | undefined;
        let second: Promise<unknown> | undefined;
        const { backend } = await startWorker(
          [SucceedingJob],
          async (_payload, next) => {
            first = next();
            second = next();
            await first;
          },
        );

        await backend.process(payloadFor("succeeding_job"));

        assert(first !== undefined);
        assert(first === second);
        assertEquals(performRuns, 1);
        assertEquals(logs.hookErrors(), []);
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "row 9: timeout fires with a wrapper present = JobTimeoutError, abort, chain stays tracked",
    async () => {
      const logs = captureLogs();
      try {
        const { backend, worker } = await startWorker(
          [ControlledTimedJob],
          async (_payload, next) => {
            await next();
          },
        );

        const error = await assertRejects(() =>
          backend.process(payloadFor("controlled_timed_job"))
        );
        assert(error instanceof Error);
        assertEquals(error.name, "JobTimeoutError");

        // The wrapped chain keeps running after the timeout and stays
        // tracked until it settles, so graceful shutdown covers it.
        let drained = false;
        const draining = worker.awaitInFlight().then(() => {
          drained = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 20));
        assertEquals(drained, false);

        releasePerform();
        await draining;
        assertEquals(drained, true);
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "a wrapper that hangs after next resolved holds the timeout budget and the in-flight slot",
    async () => {
      const logs = captureLogs();
      try {
        let releaseWrapper: () => void = () => {};
        const { backend, worker } = await startWorker(
          [ControlledTimedJob],
          async (_payload, next) => {
            const result = next();
            // perform() starts on a microtask; yield once so it installs
            // its resolver before we release it.
            await Promise.resolve();
            releasePerform();
            await result;
            await new Promise<void>((resolve) => {
              releaseWrapper = resolve;
            });
          },
        );

        // The perform resolved almost instantly, but the composite outcome
        // waits on the wrapper — so the job's timeout still fires.
        const error = await assertRejects(() =>
          backend.process(payloadFor("controlled_timed_job"))
        );
        assert(error instanceof Error);
        assertEquals(error.name, "JobTimeoutError");

        let drained = false;
        const draining = worker.awaitInFlight().then(() => {
          drained = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 20));
        assertEquals(drained, false);

        releaseWrapper();
        await draining;
        assertEquals(drained, true);
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );

  await t.step(
    "no wrapper configured: jobs run exactly as before",
    async () => {
      const logs = captureLogs();
      try {
        const { backend } = await startWorker([SucceedingJob], undefined);

        await backend.process(payloadFor("succeeding_job"));

        assertEquals(performRuns, 1);
        assertEquals(logs.hookErrors(), []);
        assert(logs.entries.some((e) => e.event === "job_succeeded"));
      } finally {
        logs.restore();
        clearHooks();
      }
    },
  );
});

Deno.test("hook interplay invariants (design §4.2–§4.4)", async (t) => {
  await t.step(
    "unknown jobs are skipped before the wrapper ever runs",
    async () => {
      let wrapperRan = false;
      const { backend } = await startWorker(
        [SucceedingJob],
        async (_payload, next) => {
          wrapperRan = true;
          await next();
        },
      );
      try {
        await backend.process(payloadFor("job_nobody_registered"));
        assertEquals(wrapperRan, false);
      } finally {
        clearHooks();
      }
    },
  );

  await t.step(
    "§5.3 pin: a scheduled/recurring tick (registration-built 3-field payload) reaches the wrapper and perform with metadata === undefined, and enqueueMetadata never runs",
    async () => {
      let enqueueHookRuns = 0;
      let wrapperMetadata: unknown = "unset";
      let contextMetadata: unknown = "unset";

      class RecurringTickJob extends Job {
        readonly jobName = "recurring_tick_job";
        readonly queueName = "default";
        override readonly every = "5m";

        // deno-lint-ignore require-await
        async perform(_jobBody: unknown, context?: unknown): Promise<unknown> {
          contextMetadata = (context as { metadata?: unknown }).metadata;
          return null;
        }
      }

      performRuns = 0;
      clearHooks();
      setHooks({
        enqueueMetadata: () => {
          enqueueHookRuns++;
          return { boot: "context" };
        },
        aroundPerform: async (payload, next) => {
          wrapperMetadata = payload.metadata;
          await next();
        },
      });
      const backend = new MockBackend();
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([
        ["recurring_tick_job", RecurringTickJob],
      ]);
      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      try {
        // Both backends build exactly this 3-field payload at registration
        // time (design §4.4); a tick delivers it as-is.
        await backend.process({
          jobName: "recurring_tick_job",
          queueName: "default",
          jobBody: undefined,
        });

        assertEquals(wrapperMetadata, undefined);
        assertEquals(contextMetadata, undefined);
        assertEquals(enqueueHookRuns, 0);
      } finally {
        clearHooks();
      }
    },
  );

  await t.step(
    "§4.3 job→job chain: performLater inside perform is stamped via the registry Hermes() populates",
    async () => {
      clearBackend();
      clearHooks();
      const backend = new MockBackend();

      class ChildJob extends Job {
        readonly jobName = "child_job";
        readonly queueName = "default";

        async perform(): Promise<unknown> {
          return await Promise.resolve(null);
        }
      }

      class ParentJob extends Job {
        readonly jobName = "parent_job";
        readonly queueName = "default";

        async perform(): Promise<unknown> {
          await new ChildJob().performLater({ from: "parent" });
          return null;
        }
      }

      // Constructing Hermes registers backend AND hooks in the same
      // module-global registries performLater reads — no start() needed.
      Hermes({
        manifest: "./unused_manifest.ts",
        backend,
        hooks: { enqueueMetadata: () => ({ traceId: "parent-trace" }) },
      });

      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([["parent_job", ParentJob]]);
      await Worker.start({
        jobsMap,
        backend,
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      try {
        await backend.process(payloadFor("parent_job"));

        assertEquals(backend.enqueued.length, 1);
        assertEquals(backend.enqueued[0].payload.jobName, "child_job");
        assertEquals(backend.enqueued[0].payload.metadata, {
          traceId: "parent-trace",
        });
      } finally {
        clearBackend();
        clearHooks();
      }
    },
  );
});
