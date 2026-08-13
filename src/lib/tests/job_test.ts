import { assertEquals, assertRejects } from "@std/assert";
import { clearBackend, setBackend } from "../backend_registry.ts";
import { clearHooks, setHooks } from "../hooks_registry.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import {
  CustomQueueJob,
  RecurringCronJob,
  RecurringEveryJob,
  TestJob,
} from "./helpers/test_jobs.ts";

class PrioritizedJob extends TestJob {
  override readonly priority = 10;
}

Deno.test("Job", async (t) => {
  await t.step("subclass can define jobName and queueName", () => {
    const job = new TestJob();
    assertEquals(job.jobName, "test_job");
    assertEquals(job.queueName, "default");
  });

  await t.step(
    "performLater() delegates to the configured backend",
    async () => {
      clearBackend();
      const backend = new MockBackend();
      setBackend(backend);

      const job = new TestJob();
      await job.performLater({ message: "hello" });

      assertEquals(backend.enqueued.length, 1);
      assertEquals(backend.enqueued[0].payload.jobName, "test_job");
      assertEquals(backend.enqueued[0].payload.queueName, "default");
      assertEquals(backend.enqueued[0].payload.jobBody, { message: "hello" });
    },
  );

  await t.step("performLater() throws if no backend configured", async () => {
    clearBackend();
    const job = new TestJob();
    await assertRejects(
      () => job.performLater({ message: "hello" }),
      Error,
      "No backend configured",
    );
  });

  await t.step("performLater() passes delay option through", async () => {
    clearBackend();
    const backend = new MockBackend();
    setBackend(backend);

    const job = new TestJob();
    await job.performLater({ message: "delayed" }, { delay: 5000 });

    assertEquals(backend.enqueued.length, 1);
    assertEquals(backend.enqueued[0].options?.delay, 5000);
  });

  await t.step("performLater() passes the job priority through", async () => {
    clearBackend();
    const backend = new MockBackend();
    setBackend(backend);

    await new PrioritizedJob().performLater({ message: "priority" });

    assertEquals(backend.enqueued[0].options?.priority, 10);
  });

  await t.step(
    "performLater() lets the call override job priority",
    async () => {
      clearBackend();
      const backend = new MockBackend();
      setBackend(backend);

      await new PrioritizedJob().performLater(undefined, { priority: 2 });

      assertEquals(backend.enqueued[0].options?.priority, 2);
    },
  );

  await t.step("performLater() constructs correct JobPayload", async () => {
    clearBackend();
    const backend = new MockBackend();
    setBackend(backend);

    const job = new CustomQueueJob();
    await job.performLater({ data: 42 });

    const payload = backend.enqueued[0].payload;
    assertEquals(payload.jobName, "custom_queue_job");
    assertEquals(payload.queueName, "priority");
    assertEquals(payload.jobBody, { data: 42 });
  });

  await t.step(
    "performLater() with no body sends undefined jobBody",
    async () => {
      clearBackend();
      const backend = new MockBackend();
      setBackend(backend);

      const job = new TestJob();
      await job.performLater();

      assertEquals(backend.enqueued[0].payload.jobBody, undefined);
    },
  );

  await t.step("isRecurring() returns false for non-recurring jobs", () => {
    const job = new TestJob();
    assertEquals(job.isRecurring(), false);
  });

  await t.step("isRecurring() returns true for every-based jobs", () => {
    const job = new RecurringEveryJob();
    assertEquals(job.isRecurring(), true);
  });

  await t.step("isRecurring() returns true for cron-based jobs", () => {
    const job = new RecurringCronJob();
    assertEquals(job.isRecurring(), true);
  });

  await t.step(
    "getScheduleMs() returns milliseconds for every-based jobs",
    () => {
      const job = new RecurringEveryJob();
      assertEquals(job.getScheduleMs(), 300_000); // 5m = 300,000ms
    },
  );

  await t.step("getScheduleMs() returns undefined for cron-based jobs", () => {
    const job = new RecurringCronJob();
    assertEquals(job.getScheduleMs(), undefined);
  });

  await t.step(
    "getScheduleMs() returns undefined for non-recurring jobs",
    () => {
      const job = new TestJob();
      assertEquals(job.getScheduleMs(), undefined);
    },
  );
});

Deno.test("Job metadata and enqueueMetadata hook", async (t) => {
  function freshBackend(): MockBackend {
    clearBackend();
    clearHooks();
    const backend = new MockBackend();
    setBackend(backend);
    return backend;
  }

  await t.step(
    "#1 no hooks: payload deep-equals today's shape, no metadata key",
    async () => {
      const backend = freshBackend();

      await new TestJob().performLater({ message: "hello" });

      const payload = backend.enqueued[0].payload;
      assertEquals(payload, {
        jobName: "test_job",
        queueName: "default",
        jobBody: { message: "hello" },
      });
      assertEquals("metadata" in payload, false);
    },
  );

  await t.step(
    "per-call metadata rides the payload without hooks",
    async () => {
      const backend = freshBackend();

      await new TestJob().performLater({ n: 1 }, {
        metadata: { requestedBy: "user-7" },
      });

      assertEquals(backend.enqueued[0].payload.metadata, {
        requestedBy: "user-7",
      });
    },
  );

  await t.step(
    "#2 hook-contributed metadata is stamped on the payload",
    async () => {
      const backend = freshBackend();
      setHooks({ enqueueMetadata: () => ({ traceId: "abc" }) });

      await new TestJob().performLater();

      assertEquals(backend.enqueued[0].payload.metadata, { traceId: "abc" });
      clearHooks();
    },
  );

  await t.step("#2 per-call keys win over hook keys on collision", async () => {
    const backend = freshBackend();
    setHooks({ enqueueMetadata: () => ({ traceId: "hook", extra: true }) });

    await new TestJob().performLater(undefined, {
      metadata: { traceId: "explicit" },
    });

    assertEquals(backend.enqueued[0].payload.metadata, {
      traceId: "explicit",
      extra: true,
    });
    clearHooks();
  });

  await t.step(
    "#2 empty merge results leave the payload without a metadata key",
    async () => {
      const backend = freshBackend();

      // Hook returns undefined, no per-call metadata.
      setHooks({ enqueueMetadata: () => undefined });
      await new TestJob().performLater();

      // Hook returns {}, no per-call metadata.
      setHooks({ enqueueMetadata: () => ({}) });
      await new TestJob().performLater();

      // Empty per-call metadata, no hook.
      clearHooks();
      await new TestJob().performLater(undefined, { metadata: {} });

      for (const { payload } of backend.enqueued) {
        assertEquals("metadata" in payload, false);
      }
      assertEquals(backend.enqueued.length, 3);
    },
  );

  await t.step(
    "#3 a throwing hook rejects performLater and nothing is enqueued",
    async () => {
      const backend = freshBackend();
      setHooks({
        enqueueMetadata: () => {
          throw new Error("hook exploded");
        },
      });

      await assertRejects(
        () => new TestJob().performLater({ n: 1 }),
        Error,
        "hook exploded",
      );
      assertEquals(backend.enqueued.length, 0);
      clearHooks();
    },
  );

  await t.step(
    "the hook sees the pre-merge payload (no metadata key yet)",
    async () => {
      const backend = freshBackend();
      let seen: unknown;
      setHooks({
        enqueueMetadata: (payload) => {
          seen = { ...payload, hadMetadata: "metadata" in payload };
          return { stamped: true };
        },
      });

      await new CustomQueueJob().performLater({ data: 42 }, {
        metadata: { requestId: "r1" },
      });

      assertEquals(seen, {
        jobName: "custom_queue_job",
        queueName: "priority",
        jobBody: { data: 42 },
        hadMetadata: false,
      });
      assertEquals(backend.enqueued[0].payload.metadata, {
        stamped: true,
        requestId: "r1",
      });
      clearHooks();
    },
  );

  await t.step(
    "the hook cannot affect enqueue options (delay, priority)",
    async () => {
      const backend = freshBackend();
      setHooks({ enqueueMetadata: () => ({ traceId: "t" }) });

      await new TestJob().performLater(undefined, { delay: 250, priority: 3 });

      assertEquals(backend.enqueued[0].options, { delay: 250, priority: 3 });
      clearHooks();
    },
  );

  clearBackend();
  clearHooks();
});
