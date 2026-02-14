import { assertEquals, assertRejects } from "@std/assert";
import { clearBackend, setBackend } from "../backend_registry.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import { CustomQueueJob, TestJob } from "./helpers/test_jobs.ts";

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
});
