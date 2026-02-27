import { assertEquals, assertRejects } from "@std/assert";
import type { BackendAdapter, EnqueueOptions } from "../../backend.ts";
import type { JobPayload } from "../../types.ts";

/** Create an isolated DenoKvBackend using a temp directory */
async function createIsolatedBackend(): Promise<
  { backend: BackendAdapter; cleanup: () => Promise<void> }
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

  return { backend, cleanup };
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
  name: "DenoKvBackend: listen ignores queueNames option (single global queue)",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      const receivedPayloads: JobPayload[] = [];

      // Passing queueNames should not throw or change behavior
      await backend.listen(async (payload: JobPayload) => {
        receivedPayloads.push(payload);
        await Promise.resolve();
      }, { queueNames: ["custom", "other"] });

      await backend.enqueue({
        jobName: "any_job",
        queueName: "custom",
        jobBody: null,
      });

      await new Promise((resolve) => setTimeout(resolve, 1000));
      assertEquals(receivedPayloads.length, 1);
    } finally {
      await cleanup();
    }
  },
});

Deno.test({
  name: "DenoKvBackend: registerRecurringJob throws for seconds interval",
  async fn() {
    const { backend, cleanup } = await createIsolatedBackend();

    try {
      await assertRejects(
        () =>
          backend.registerRecurringJob!({
            jobName: "seconds_job",
            queueName: "default",
            every: "5s",
          }),
        Error,
        "Seconds-level intervals are not supported on the Deno KV backend",
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
