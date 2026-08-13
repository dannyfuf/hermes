import { assertEquals } from "@std/assert";
import { resolveJobTimeouts, Worker } from "../../worker.ts";
import { Job } from "../../job.ts";
import { clearBackend, setBackend } from "../../backend_registry.ts";
import { clearHooks, setHooks } from "../../hooks_registry.ts";
import { DenoKvBackend } from "../../backends/deno_kv.ts";
import type { BackendAdapter } from "../../backend.ts";
import type { JobContext } from "../../types.ts";
import { waitFor } from "./helpers/integration.ts";

async function withIsolatedKvBackend(
  test: (backend: BackendAdapter) => Promise<void>,
): Promise<void> {
  const tempDir = await Deno.makeTempDir();
  const backend = DenoKvBackend({ path: `${tempDir}/test.kv` });
  clearBackend();
  clearHooks();
  try {
    await test(backend);
  } finally {
    clearBackend();
    clearHooks();
    await backend.close();
    try {
      await Deno.remove(tempDir, { recursive: true });
    } catch { /* best effort */ }
  }
}

Deno.test(
  "Deno KV: metadata survives the structured clone through kv.enqueue → listenQueue → JobContext.metadata",
  async () => {
    await withIsolatedKvBackend(async (backend) => {
      let seenMetadata: unknown = "unset";

      class KvMetadataJob extends Job {
        readonly jobName = "kv_metadata_job";
        readonly queueName = "default";

        // deno-lint-ignore require-await
        async perform(
          _jobBody: unknown,
          context?: JobContext,
        ): Promise<unknown> {
          seenMetadata = context?.metadata;
          return null;
        }
      }

      setBackend(backend);
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([
        ["kv_metadata_job", KvMetadataJob],
      ]);
      await Worker.start({
        jobsMap,
        backend,
        queueNames: ["default"],
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      const metadata = {
        traceId: "kv-trace",
        nested: { list: [1, 2, 3], flag: true, empty: null },
      };
      await new KvMetadataJob().performLater({ n: 1 }, { metadata });

      await waitFor(
        () => seenMetadata !== "unset",
        "Deno KV metadata job did not run",
        5_000,
      );
      assertEquals(seenMetadata, metadata);
    });
  },
);

Deno.test(
  "Deno KV: a scheduled tick's registration-built payload arrives with metadata === undefined",
  async () => {
    // Driving a real Deno.cron tick in-test is impractical (minimum 1-minute
    // granularity). The cron closure enqueues the registration-built 3-field
    // payload via backend.enqueue (deno_kv.ts registerRecurringJob), so this
    // exercises exactly that payload through the real KV queue.
    await withIsolatedKvBackend(async (backend) => {
      let enqueueHookRuns = 0;
      let wrapperMetadata: unknown = "unset";
      let contextMetadata: unknown = "unset";

      class KvRecurringJob extends Job {
        readonly jobName = "kv_recurring_job";
        readonly queueName = "default";
        override readonly every = "5m";

        // deno-lint-ignore require-await
        async perform(
          _jobBody: unknown,
          context?: JobContext,
        ): Promise<unknown> {
          contextMetadata = context?.metadata;
          return null;
        }
      }

      setBackend(backend);
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
      // deno-lint-ignore no-explicit-any
      const jobsMap = new Map<string, any>([
        ["kv_recurring_job", KvRecurringJob],
      ]);
      await Worker.start({
        jobsMap,
        backend,
        queueNames: ["default"],
        timeoutByJobName: resolveJobTimeouts(jobsMap),
      });

      // The exact payload the KV cron closure enqueues on every tick.
      await backend.enqueue({
        jobName: "kv_recurring_job",
        queueName: "default",
        jobBody: undefined,
      });

      await waitFor(
        () => contextMetadata !== "unset",
        "Deno KV recurring-tick payload did not run",
        5_000,
      );
      assertEquals(wrapperMetadata, undefined);
      assertEquals(contextMetadata, undefined);
      assertEquals(enqueueHookRuns, 0);
    });
  },
);
