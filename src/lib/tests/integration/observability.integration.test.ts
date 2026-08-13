import { assert, assertEquals } from "@std/assert";
import { configure } from "../../../main.ts";
import type { LogEvent } from "../../../main.ts";
import {
  AlwaysFailJob,
  ChainParentJob,
  EchoJob,
  MetadataEchoJob,
} from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  onlyQueueStats,
  readSink,
  runIntegration,
  waitFor,
} from "./helpers/integration.ts";

const echoManifest =
  new URL("./fixtures/echo_manifest.ts", import.meta.url).href;
const hooksManifest = new URL(
  "./fixtures/hooks_manifest.ts",
  import.meta.url,
).href;
const chainManifest = new URL(
  "./fixtures/chain_manifest.ts",
  import.meta.url,
).href;
const recurring2sManifest = new URL(
  "./fixtures/recurring_2s_manifest.ts",
  import.meta.url,
).href;

integrationTest(
  "O1 the sink receives core and backend-emitted events with the metadata echo",
  async () => {
    await runIntegration("o1", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend(); // default attempts: 1
      const hermes = scope.hermes(hooksManifest, backend, {}, {
        logger: (event) => events.push(event),
      });
      await hermes.start();

      const metadata = { traceId: "o1-trace" };
      await new AlwaysFailJob().performLater(
        { sinkPath: scope.sinkPath, marker: "o1-fail" },
        { metadata },
      );

      // worker_job_failed is emitted by the BullMQ backend itself — it
      // reaching the sink proves backend log lines route with zero backend
      // changes.
      await waitFor(
        () =>
          events.some((e) => e.event === "job_failed") &&
          events.some((e) => e.event === "worker_job_failed"),
        "O1 job_failed and worker_job_failed did not both reach the sink",
        10_000,
      );

      for (const name of ["job_received", "job_started", "job_failed"]) {
        const event = events.find((e) => e.event === name);
        assert(event !== undefined, `O1 ${name} missing from the sink`);
        assertEquals(event.jobName, "always_fail");
        assertEquals(event.metadata, metadata);
      }
      const backendEvent = events.find((e) => e.event === "worker_job_failed");
      assertEquals(backendEvent!.jobName, "always_fail");
      assertEquals(backendEvent!.attemptsMade, 1);
      assertEquals(backendEvent!.error, "always fails");
    });
  },
);

integrationTest(
  "O2 a wrapper that throws after next() resolved cannot fail the job or trigger a retry (§5.2 row 4, end to end)",
  async () => {
    await runIntegration("o2", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend({
        defaultJobOptions: { attempts: 3 }, // retries WOULD happen on failure
      });
      const hermes = scope.hermes(echoManifest, backend, {}, {
        hooks: {
          aroundPerform: async (_payload, next) => {
            await next();
            throw new Error("buggy reporter after success");
          },
        },
        logger: (event) => events.push(event),
      });
      await hermes.start();

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "o2-row4",
      });

      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.completed === 1,
        "O2 job did not complete despite the wrapper fault",
        10_000,
      );

      // The job ran exactly once, BullMQ counted it completed, and the
      // wrapper fault surfaced only as a hook_error event.
      const stats = await onlyQueueStats(hermes);
      assertEquals(stats.counts.failed, 0);
      assertEquals(
        (await readSink(scope.sinkPath)).filter((r) => r.marker === "o2-row4")
          .length,
        1,
      );
      const hookErrors = events.filter((e) => e.event === "hook_error");
      assertEquals(hookErrors.length, 1);
      assertEquals(hookErrors[0].hook, "aroundPerform");
      assertEquals(hookErrors[0].error, "buggy reporter after success");
      assert(events.some((e) => e.event === "job_succeeded"));
      assertEquals(events.some((e) => e.event === "job_failed"), false);
    });
  },
);

integrationTest(
  "O3 metadata stamped by a producer-process hook is consumed by a worker that never had the hook",
  async () => {
    await runIntegration("o3", async (scope) => {
      // Producer process: configure() with an enqueueMetadata hook stamps
      // the payload at enqueue time.
      const producerBackend = scope.backend();
      configure({
        backend: producerBackend,
        hooks: { enqueueMetadata: () => ({ stampedBy: "producer" }) },
      });
      await new MetadataEchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "o3-handoff",
      });

      // Worker process: constructing Hermes without hooks REPLACES the
      // registration (clears the producer hook) — the payload must already
      // carry the stamp on the wire.
      const workerBackend = scope.backend();
      const hermes = scope.hermes(hooksManifest, workerBackend);
      await hermes.start();

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "o3-handoff"
          ),
        "O3 stamped job was not consumed",
        5_000,
      );
      const record = (await readSink(scope.sinkPath)).find((r) =>
        r.marker === "o3-handoff"
      );
      assertEquals(record!.metadata, { stampedBy: "producer" });
    });
  },
);

integrationTest(
  "O5 a real recurring tick reaches the wrapper with metadata === undefined (§5.3 pin, end to end)",
  async () => {
    await runIntegration("o5", async (scope) => {
      let enqueueHookRuns = 0;
      const tickMetadata: unknown[] = [];
      const backend = scope.backend();
      const hermes = scope.hermes(recurring2sManifest, backend, {}, {
        hooks: {
          enqueueMetadata: () => {
            enqueueHookRuns++;
            return { boot: "context" };
          },
          aroundPerform: async (payload, next) => {
            tickMetadata.push(payload.metadata);
            await next();
          },
        },
      });
      await hermes.start();

      await waitFor(
        () => tickMetadata.length >= 1,
        "O5 no recurring tick executed within 7s",
        7_000,
      );

      // The scheduler-stored payload was frozen at registration time:
      // enqueue-time stamping never ran and no boot context leaked into the
      // tick — undefined is the documented "start fresh" signal.
      assertEquals(tickMetadata[0], undefined);
      assertEquals(enqueueHookRuns, 0);
    });
  },
);

integrationTest(
  "O4 job→job chains on BullMQ are stamped by the worker-registered enqueueMetadata hook",
  async () => {
    await runIntegration("o4", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(chainManifest, backend, {}, {
        hooks: { enqueueMetadata: () => ({ chainStamp: "o4" }) },
      });
      await hermes.start();

      await new ChainParentJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "o4-parent",
        childMarker: "o4-child",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) => r.marker === "o4-child"),
        "O4 the chained child job did not complete",
        5_000,
      );
      const child = (await readSink(scope.sinkPath)).find((r) =>
        r.marker === "o4-child"
      );
      assertEquals(child!.metadata, { chainStamp: "o4" });
      assert(
        (await readSink(scope.sinkPath)).some((r) =>
          r.event === "chain_parent" && r.marker === "o4-parent"
        ),
      );
    });
  },
);
