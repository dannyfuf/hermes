import { assertEquals } from "@std/assert";
import { clearHooks } from "../../hooks_registry.ts";
import { AlwaysFailJob, MetadataEchoJob } from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  onlyQueueStats,
  readSink,
  runIntegration,
  waitFor,
} from "./helpers/integration.ts";

const hooksManifest = new URL(
  "./fixtures/hooks_manifest.ts",
  import.meta.url,
).href;

integrationTest(
  "H1 metadata round-trips queue.add → job.data → JobContext.metadata with zero backend changes",
  async () => {
    await runIntegration("h1", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(hooksManifest, backend);
      await hermes.start();

      const metadata = {
        traceId: "h1-trace",
        requestedBy: "user-42",
        nested: { flags: [true, false], count: 3 },
      };
      await new MetadataEchoJob().performLater(
        { sinkPath: scope.sinkPath, marker: "h1-metadata" },
        { metadata },
      );

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "h1-metadata"
          ),
        "H1 metadata echo job did not complete",
        5_000,
      );
      const record = (await readSink(scope.sinkPath)).find((entry) =>
        entry.marker === "h1-metadata"
      );
      assertEquals(record!.metadata, metadata);
    });
  },
);

integrationTest(
  "H1b a payload without metadata reaches the job with undefined context metadata",
  async () => {
    await runIntegration("h1b", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(hooksManifest, backend);
      await hermes.start();

      await new MetadataEchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "h1b-no-metadata",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "h1b-no-metadata"
          ),
        "H1b metadata echo job did not complete",
        5_000,
      );
      const record = (await readSink(scope.sinkPath)).find((entry) =>
        entry.marker === "h1b-no-metadata"
      );
      // The fixture writes null when context.metadata is undefined.
      assertEquals(record!.metadata, null);
    });
  },
);

integrationTest(
  "H2 a wrapper that swallows next()'s rejection cannot stop backend retries (§5.2 row 3, end to end)",
  async () => {
    await runIntegration("h2", async (scope) => {
      try {
        const backend = scope.backend({
          defaultJobOptions: {
            attempts: 2,
            backoff: { type: "fixed", delay: 200 },
          },
        });
        const hermes = scope.hermes(hooksManifest, backend, {
          concurrency: 1,
        }, {
          hooks: {
            aroundPerform: async (_payload, next) => {
              try {
                await next();
              } catch {
                // Swallow the failure. Outcome inertness means BullMQ still
                // sees the rejection and drives its retry machinery.
              }
            },
          },
        });
        await hermes.start();

        await new AlwaysFailJob().performLater({
          sinkPath: scope.sinkPath,
          marker: "h2-swallow",
        });

        await waitFor(
          async () => (await onlyQueueStats(hermes)).counts.failed === 1,
          "H2 job did not reach terminal failure despite the swallow",
          10_000,
        );
        const attempts = (await readSink(scope.sinkPath)).filter((record) =>
          record.event === "always_fail_attempt"
        );
        // attempts: 2 ⇒ the job was retried once after the first failure —
        // the swallow never converted the failure into a success.
        assertEquals(attempts.length, 2);
        assertEquals((await onlyQueueStats(hermes)).counts.completed, 0);
      } finally {
        clearHooks();
      }
    });
  },
);
