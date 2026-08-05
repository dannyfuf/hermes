import { assert, assertEquals } from "@std/assert";
import {
  AlwaysFailJob,
  EchoJob,
  FlakyJob,
} from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  onlyQueueStats,
  readSink,
  runIntegration,
  waitFor,
} from "./helpers/integration.ts";

const ordinaryManifest = new URL(
  "./fixtures/ordinary_manifest.ts",
  import.meta.url,
).href;
const echoManifest =
  new URL("./fixtures/echo_manifest.ts", import.meta.url).href;

integrationTest(
  "B6 a flaky job succeeds on its third backend attempt",
  async () => {
    await runIntegration("b6", async (scope) => {
      const backend = scope.backend({
        defaultJobOptions: {
          attempts: 3,
          backoff: { type: "fixed", delay: 200 },
        },
      });
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();

      await new FlakyJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b6-flaky",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "flaky_success"
          ),
        "B6 flaky job did not succeed within three attempts",
        15_000,
      );
      const records = await readSink(scope.sinkPath);
      assertEquals(
        records.filter((record) => record.event === "flaky_attempt").map(
          (record) => record.attempt,
        ),
        [1, 2, 3],
      );
      assert(records.some((record) => record.event === "flaky_success"));
    });
  },
);

integrationTest(
  "C10 terminal failure is counted once across attempts",
  async () => {
    await runIntegration("c10", async (scope) => {
      const backend = scope.backend({ defaultJobOptions: { attempts: 2 } });
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();

      await new AlwaysFailJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "c10-always-fail",
      });
      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.failed === 1,
        "C10 job did not reach terminal failure",
        5_000,
      );

      const attempts = (await readSink(scope.sinkPath)).filter((record) =>
        record.event === "always_fail_attempt"
      );
      assertEquals(attempts.length, 2);
      assertEquals((await onlyQueueStats(hermes)).counts.failed, 1);
    });
  },
);

integrationTest(
  "B12 completed jobs carry bounded retention defaults",
  async () => {
    await runIntegration("b12", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend);
      await hermes.start();
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b12-retention",
      });
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "b12-retention"
          ),
        "B12 retained job did not complete",
        5_000,
      );

      const inspector = scope.inspector();
      try {
        const [job] = await inspector.getCompleted(0, 0);
        // White-box: exercising count-based trimming would require 1,001 jobs.
        assert(job !== undefined);
        assertEquals(job.opts.removeOnComplete, { count: 1_000 });
        assertEquals(job.opts.removeOnFail, { count: 5_000 });
      } finally {
        await inspector.close();
      }
    });
  },
);
