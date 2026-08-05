import { assert, assertEquals } from "@std/assert";
import {
  AbortAwareJob,
  DelayTimedJob,
  EchoJob,
  NumericTimedHangsJob,
  RetryTimedHangsJob,
  SlowJob,
  TimedHangsJob,
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

integrationTest("B3 a timed-out job frees a concurrency-one slot", async () => {
  await runIntegration("b3", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend, { concurrency: 1 });
    await hermes.start();

    await new TimedHangsJob().performLater();
    await new EchoJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "b3-after-timeout",
    });

    await waitFor(
      async () =>
        (await readSink(scope.sinkPath)).some((record) =>
          record.marker === "b3-after-timeout"
        ),
      "B3 echo did not run after the timed-out job",
      10_000,
    );
    await waitFor(
      async () => (await onlyQueueStats(hermes)).counts.failed >= 1,
      "B3 timed-out job did not reach failed state",
      5_000,
    );
  });
});

integrationTest(
  "B4 a timed-out job receives cooperative cancellation",
  async () => {
    await runIntegration("b4", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();

      await new AbortAwareJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b4-aborted",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "aborted" && record.marker === "b4-aborted"
          ),
        "B4 abort marker did not reach the sink",
        5_000,
      );
    });
  },
);

integrationTest("B5 a job without a timeout completes untouched", async () => {
  await runIntegration("b5", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend);
    await hermes.start();

    await new SlowJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "b5-slow",
    });

    await waitFor(
      async () =>
        (await readSink(scope.sinkPath)).some((record) =>
          record.marker === "b5-slow"
        ),
      "B5 slow job did not complete",
      5_000,
    );
    assertEquals((await onlyQueueStats(hermes)).counts.failed, 0);
  });
});

integrationTest(
  "C2 job timeout takes precedence over worker default",
  async () => {
    await runIntegration("c2", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        defaultJobTimeout: "10s",
      });
      await hermes.start();

      const startedAt = performance.now();
      await new NumericTimedHangsJob().performLater();
      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.failed === 1,
        "C2 job-level timeout did not fail the job",
        3_000,
      );
      assert(performance.now() - startedAt < 3_000);
    });
  },
);

integrationTest(
  "C8 enqueue delay does not consume execution timeout",
  async () => {
    await runIntegration("c8", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();

      await new DelayTimedJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "c8-delay-timeout",
      }, { delay: 1_000 });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "delay_timed_completed"
          ),
        "C8 delayed timed job did not complete",
        5_000,
      );
      assertEquals((await onlyQueueStats(hermes)).counts.failed, 0);
    });
  },
);

integrationTest(
  "C1 timed-out jobs retry and then fail terminally",
  async () => {
    await runIntegration("c1", async (scope) => {
      const backend = scope.backend({
        defaultJobOptions: {
          attempts: 2,
          backoff: { type: "fixed", delay: 100 },
        },
      });
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();

      await new RetryTimedHangsJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "c1-timeout-retry",
      });
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "c1-after-timeouts",
      });

      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.failed === 1,
        "C1 timed-out job did not fail terminally",
        8_000,
      );
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "c1-after-timeouts"
          ),
        "C1 worker slot was not freed for the echo job",
        5_000,
      );

      const timeoutAttempts = (await readSink(scope.sinkPath)).filter((
        record,
      ) => record.event === "timeout_attempt");
      assertEquals(timeoutAttempts.length, 2);
      const inspector = scope.inspector();
      try {
        const [failedJob] = await inspector.getFailed(0, 0);
        // White-box: BullMQ's attemptsMade is the durable evidence of backend retries.
        assert(failedJob !== undefined);
        assertEquals(failedJob.attemptsMade, 2);
      } finally {
        await inspector.close();
      }
    });
  },
);
