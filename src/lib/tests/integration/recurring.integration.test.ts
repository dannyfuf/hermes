import { assert, assertEquals } from "@std/assert";
import {
  integrationTest,
  onlyQueueStats,
  readSink,
  runIntegration,
  waitFor,
  withTimeout,
} from "./helpers/integration.ts";

const recurringTwoSecondsManifest = new URL(
  "./fixtures/recurring_2s_manifest.ts",
  import.meta.url,
).href;
const recurringOneHundredTwentySecondsManifest = new URL(
  "./fixtures/recurring_120s_manifest.ts",
  import.meta.url,
).href;
const cronManifest =
  new URL("./fixtures/cron_manifest.ts", import.meta.url).href;
const recurringTimeoutManifest = new URL(
  "./fixtures/recurring_timeout_manifest.ts",
  import.meta.url,
).href;
const recurringPriorityManifest = new URL(
  "./fixtures/recurring_priority_manifest.ts",
  import.meta.url,
).href;

integrationTest(
  "B10a every 2s executes at least twice about two seconds apart",
  async () => {
    await runIntegration("b10a", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(recurringTwoSecondsManifest, backend);
      await hermes.start();

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).filter((record) =>
            record.event === "recurring_2s"
          ).length >= 2,
        "B10a 2s recurring job did not execute twice within 7s",
        7_000,
      );
      const executions = (await readSink(scope.sinkPath)).filter((record) =>
        record.event === "recurring_2s"
      );
      const intervalMs = Number(executions[1].at) - Number(executions[0].at);
      assert(intervalMs >= 1_500 && intervalMs <= 3_000);
    });
  },
);

integrationTest("B10b every 120s registers as 120000ms", async () => {
  await runIntegration("b10b", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(
      recurringOneHundredTwentySecondsManifest,
      backend,
    );
    await withTimeout(
      hermes.start(),
      5_000,
      "B10b 120s recurring job did not register promptly",
    );

    const inspector = scope.inspector();
    try {
      const scheduler = await inspector.getJobScheduler(
        "hermes:recurring_120s",
      );
      // White-box: waiting two minutes is outside this suite's time budget.
      assert(scheduler !== undefined);
      assertEquals(scheduler.every, 120_000);
    } finally {
      await inspector.close();
    }
  });
});

integrationTest(
  "cron expression registers its BullMQ scheduler pattern",
  async () => {
    await runIntegration("cron", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(cronManifest, backend);
      await withTimeout(
        hermes.start(),
        5_000,
        "Cron scheduler did not register",
      );

      const inspector = scope.inspector();
      try {
        const scheduler = await inspector.getJobScheduler("hermes:cron_job");
        // White-box: scheduler metadata is the observable registration result.
        assert(scheduler !== undefined);
        assertEquals(scheduler.pattern, "*/2 * * * *");
      } finally {
        await inspector.close();
      }
    });
  },
);

integrationTest(
  "C3 recurring timed-out jobs keep ticking without wedging",
  async () => {
    await runIntegration("c3", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(recurringTimeoutManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();

      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.failed >= 2,
        "C3 did not accrue two failed recurring executions within 8s",
        8_000,
      );
      const starts = (await readSink(scope.sinkPath)).filter((record) =>
        record.event === "recurring_timeout_started"
      );
      assert(starts.length >= 2);
      assert((await onlyQueueStats(hermes)).counts.failed >= 2);
    });
  },
);

integrationTest("recurring priority flows to scheduled instances", async () => {
  await runIntegration("recurring_priority", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(recurringPriorityManifest, backend);
    await hermes.start();

    const inspector = scope.inspector();
    try {
      const scheduler = await inspector.getJobScheduler(
        "hermes:recurring_priority",
      );
      // White-box: the scheduler template is BullMQ's source for future ticks.
      assert(scheduler !== undefined);
      assertEquals(scheduler.template?.opts?.priority, 3);
    } finally {
      await inspector.close();
    }
  });
});
