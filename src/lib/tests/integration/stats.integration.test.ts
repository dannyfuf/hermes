import { assert, assertEquals, assertRejects } from "@std/assert";
import {
  AlwaysFailJob,
  EchoJob,
  UntimedHangsJob,
} from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  onlyQueueStats,
  runIntegration,
  waitFor,
} from "./helpers/integration.ts";

const ordinaryManifest = new URL(
  "./fixtures/ordinary_manifest.ts",
  import.meta.url,
).href;

integrationTest(
  "B8 stats snapshot active and prioritized waiting work",
  async () => {
    await runIntegration("b8", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
        gracefulShutdownTimeout: 250,
      });
      await hermes.start();

      await new UntimedHangsJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b8-active",
      });
      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.active === 1,
        "B8 hung job never became active",
        5_000,
      );
      const activeObservedAt = Date.now();

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b8-waiting-1",
      }, { priority: 1 });
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b8-waiting-2",
      }, { priority: 2 });

      await waitFor(
        async () => {
          const stats = await onlyQueueStats(hermes);
          return stats.counts.active === 1 && stats.counts.waiting >= 2 &&
            (stats.counts.prioritized ?? 0) >= 2 &&
            (stats.oldestActiveJobAgeMs ?? -1) >= Date.now() - activeObservedAt;
        },
        "B8 stats did not show one active job and two prioritized waiters",
        5_000,
      );

      const first = await onlyQueueStats(hermes);
      const firstAge = first.oldestActiveJobAgeMs;
      assertEquals(first.counts.active, 1);
      assert(first.counts.waiting >= 2);
      assert((first.counts.prioritized ?? 0) >= 2);
      assert(firstAge !== undefined);

      let secondAge: number | undefined;
      await waitFor(
        async () => {
          secondAge = (await onlyQueueStats(hermes)).oldestActiveJobAgeMs;
          return secondAge !== undefined && secondAge > firstAge;
        },
        "B8 oldest active job age did not grow",
        2_000,
      );
      assert(secondAge !== undefined && secondAge > firstAge);
    });
  },
);

integrationTest(
  "stats count completed and terminally failed jobs",
  async () => {
    await runIntegration("stats_terminal", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "stats-completed",
      });
      await new AlwaysFailJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "stats-failed",
      });
      await waitFor(
        async () => {
          const stats = await onlyQueueStats(hermes);
          return stats.counts.completed >= 1 && stats.counts.failed === 1;
        },
        "Stats did not count completed and terminally failed jobs",
        5_000,
      );
      const stats = await onlyQueueStats(hermes);
      assert(stats.counts.completed >= 1);
      assertEquals(stats.counts.failed, 1);
    });
  },
);

integrationTest(
  "stats polling tolerates jobs removed during high-churn completion",
  async () => {
    await runIntegration("stats_churn", async (scope) => {
      const backend = scope.backend({
        defaultJobOptions: { removeOnComplete: true },
      });
      let handled = 0;
      await backend.listen(() => {
        handled += 1;
        return Promise.resolve();
      }, { queueNames: [scope.queueName], concurrency: 10 });

      let polling = true;
      let polls = 0;
      const pollingErrors: unknown[] = [];
      const pollStats = (async () => {
        while (polling) {
          try {
            await backend.getQueueStats!(scope.queueName);
            polls += 1;
          } catch (error) {
            pollingErrors.push(error);
          }
        }
      })();

      try {
        await Promise.all(
          Array.from({ length: 300 }, (_, index) =>
            backend.enqueue({
              jobName: `stats_churn_${index}`,
              queueName: scope.queueName,
              jobBody: null,
            })),
        );
        await waitFor(
          () => handled === 300,
          "High-churn jobs did not all complete",
          10_000,
        );
      } finally {
        polling = false;
        await pollStats;
      }

      assert(polls > 0);
      assertEquals(pollingErrors, []);
    });
  },
);

integrationTest("stats reject before start and after stop", async () => {
  await runIntegration("stats_lifecycle", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend);
    await assertRejects(
      () => hermes.stats(),
      Error,
      "Hermes stats are only available after start().",
    );
    await hermes.start();
    await hermes.stop();
    await assertRejects(
      () => hermes.stats(),
      Error,
      "Hermes stats are only available after start().",
    );
  });
});
