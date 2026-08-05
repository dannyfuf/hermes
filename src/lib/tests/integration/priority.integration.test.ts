import { assert, assertEquals } from "@std/assert";
import { configure } from "../../../main.ts";
import {
  BlockingJob,
  DefaultPriorityJob,
  EchoJob,
  HighPriorityJob,
  LowPriorityJob,
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

function priorityMarkers(
  records: Record<string, unknown>[],
): unknown[] {
  return records.filter((record) => record.event === "priority").map((record) =>
    record.marker
  );
}

integrationTest(
  "B7 lower numeric priority runs first from a backlog",
  async () => {
    await runIntegration("b7", async (scope) => {
      const backend = scope.backend();
      configure({ backend });
      await new LowPriorityJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "low",
      });
      await new HighPriorityJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "high",
      });

      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();
      await waitFor(
        async () =>
          priorityMarkers(await readSink(scope.sinkPath)).length === 2,
        "B7 priority jobs did not both complete",
        5_000,
      );

      assertEquals(priorityMarkers(await readSink(scope.sinkPath)), [
        "high",
        "low",
      ]);
    });
  },
);

integrationTest(
  "job priority property defaults and performLater override wins",
  async () => {
    await runIntegration("priority_override", async (scope) => {
      const backend = scope.backend();
      configure({ backend });
      await new DefaultPriorityJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "class-default",
      });
      await new DefaultPriorityJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "call-override",
      }, { priority: 1 });

      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();
      await waitFor(
        async () =>
          priorityMarkers(await readSink(scope.sinkPath)).length === 2,
        "Priority default/override jobs did not both complete",
        5_000,
      );
      assertEquals(priorityMarkers(await readSink(scope.sinkPath)), [
        "call-override",
        "class-default",
      ]);
    });
  },
);

integrationTest("C4 high priority jumps a waiting backlog", async () => {
  await runIntegration("c4", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend, { concurrency: 1 });
    await hermes.start();

    await new BlockingJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "blocker",
      durationMs: 800,
    });
    await waitFor(
      async () =>
        (await readSink(scope.sinkPath)).some((record) =>
          record.event === "blocking_started"
        ),
      "C4 blocker did not occupy the worker slot",
      5_000,
    );
    await new LowPriorityJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "low",
    });
    await new HighPriorityJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "high",
    });

    await waitFor(
      async () => priorityMarkers(await readSink(scope.sinkPath)).length === 2,
      "C4 priority backlog did not drain",
      5_000,
    );
    assertEquals(priorityMarkers(await readSink(scope.sinkPath)), [
      "high",
      "low",
    ]);
  });
});

integrationTest(
  "prioritized jobs appear in the waiting backlog stats",
  async () => {
    await runIntegration("priority_stats", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
        gracefulShutdownTimeout: 250,
      });
      await hermes.start();
      await new BlockingJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "stats-blocker",
        durationMs: 800,
      });
      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.active === 1,
        "Priority stats blocker did not become active",
        5_000,
      );
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "stats-priority",
      }, { priority: 1 });

      await waitFor(
        async () => {
          const stats = await onlyQueueStats(hermes);
          return stats.counts.waiting >= 1 &&
            (stats.counts.prioritized ?? 0) >= 1;
        },
        "Prioritized job was absent from waiting stats",
        5_000,
      );
      const stats = await onlyQueueStats(hermes);
      assert(stats.counts.waiting >= 1);
      assert((stats.counts.prioritized ?? 0) >= 1);
    });
  },
);
