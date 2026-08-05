import { assert, assertEquals } from "@std/assert";
import { configure, Job } from "../../../main.ts";
import {
  EchoJob,
  PrimaryQueueJob,
  SecondaryQueueJob,
} from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  readSink,
  runIntegration,
  waitFor,
  waitForElapsed,
} from "./helpers/integration.ts";

const echoManifest =
  new URL("./fixtures/echo_manifest.ts", import.meta.url).href;
const multiQueueManifest = new URL(
  "./fixtures/multi_queue_manifest.ts",
  import.meta.url,
).href;

integrationTest("B1 round-trip through the public API", async () => {
  await runIntegration("b1", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(echoManifest, backend);
    await hermes.start();

    await new EchoJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "b1-body",
    });

    await waitFor(
      async () =>
        (await readSink(scope.sinkPath)).some((record) =>
          record.marker === "b1-body"
        ),
      "B1 echo body did not reach the sink",
      5_000,
    );
  });
});

integrationTest(
  "B2 delayed jobs do not run early and do run by the deadline",
  async () => {
    await runIntegration("b2", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend);
      await hermes.start();

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b2-delayed",
      }, { delay: 800 });

      await waitForElapsed(300);
      assert(
        !(await readSink(scope.sinkPath)).some((record) =>
          record.marker === "b2-delayed"
        ),
        "B2 delayed job ran within 300ms",
      );
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "b2-delayed"
          ),
        "B2 delayed job did not run within 5s",
        5_000,
      );
    });
  },
);

integrationTest(
  "C7 payload fidelity preserves a deeply nested body",
  async () => {
    await runIntegration("c7", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend);
      await hermes.start();

      const body = {
        sinkPath: scope.sinkPath,
        marker: "payload-fidelity-こんにちは-🚚",
        nested: {
          array: [0, -17, 3.14159, true, false, null, "áéíóú"],
          object: { empty: "", zero: 0, negativeFloat: -0.125 },
        },
        enabled: true,
        absent: null,
      };
      await new EchoJob().performLater(body);

      await waitFor(
        async () => (await readSink(scope.sinkPath)).length === 1,
        "C7 payload did not reach the sink",
        5_000,
      );
      assertEquals((await readSink(scope.sinkPath))[0], body);
    });
  },
);

integrationTest(
  "B11 unknown jobs are acknowledged without poisoning the queue",
  async () => {
    await runIntegration("b11", async (scope) => {
      class GhostJob extends Job {
        readonly jobName = "ghost";
        readonly queueName = scope.queueName;

        perform(): Promise<void> {
          return Promise.resolve();
        }
      }

      const producerBackend = scope.backend();
      configure({ backend: producerBackend });
      await new GhostJob().performLater();

      const workerBackend = scope.backend();
      const hermes = scope.hermes(echoManifest, workerBackend);
      await hermes.start();
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b11-after-ghost",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.marker === "b11-after-ghost"
          ),
        "B11 echo did not process after the unknown job",
        5_000,
      );
      const [stats] = await hermes.stats();
      assertEquals(stats.counts.failed, 0);
    });
  },
);

integrationTest("C9 one Hermes routes and reports two queues", async () => {
  await runIntegration("c9", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(multiQueueManifest, backend);
    await hermes.start();

    await new PrimaryQueueJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "primary",
    });
    await new SecondaryQueueJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "secondary",
    });

    await waitFor(
      async () =>
        (await readSink(scope.sinkPath)).filter((record) =>
          record.event === "multi_queue"
        ).length === 2,
      "C9 jobs did not complete on both queues",
      5_000,
    );

    const records = await readSink(scope.sinkPath);
    assert(
      records.some((record) =>
        record.marker === "primary" && record.queueName === scope.queueName
      ),
    );
    assert(
      records.some((record) =>
        record.marker === "secondary" &&
        record.queueName === scope.secondaryQueueName
      ),
    );
    const stats = await hermes.stats();
    assertEquals(
      stats.map((entry) => entry.queueName).sort(),
      [scope.queueName, scope.secondaryQueueName].sort(),
    );
    assert(stats.every((entry) => entry.counts.completed >= 1));
  });
});
