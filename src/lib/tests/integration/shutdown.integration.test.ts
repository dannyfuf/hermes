import { assert } from "@std/assert";
import {
  GracefulJob,
  RescueHangsJob,
  setRescueCompletes,
  UntimedHangsJob,
} from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  onlyQueueStats,
  readSink,
  runIntegration,
  waitFor,
  withTimeout,
} from "./helpers/integration.ts";

const ordinaryManifest = new URL(
  "./fixtures/ordinary_manifest.ts",
  import.meta.url,
).href;
const rescueManifest = new URL(
  "./fixtures/rescue_manifest.ts",
  import.meta.url,
).href;

integrationTest(
  "C6 graceful stop drains in-flight work before resolving",
  async () => {
    await runIntegration("c6", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        gracefulShutdownTimeout: 5_000,
      });
      await hermes.start();
      await new GracefulJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "c6-graceful",
      });
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "graceful_started"
          ),
        "C6 graceful job did not start",
        5_000,
      );

      await hermes.stop();
      assert(
        (await readSink(scope.sinkPath)).some((record) =>
          record.event === "graceful_completed" &&
          record.marker === "c6-graceful"
        ),
        "C6 sink was not complete before stop resolved",
      );
    });
  },
);

integrationTest(
  "B9 graceful timeout force-closes a hung job within a bound",
  async () => {
    await runIntegration("b9", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
        gracefulShutdownTimeout: 1_000,
      });
      await hermes.start();
      await new UntimedHangsJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b9-active",
      });
      await waitFor(
        async () => (await onlyQueueStats(hermes)).counts.active === 1,
        "B9 hung job never became active",
        5_000,
      );

      const startedAt = performance.now();
      await withTimeout(
        hermes.stop(),
        5_000,
        "B9 stop() did not resolve within 5s",
      );
      assert(
        performance.now() - startedAt < 5_000,
        "B9 stop() exceeded its 5s wall-clock bound",
      );
    });
  },
);

integrationTest(
  "stop is idempotent and the second call resolves immediately",
  async () => {
    await runIntegration("stop_idempotent", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();
      await hermes.stop();

      const startedAt = performance.now();
      await withTimeout(
        hermes.stop(),
        250,
        "Second stop call did not resolve promptly",
      );
      assert(performance.now() - startedAt < 250);
    });
  },
);

integrationTest(
  "B13 a force-closed hung job is rescued after its lock expires",
  async () => {
    await runIntegration("b13", async (scope) => {
      setRescueCompletes(false);
      const backendA = scope.backend();
      const workerA = scope.hermes(rescueManifest, backendA, {
        concurrency: 1,
        gracefulShutdownTimeout: 1_000,
      });
      await workerA.start();
      await new RescueHangsJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "b13-rescue",
      });
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "rescue_started"
          ),
        "B13 worker A did not acquire the hung job",
        5_000,
      );
      await withTimeout(
        workerA.stop(),
        5_000,
        "B13 worker A did not force-close within 5s",
      );

      setRescueCompletes(true);
      const backendB = scope.backend();
      const workerB = scope.hermes(rescueManifest, backendB, {
        concurrency: 1,
        gracefulShutdownTimeout: 1_000,
      });
      const rescueStartedAt = performance.now();
      await workerB.start();
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((record) =>
            record.event === "rescue_completed"
          ),
        "B13 force-closed job was not rescued within 120s",
        120_000,
      );
      const durationMs = Math.round(performance.now() - rescueStartedAt);
      console.log(JSON.stringify({
        event: "integration_b13_duration",
        durationMs,
      }));
      assert(durationMs <= 120_000);
    });
  },
  { slow: true },
);
