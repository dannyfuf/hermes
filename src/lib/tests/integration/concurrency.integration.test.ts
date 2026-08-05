import { assert, assertEquals } from "@std/assert";
import { ConcurrentJob } from "./fixtures/integration_jobs.ts";
import {
  integrationTest,
  readSink,
  runIntegration,
  waitFor,
} from "./helpers/integration.ts";

const ordinaryManifest = new URL(
  "./fixtures/ordinary_manifest.ts",
  import.meta.url,
).href;

type ExecutionWindow = { start: number; end: number };

async function executionWindows(sinkPath: string): Promise<ExecutionWindow[]> {
  const records = await readSink(sinkPath);
  const markers = ["one", "two", "three"];
  return markers.map((marker) => {
    const start = records.find((record) =>
      record.event === "concurrency_started" && record.marker === marker
    );
    const end = records.find((record) =>
      record.event === "concurrency_ended" && record.marker === marker
    );
    if (!start || !end) {
      throw new Error(`Missing execution window for ${marker}`);
    }
    return { start: Number(start.at), end: Number(end.at) };
  });
}

async function enqueueThree(sinkPath: string): Promise<void> {
  for (const marker of ["one", "two", "three"]) {
    await new ConcurrentJob().performLater({
      sinkPath,
      marker,
      durationMs: 800,
    });
  }
}

async function waitForThreeCompletions(sinkPath: string): Promise<void> {
  await waitFor(
    async () =>
      (await readSink(sinkPath)).filter((record) =>
        record.event === "concurrency_ended"
      ).length === 3,
    "Three concurrency jobs did not complete",
    8_000,
  );
}

integrationTest("C5 concurrency three jobs overlap", async () => {
  await runIntegration("c5_overlap", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend, { concurrency: 3 });
    await hermes.start();

    const startedAt = performance.now();
    await enqueueThree(scope.sinkPath);
    await waitForThreeCompletions(scope.sinkPath);
    const wallMs = performance.now() - startedAt;
    const windows = await executionWindows(scope.sinkPath);

    assert(wallMs < 1_600, `Expected overlap, observed ${wallMs}ms wall time`);
    assert(
      Math.max(...windows.map((window) => window.start)) <
        Math.min(...windows.map((window) => window.end)),
    );
  });
});

integrationTest("concurrency one serializes three jobs", async () => {
  await runIntegration("concurrency_serial", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(ordinaryManifest, backend, { concurrency: 1 });
    await hermes.start();
    await enqueueThree(scope.sinkPath);
    await waitForThreeCompletions(scope.sinkPath);

    const windows = await executionWindows(scope.sinkPath);
    assertEquals(windows[0].end <= windows[1].start, true);
    assertEquals(windows[1].end <= windows[2].start, true);
  });
});

integrationTest(
  "worker concurrency overrides backend concurrency",
  async () => {
    await runIntegration("concurrency_precedence", async (scope) => {
      const backend = scope.backend({ concurrency: 3 });
      const hermes = scope.hermes(ordinaryManifest, backend, {
        concurrency: 1,
      });
      await hermes.start();
      await enqueueThree(scope.sinkPath);
      await waitForThreeCompletions(scope.sinkPath);

      const windows = await executionWindows(scope.sinkPath);
      assertEquals(windows[0].end <= windows[1].start, true);
      assertEquals(windows[1].end <= windows[2].start, true);
    });
  },
);
