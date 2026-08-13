import { assert, assertEquals, assertRejects } from "@std/assert";
import { configure, Job } from "../../../main.ts";
import type { LogEvent } from "../../../main.ts";
import {
  BlockingJob,
  ChainParentJob,
  EchoJob,
  FlakyJob,
  MetadataEchoJob,
  PrimaryQueueJob,
  SecondaryQueueJob,
  TimedHangsJob,
} from "./fixtures/integration_jobs.ts";
import { readSink, waitFor, waitForElapsed } from "./helpers/integration.ts";
import { kvTest, runKv } from "./helpers/kv.ts";

const echoManifest =
  new URL("./fixtures/echo_manifest.ts", import.meta.url).href;
const multiQueueManifest = new URL(
  "./fixtures/multi_queue_manifest.ts",
  import.meta.url,
).href;
const hooksManifest = new URL(
  "./fixtures/hooks_manifest.ts",
  import.meta.url,
).href;
const ordinaryManifest = new URL(
  "./fixtures/ordinary_manifest.ts",
  import.meta.url,
).href;
const chainManifest = new URL(
  "./fixtures/chain_manifest.ts",
  import.meta.url,
).href;
const unsupportedIntervalManifest = new URL(
  "./fixtures/unsupported_interval_manifest.ts",
  import.meta.url,
).href;
const recurring120sManifest = new URL(
  "./fixtures/recurring_120s_manifest.ts",
  import.meta.url,
).href;

kvTest("K1 round-trip through the public API on Deno KV", async () => {
  await runKv("k1", async (scope) => {
    const backend = scope.backend();
    const hermes = scope.hermes(echoManifest, backend);
    await hermes.start();

    await new EchoJob().performLater({
      sinkPath: scope.sinkPath,
      marker: "k1-body",
    });
    // Deno KV ignores priorities but must accept the portable API.
    await new EchoJob().performLater(
      { sinkPath: scope.sinkPath, marker: "k1-prioritized" },
      { priority: 3 },
    );

    await waitFor(
      async () => (await readSink(scope.sinkPath)).length === 2,
      "K1 both echo jobs did not reach the sink",
      5_000,
    );
    const markers = (await readSink(scope.sinkPath)).map((r) => r.marker);
    assert(markers.includes("k1-body"));
    assert(markers.includes("k1-prioritized"));
  });
});

kvTest(
  "K2 delayed jobs on Deno KV do not run early and do run by the deadline",
  async () => {
    await runKv("k2", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend);
      await hermes.start();

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k2-delayed",
      }, { delay: 800 });

      await waitForElapsed(300);
      assert(
        !(await readSink(scope.sinkPath)).some((r) =>
          r.marker === "k2-delayed"
        ),
        "K2 delayed job ran within 300ms",
      );
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k2-delayed"
          ),
        "K2 delayed job did not run within 5s",
        5_000,
      );
    });
  },
);

kvTest(
  "K3 body and metadata survive the structured clone through the public API",
  async () => {
    await runKv("k3", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(hooksManifest, backend);
      await hermes.start();

      const metadata = {
        traceId: "k3-trace-こんにちは-🚚",
        nested: { list: [0, -17, 3.14159, true, null, "áéíóú"], empty: "" },
      };
      await new MetadataEchoJob().performLater(
        { sinkPath: scope.sinkPath, marker: "k3-fidelity" },
        { metadata },
      );

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k3-fidelity"
          ),
        "K3 metadata echo job did not complete",
        5_000,
      );
      const record = (await readSink(scope.sinkPath)).find((r) =>
        r.marker === "k3-fidelity"
      );
      assertEquals(record!.metadata, metadata);
    });
  },
);

kvTest(
  "K4 unknown jobs are acknowledged and dropped without poisoning the queue",
  async () => {
    await runKv("k4", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend, {}, {
        logger: (event) => events.push(event),
      });
      await hermes.start();

      class GhostJob extends Job {
        readonly jobName = "ghost";
        readonly queueName = scope.queueName;

        perform(): Promise<void> {
          return Promise.resolve();
        }
      }

      await new GhostJob().performLater();
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k4-after-ghost",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k4-after-ghost"
          ),
        "K4 echo did not process after the unknown job",
        5_000,
      );
      await waitFor(
        () => events.some((e) => e.event === "unknown_job"),
        "K4 unknown_job event was never logged",
        5_000,
      );
      const unknown = events.find((e) => e.event === "unknown_job");
      assertEquals(unknown!.jobName, "ghost");
      // Acknowledged, not retried: exactly one delivery of the ghost.
      assertEquals(
        events.filter((e) => e.event === "unknown_job").length,
        1,
      );
    });
  },
);

kvTest("K5 one Hermes worker routes two queues on Deno KV", async () => {
  await runKv("k5", async (scope) => {
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
        (await readSink(scope.sinkPath)).filter((r) =>
          r.event === "multi_queue"
        ).length === 2,
      "K5 jobs did not complete on both queues",
      5_000,
    );
    const records = await readSink(scope.sinkPath);
    assert(
      records.some((r) =>
        r.marker === "primary" && r.queueName === scope.queueName
      ),
    );
    assert(
      records.some((r) =>
        r.marker === "secondary" && r.queueName === scope.secondaryQueueName
      ),
    );
  });
});

kvTest(
  "K6 a payload for an unowned queue is rejected by queue filtering and other work is unaffected",
  async () => {
    await runKv("k6", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend, {}, {
        logger: (event) => events.push(event),
      });
      await hermes.start();

      class UnownedQueueJob extends Job {
        readonly jobName = "unowned";
        readonly queueName = `${scope.queueName}_nobody`;

        perform(): Promise<void> {
          return Promise.resolve();
        }
      }

      // Deno KV is one global queue filtered at delivery time: this payload
      // reaches the only listener, which rejects it for native retry.
      await new UnownedQueueJob().performLater();
      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k6-owned",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) => r.marker === "k6-owned"),
        "K6 owned-queue job did not process",
        5_000,
      );
      await waitFor(
        () => events.some((e) => e.event === "job_skipped"),
        "K6 job_skipped was never logged for the unowned queue",
        5_000,
      );
      const skipped = events.find((e) => e.event === "job_skipped");
      assertEquals(skipped!.jobName, "unowned");
      assertEquals(skipped!.reason, "queue filtering");
      // The unknown-job path never fired: filtering rejected the payload
      // before dispatch, so it stays eligible for another listener.
      assertEquals(events.some((e) => e.event === "unknown_job"), false);
    });
  },
);

kvTest(
  "K7 a failing job is redelivered by Deno KV's native retry until it succeeds",
  async () => {
    await runKv("k7", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend);
      await hermes.start();

      await new FlakyJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k7-flaky",
      });

      // Worker rethrow → listenQueue rejection → KV backoff redelivery.
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.event === "flaky_success"
          ),
        "K7 flaky job was not redelivered to success by native retry",
        20_000,
      );
      const attempts = (await readSink(scope.sinkPath))
        .filter((r) => r.event === "flaky_attempt")
        .map((r) => r.attempt);
      assertEquals(attempts, [1, 2, 3]);
    });
  },
);

kvTest(
  "K8 a timed-out job on Deno KV fails with JobTimeoutError and later work still runs",
  async () => {
    await runKv("k8", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {}, {
        logger: (event) => events.push(event),
      });
      await hermes.start();

      await new TimedHangsJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k8-hang",
      });

      await waitFor(
        () =>
          events.some((e) =>
            e.event === "job_failed" && e.jobName === "timed_hangs" &&
            String(e.error).includes("timed out after 1000ms")
          ),
        "K8 hung job did not fail with JobTimeoutError",
        8_000,
      );

      await new EchoJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k8-after-timeout",
      });
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k8-after-timeout"
          ),
        "K8 echo did not run after the timed-out job",
        5_000,
      );
    });
  },
);

kvTest(
  "K9 graceful stop on Deno KV drains in-flight work before resolving",
  async () => {
    await runKv("k9", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(ordinaryManifest, backend, {
        gracefulShutdownTimeout: 5_000,
      });
      await hermes.start();

      await new BlockingJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k9-drained",
        durationMs: 600,
      });
      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.event === "blocking_started"
          ),
        "K9 blocking job never started",
        5_000,
      );

      await hermes.stop();

      // stop() resolved only after the in-flight perform drained: the
      // completion record must already be on disk, with no further waiting.
      assert(
        (await readSink(scope.sinkPath)).some((r) =>
          r.event === "blocking_completed" && r.marker === "k9-drained"
        ),
        "K9 stop() resolved before the in-flight job drained",
      );
    });
  },
);

kvTest(
  "K10 an undelivered enqueue survives a backend restart on the same store",
  async () => {
    await runKv("k10", async (scope) => {
      // Producer process: enqueue a delayed job, then shut down before the
      // delay elapses.
      const producer = scope.backend();
      configure({ backend: producer });
      await new EchoJob().performLater(
        { sinkPath: scope.sinkPath, marker: "k10-survivor" },
        { delay: 400 },
      );
      await producer.close();

      // Worker process: a fresh backend on the same store must receive it.
      const consumer = scope.backend();
      const hermes = scope.hermes(echoManifest, consumer);
      await hermes.start();

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k10-survivor"
          ),
        "K10 the enqueued job did not survive the backend restart",
        10_000,
      );
    });
  },
);

kvTest(
  "K11 stats() rejects on Deno KV, which has no stats capability",
  async () => {
    await runKv("k11", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(echoManifest, backend);
      await hermes.start();

      await assertRejects(
        () => hermes.stats(),
        Error,
        "does not support queue stats",
      );
    });
  },
);

kvTest(
  "K12 start() fails fast when a recurring interval is unsupported by Deno KV",
  async () => {
    await runKv("k12", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(unsupportedIntervalManifest, backend);

      await assertRejects(
        () => hermes.start(),
        Error,
        'cannot represent "7m"',
      );

      // A failed start consumes the instance: restarting must not work.
      await assertRejects(() => hermes.start(), Error, "create a new Hermes");
    });
  },
);

kvTest(
  "K13 job→job chains on Deno KV are stamped by the registered enqueueMetadata hook",
  async () => {
    await runKv("k13", async (scope) => {
      const backend = scope.backend();
      const hermes = scope.hermes(chainManifest, backend, {}, {
        hooks: { enqueueMetadata: () => ({ chainStamp: "k13" }) },
      });
      await hermes.start();

      await new ChainParentJob().performLater({
        sinkPath: scope.sinkPath,
        marker: "k13-parent",
        childMarker: "k13-child",
      });

      await waitFor(
        async () =>
          (await readSink(scope.sinkPath)).some((r) =>
            r.marker === "k13-child"
          ),
        "K13 the chained child job did not complete",
        5_000,
      );
      const child = (await readSink(scope.sinkPath)).find((r) =>
        r.marker === "k13-child"
      );
      // The child was enqueued from inside perform(), so the worker-process
      // hook stamped it via the same global registry performLater reads.
      assertEquals(child!.metadata, { chainStamp: "k13" });
    });
  },
);

kvTest(
  "K14 recurring registration reaches Deno.cron through start() and is logged",
  async () => {
    await runKv("k14", async (scope) => {
      const events: LogEvent[] = [];
      const backend = scope.backend();
      const hermes = scope.hermes(recurring120sManifest, backend, {}, {
        logger: (event) => events.push(event),
      });
      await hermes.start();

      const registered = events.find((e) =>
        e.event === "recurring_job_registered"
      );
      assert(registered !== undefined, "K14 recurring job was not registered");
      assertEquals(registered.jobName, "recurring_120s");
      assertEquals(registered.schedule, "120s");
    });
  },
);
