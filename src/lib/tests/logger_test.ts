import { assert, assertEquals } from "@std/assert";
import { resolveJobTimeouts, Worker } from "../worker.ts";
import { Job } from "../job.ts";
import { clearBackend } from "../backend_registry.ts";
import {
  clearHooks,
  clearLoggerSink,
  setLoggerSink,
} from "../hooks_registry.ts";
import { configure } from "../hermes.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import { Logger } from "../logger.ts";
import type { LogEvent } from "../logger.ts";

class EchoJob extends Job {
  readonly jobName = "echo_job";
  readonly queueName = "default";

  async perform(): Promise<unknown> {
    return await Promise.resolve("ok");
  }
}

class EchoFailingJob extends Job {
  readonly jobName = "echo_failing_job";
  readonly queueName = "default";

  perform(): Promise<unknown> {
    return Promise.reject(new Error("echo failure"));
  }
}

async function startWorker(
  // deno-lint-ignore no-explicit-any
  jobsMap: Map<string, any>,
): Promise<MockBackend> {
  const backend = new MockBackend();
  await Worker.start({
    jobsMap,
    backend,
    timeoutByJobName: resolveJobTimeouts(jobsMap),
  });
  return backend;
}

function captureConsole(): {
  lines: Record<string, unknown>[];
  restore: () => void;
} {
  const lines: Record<string, unknown>[] = [];
  const originalLog = console.log;
  console.log = (msg: string) => {
    try {
      lines.push(JSON.parse(msg));
    } catch { /* ignore non-JSON */ }
  };
  return {
    lines,
    restore: () => {
      console.log = originalLog;
    },
  };
}

Deno.test("logger sink", async (t) => {
  await t.step(
    "a configured sink receives every structured event; console stays silent",
    async () => {
      clearLoggerSink();
      const events: LogEvent[] = [];
      setLoggerSink((event) => events.push(event));
      const captured = captureConsole();

      try {
        // deno-lint-ignore no-explicit-any
        const backend = await startWorker(
          new Map<string, any>([["echo_job", EchoJob]]),
        );
        await backend.process({
          jobName: "echo_job",
          queueName: "default",
          jobBody: null,
        });

        const names = events.map((e) => e.event);
        assert(names.includes("worker_started"));
        assert(names.includes("job_received"));
        assert(names.includes("job_started"));
        assert(names.includes("job_succeeded"));
        assertEquals(captured.lines.length, 0);
      } finally {
        captured.restore();
        clearLoggerSink();
      }
    },
  );

  await t.step(
    "a throwing sink falls back to console, emits one logger_error, and dispatch continues",
    async () => {
      clearLoggerSink();
      let sinkCalls = 0;
      setLoggerSink(() => {
        sinkCalls++;
        throw new Error("sink is broken");
      });
      const captured = captureConsole();

      try {
        // deno-lint-ignore no-explicit-any
        const backend = await startWorker(
          new Map<string, any>([["echo_job", EchoJob]]),
        );
        const before = captured.lines.length;
        await backend.process({
          jobName: "echo_job",
          queueName: "default",
          jobBody: null,
        });

        // Dispatch survived a sink that throws on every event; each event
        // reached console followed by exactly one logger_error breadcrumb.
        const during = captured.lines.slice(before);
        const originals = during.filter((e) => e.event !== "logger_error");
        const breadcrumbs = during.filter((e) => e.event === "logger_error");
        assertEquals(
          originals.map((e) => e.event),
          ["job_received", "job_started", "job_succeeded"],
        );
        assertEquals(breadcrumbs.length, originals.length);
        for (const breadcrumb of breadcrumbs) {
          assertEquals(breadcrumb.error, "sink is broken");
        }
        // The sink was called once per original event — never re-invoked
        // for the fallback or the breadcrumb (no recursion).
        const originalsAll = captured.lines.filter(
          (e) => e.event !== "logger_error",
        );
        assertEquals(sinkCalls, originalsAll.length);

        // Subsequent jobs still process.
        await backend.process({
          jobName: "echo_job",
          queueName: "default",
          jobBody: null,
        });
        assert(
          captured.lines.filter((e) => e.event === "job_succeeded").length >= 2,
        );
      } finally {
        captured.restore();
        clearLoggerSink();
      }
    },
  );

  await t.step(
    "metadata is echoed on all four job lifecycle events iff the payload carries it",
    async () => {
      clearLoggerSink();
      const events: LogEvent[] = [];
      setLoggerSink((event) => events.push(event));

      try {
        // deno-lint-ignore no-explicit-any
        const backend = await startWorker(
          // deno-lint-ignore no-explicit-any
          new Map<string, any>([
            ["echo_job", EchoJob],
            ["echo_failing_job", EchoFailingJob],
          ]),
        );

        const metadata = { traceId: "trace-9" };
        await backend.process({
          jobName: "echo_job",
          queueName: "default",
          jobBody: null,
          metadata,
        });
        await backend
          .process({
            jobName: "echo_failing_job",
            queueName: "default",
            jobBody: null,
            metadata,
          })
          .catch(() => {/* expected */});

        const withMetadata = events.filter((e) => e.metadata !== undefined);
        assertEquals(
          withMetadata.map((e) => e.event).sort(),
          [
            "job_failed",
            "job_received",
            "job_received",
            "job_started",
            "job_started",
            "job_succeeded",
          ].sort(),
        );
        for (const event of withMetadata) {
          assertEquals(event.metadata, metadata);
        }

        // Without payload metadata the field is absent, keeping event shapes
        // byte-identical to previous versions.
        events.length = 0;
        await backend.process({
          jobName: "echo_job",
          queueName: "default",
          jobBody: null,
        });
        for (const event of events) {
          assertEquals("metadata" in event, false);
        }
      } finally {
        clearLoggerSink();
      }
    },
  );

  await t.step(
    "a sink registered via configure() routes events in enqueue-only processes",
    () => {
      clearBackend();
      clearHooks();
      clearLoggerSink();
      const events: LogEvent[] = [];
      const captured = captureConsole();

      try {
        configure({
          backend: new MockBackend(),
          logger: (event) => events.push(event),
        });

        Logger.error("something enqueue-side", "boom");

        assertEquals(events.length, 1);
        assertEquals(events[0].event, "error");
        assertEquals(captured.lines.length, 0);
      } finally {
        captured.restore();
        clearBackend();
        clearHooks();
        clearLoggerSink();
      }
    },
  );

  await t.step("no sink configured: console output is unchanged", () => {
    clearLoggerSink();
    const captured = captureConsole();
    try {
      Logger.info("plain console path");
      assertEquals(captured.lines.length, 1);
      assertEquals(captured.lines[0].event, "info");
      assertEquals(captured.lines[0].message, "plain console path");
    } finally {
      captured.restore();
    }
  });
});
