import { assertEquals } from "@std/assert";
import {
  clearHooks,
  clearLoggerSink,
  getHooks,
  getLoggerSink,
  setHooks,
  setLoggerSink,
} from "../hooks_registry.ts";
import { clearBackend } from "../backend_registry.ts";
import { configure, Hermes } from "../hermes.ts";
import { MockBackend } from "./helpers/mock_backend.ts";
import type { HermesHooks, LoggerSink } from "../types.ts";

function clearRegistries(): void {
  clearBackend();
  clearHooks();
  clearLoggerSink();
}

Deno.test("hooks registry", async (t) => {
  await t.step("starts empty", () => {
    clearRegistries();
    assertEquals(getHooks(), null);
    assertEquals(getLoggerSink(), null);
  });

  await t.step("set/get/clear hooks", () => {
    clearRegistries();
    const hooks: HermesHooks = { enqueueMetadata: () => ({ a: 1 }) };
    setHooks(hooks);
    assertEquals(getHooks(), hooks);
    clearHooks();
    assertEquals(getHooks(), null);
  });

  await t.step("set/get/clear logger sink", () => {
    clearRegistries();
    const sink: LoggerSink = () => {};
    setLoggerSink(sink);
    assertEquals(getLoggerSink(), sink);
    clearLoggerSink();
    assertEquals(getLoggerSink(), null);
  });

  await t.step("Hermes() registers hooks and logger at construction", () => {
    clearRegistries();
    const hooks: HermesHooks = {
      aroundPerform: async (_p, next) => void await next(),
    };
    const sink: LoggerSink = () => {};

    Hermes({
      manifest: "./unused_manifest.ts",
      backend: new MockBackend(),
      hooks,
      logger: sink,
    });

    assertEquals(getHooks(), hooks);
    assertEquals(getLoggerSink(), sink);
    clearRegistries();
  });

  await t.step("configure() registers hooks and logger", () => {
    clearRegistries();
    const hooks = { enqueueMetadata: () => ({ traceId: "t" }) };
    const sink: LoggerSink = () => {};

    configure({ backend: new MockBackend(), hooks, logger: sink });

    assertEquals(getHooks(), hooks);
    assertEquals(getLoggerSink(), sink);
    clearRegistries();
  });

  await t.step(
    "registration replaces: omitted hooks/logger clears previous ones",
    () => {
      clearRegistries();
      configure({
        backend: new MockBackend(),
        hooks: { enqueueMetadata: () => ({ a: 1 }) },
        logger: () => {},
      });

      configure({ backend: new MockBackend() });

      assertEquals(getHooks(), null);
      assertEquals(getLoggerSink(), null);
      clearRegistries();
    },
  );

  await t.step(
    "registration replaces: a new registration overwrites the old one",
    () => {
      clearRegistries();
      const first: HermesHooks = { enqueueMetadata: () => ({ a: 1 }) };
      const second: HermesHooks = { enqueueMetadata: () => ({ b: 2 }) };
      configure({ backend: new MockBackend(), hooks: first });
      configure({ backend: new MockBackend(), hooks: second });

      assertEquals(getHooks(), second);
      clearRegistries();
    },
  );
});
