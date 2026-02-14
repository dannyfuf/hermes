import { assertEquals, assertThrows } from "@std/assert";
import { clearBackend, getBackend, setBackend } from "../backend_registry.ts";
import { MockBackend } from "./helpers/mock_backend.ts";

Deno.test("backend_registry", async (t) => {
  await t.step("getBackend() throws if no backend configured", () => {
    clearBackend();
    assertThrows(
      () => getBackend(),
      Error,
      "No backend configured",
    );
  });

  await t.step(
    "setBackend() followed by getBackend() returns the backend",
    () => {
      clearBackend();
      const backend = new MockBackend();
      setBackend(backend);
      assertEquals(getBackend(), backend);
    },
  );

  await t.step("setBackend() replaces previous backend", () => {
    clearBackend();
    const first = new MockBackend();
    const second = new MockBackend();
    setBackend(first);
    setBackend(second);
    assertEquals(getBackend(), second);
  });

  await t.step("clearBackend() removes configured backend", () => {
    const backend = new MockBackend();
    setBackend(backend);
    clearBackend();
    assertThrows(
      () => getBackend(),
      Error,
      "No backend configured",
    );
  });
});
