import { DenoKvBackend, Hermes } from "@hermes";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  backend: DenoKvBackend(),
  worker: { gracefulShutdownTimeout: 1000 },
});

await hermes.start();
console.log("Hermes is running (Deno KV backend)");

const shutdown = async (): Promise<void> => {
  await hermes.stop();
  Deno.exit(0);
};

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);
