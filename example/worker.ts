import { DenoKvBackend, Hermes } from "@hermes";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  backend: DenoKvBackend(),
  worker: { gracefulShutdownTimeout: 1000 },
});

await hermes.start();
console.log("Hermes is running (Deno KV backend)");
