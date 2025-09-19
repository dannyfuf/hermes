import { createAndStartHermes } from "../src/main.ts";

const hermes = await createAndStartHermes({
  manifest: "./jobs/main.ts",
  worker: {
    includeQueues: ["emails", "reports"],
    concurrency: 2,
    gracefulShutdownTimeout: 10000,
  },
});

console.log("Worker started. Status:", hermes.getStatus());

Deno.addSignalListener("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully...");
  await hermes.stop();
  Deno.exit(0);
});

Deno.addSignalListener("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully...");
  await hermes.stop();
  Deno.exit(0);
});

