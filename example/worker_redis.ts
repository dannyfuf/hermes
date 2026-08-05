import { Hermes } from "@hermes";
import { BullMQBackend } from "../src/lib/backends/bullmq.ts";

const hermes = Hermes({
  manifest: "./example/jobs/main.ts",
  backend: BullMQBackend({
    connection: { host: "localhost", port: 6379 },
    concurrency: 5,
  }),
});

await hermes.start();
console.log("Hermes is running (Redis/BullMQ backend)");

const shutdown = async (): Promise<void> => {
  await hermes.stop();
  Deno.exit(0);
};

Deno.addSignalListener("SIGINT", shutdown);
Deno.addSignalListener("SIGTERM", shutdown);
