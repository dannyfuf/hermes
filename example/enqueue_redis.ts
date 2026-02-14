import { configure } from "@hermes";
import { BullMQBackend } from "../src/lib/backends/bullmq.ts";
import { ExampleJob } from "./jobs/example_job.ts";

configure({
  backend: BullMQBackend({
    connection: { host: "localhost", port: 6379 },
  }),
});

const job = new ExampleJob();
await job.performLater({ message: "hello from redis" });
console.log("Job enqueued to Redis");
