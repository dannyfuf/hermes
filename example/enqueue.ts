import { configure, DenoKvBackend } from "@hermes";
import { ExampleJob } from "./jobs/example_job.ts";

configure({ backend: DenoKvBackend() });

const job = new ExampleJob();
await job.performLater({ message: "hello" });
console.log("Job enqueued (Deno KV backend)");
