import { ExampleJob } from "./jobs/example_job.ts";

const job = new ExampleJob();

await job.perform_later();
