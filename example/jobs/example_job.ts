import { Job } from "@hermes";

export class ExampleJob extends Job {
  jobName = "job_example";
  queueName = "default";

  async perform() {
    await new Promise((resolve) => {
      console.log("This is an example job.");
      resolve(true);
    });
  }
}
