import { Job } from "@hermes";

export class ExampleJob extends Job {
  job_name = "job_example";
  queue_name = "default";

  async perform() {
    await new Promise((resolve) => {
      console.log("This is an example job.");
      resolve(true);
    });
  }
}
