import type { WorkerParams } from "./types.ts";

export class Worker {
  static async start({ jobs_hash }: WorkerParams) {
    const kv = await Deno.openKv();

    kv.listenQueue(async ({ job_name, job_body }) => {
      const job_class = jobs_hash.get(job_name);
      this.validateJob(job_class);

      const job = new job_class();
      await job.perform(job_body);
    });
  }

  private static validateJob(job_class?: unknown) {
    if (!job_class) {
      throw new Error("Job is not defined");
    }
  }
}
