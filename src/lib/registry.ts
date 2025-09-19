import { Job } from "./job.ts";
import { JobConstructor, JobManifest } from "./types.ts";

export class JobRegistry {
  private jobs = new Map<string, JobConstructor>();

  register(jobClasses: JobManifest): void {
    this.validateManifest(jobClasses);

    for (const JobClass of jobClasses) {
      const instance = new JobClass();

      if (this.jobs.has(instance.job_name)) {
        throw new Error(
          `Duplicate job_name "${instance.job_name}" found. Job names must be unique across all registered jobs.`,
        );
      }

      this.jobs.set(instance.job_name, JobClass);
    }

    if (jobClasses.length === 0) {
      console.warn("Warning: No jobs registered in the manifest.");
    }
  }

  get(jobName: string): JobConstructor | undefined {
    return this.jobs.get(jobName);
  }

  has(jobName: string): boolean {
    return this.jobs.has(jobName);
  }

  getAllJobNames(): string[] {
    return Array.from(this.jobs.keys());
  }

  size(): number {
    return this.jobs.size;
  }

  private validateManifest(jobClasses: JobManifest): void {
    if (!Array.isArray(jobClasses)) {
      throw new Error(
        "Jobs manifest must export an array of Job classes. Expected array, got: " +
          typeof jobClasses,
      );
    }

    for (let i = 0; i < jobClasses.length; i++) {
      const JobClass = jobClasses[i];

      if (typeof JobClass !== "function") {
        throw new Error(
          `Invalid job class at index ${i}. Expected a constructor function, got: ${typeof JobClass}`,
        );
      }

      let instance: Job;
      try {
        instance = new JobClass();
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        throw new Error(
          `Failed to instantiate job class at index ${i}: ${errorMessage}`,
        );
      }

      if (!this.isValidJob(instance)) {
        throw new Error(
          `Job class at index ${i} does not properly extend the Job base class. ` +
            `Missing required properties: job_name, queue_name, or perform method.`,
        );
      }

      if (!instance.job_name || typeof instance.job_name !== "string") {
        throw new Error(
          `Job at index ${i} has invalid job_name. Expected non-empty string, got: ${instance.job_name}`,
        );
      }

      if (!instance.queue_name || typeof instance.queue_name !== "string") {
        throw new Error(
          `Job "${instance.job_name}" has invalid queue_name. Expected non-empty string, got: ${instance.queue_name}`,
        );
      }
    }
  }

  private isValidJob(obj: unknown): obj is Job {
    return (
      obj !== null &&
      typeof obj === "object" &&
      "job_name" in obj &&
      "queue_name" in obj &&
      "perform" in obj &&
      typeof obj.perform === "function"
    );
  }
}
