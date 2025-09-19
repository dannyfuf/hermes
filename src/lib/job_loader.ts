// deno-lint-ignore-file no-explicit-any

class TJobLoader {
  jobs_hash: Map<string, any>;
  manifest: any;

  constructor(manifest: any) {
    this.jobs_hash = new Map<string, any>();
    this.manifest = manifest;
  }

  // deno-lint-ignore require-await
  async run() {
    for (const job of this.manifest) {
      const job_name = (new job()).job_name;
      this.validate_job_uniqueness(job_name);

      this.jobs_hash.set(job_name, job);
    }

    return this.jobs_hash;
  }

  validate_job_uniqueness(job_name: string) {
    if (this.jobs_hash.has(job_name)) {
      throw new Error(
        `Duplicate job_name "${job_name}" found. ` +
          `Job names must be unique across all registered jobs.`,
      );
    }
  }
}

export const JobLoader = (manifest: any) => {
  return new TJobLoader(manifest);
};
