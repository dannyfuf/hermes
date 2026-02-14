// deno-lint-ignore-file no-explicit-any

class TJobLoader {
  jobsMap: Map<string, any>;
  manifest: any;

  constructor(manifest: any) {
    this.jobsMap = new Map<string, any>();
    this.manifest = manifest;
  }

  // deno-lint-ignore require-await
  async run(): Promise<Map<string, any>> {
    for (const job of this.manifest) {
      const jobName = (new job()).jobName;
      this.validateJobUniqueness(jobName);

      this.jobsMap.set(jobName, job);
    }

    return this.jobsMap;
  }

  validateJobUniqueness(jobName: string): void {
    if (this.jobsMap.has(jobName)) {
      throw new Error(
        `Duplicate jobName "${jobName}" found. ` +
          `Job names must be unique across all registered jobs.`,
      );
    }
  }
}

export const JobLoader = (manifest: any): TJobLoader => {
  return new TJobLoader(manifest);
};
