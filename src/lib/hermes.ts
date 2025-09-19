import { JobLoader } from "./job_loader.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { Worker } from "./worker.ts";
import type { HermesParams, WorkerConfig } from "./types.ts";

class THermes {
  manifest: string;
  worker: WorkerConfig;

  constructor(
    { manifest, worker }: HermesParams,
  ) {
    this.manifest = manifest;
    this.worker = worker;
  }

  async start() {
    const job_manifest = await ManifestLoader.load({
      manifestPath: this.manifest,
    });
    const jobs_hash = await JobLoader(job_manifest).run();
    await Worker.start({ jobs_hash });
  }
}

export const Hermes = ({ manifest, worker }: HermesParams) => {
  return new THermes({ manifest, worker });
};
