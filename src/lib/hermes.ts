import { JobLoader } from "./job_loader.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { resolveJobTimeouts, Worker } from "./worker.ts";
import { setBackend } from "./backend_registry.ts";
import type { BackendAdapter } from "./backend.ts";
import type { HermesParams } from "./types.ts";
import { Logger } from "./logger.ts";

/** A running Hermes instance that manages job workers. */
export interface HermesInstance {
  start(): Promise<void>;
  stop(): Promise<void>;
}

class THermes implements HermesInstance {
  private params: HermesParams;

  constructor(params: HermesParams) {
    this.params = params;
    setBackend(params.backend);
  }

  async start(): Promise<void> {
    const jobManifest = await ManifestLoader.load({
      manifestPath: this.params.manifest,
    });
    const jobsMap = await JobLoader(jobManifest).run();
    const timeoutByJobName = resolveJobTimeouts(
      jobsMap,
      this.params.worker?.defaultJobTimeout,
    );

    // Extract unique queue names from registered jobs for BullMQ routing
    const queueNames = [
      ...new Set(
        // deno-lint-ignore no-explicit-any
        Array.from(jobsMap.values()).map((cls: any) =>
          new cls().queueName as string
        ),
      ),
    ];

    // Register recurring jobs if backend supports it
    if (this.params.backend.registerRecurringJob) {
      for (const [_, jobClass] of jobsMap) {
        const instance = new jobClass();
        if (instance.isRecurring()) {
          if (instance.every && instance.cron) {
            throw new Error(
              `Job "${instance.jobName}" cannot have both 'every' and 'cron'`,
            );
          }
          await this.params.backend.registerRecurringJob({
            jobName: instance.jobName,
            queueName: instance.queueName,
            every: instance.every,
            cron: instance.cron,
          });
          Logger.recurringJobRegistered(
            instance.jobName,
            instance.every || instance.cron!,
          );
        }
      }
    }

    await Worker.start({
      jobsMap,
      backend: this.params.backend,
      queueNames,
      timeoutByJobName,
    });
  }

  async stop(): Promise<void> {
    await this.params.backend.close();
  }
}

/** Create a new Hermes instance with the given backend and job manifest. */
export const Hermes = (params: HermesParams): HermesInstance => {
  return new THermes(params);
};

/** Configure the global backend adapter for standalone job enqueueing without starting a worker. */
export const configure = (
  { backend }: { backend: BackendAdapter },
): void => {
  setBackend(backend);
};
