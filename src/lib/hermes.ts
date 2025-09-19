import { JobRegistry } from "./registry.ts";
import { ManifestLoader } from "./manifest_loader.ts";
import { Worker } from "./worker.ts";
import { Logger } from "./logger.ts";
import { ConfigValidator, HermesConfig } from "./config.ts";
import { JobManifest, WorkerConfig } from "./types.ts";

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export class Hermes {
  private registry: JobRegistry;
  private worker: Worker | null = null;
  private config: HermesConfig;

  constructor(config: HermesConfig) {
    ConfigValidator.validate(config);
    this.config = config;
    this.registry = new JobRegistry();
  }

  async initialize(): Promise<void> {
    try {
      const manifest = await this.loadManifest();
      this.registry.register(manifest);

      const workerConfig = {
        ...ConfigValidator.getDefaultWorkerConfig(),
        ...this.config.worker,
      };

      this.worker = new Worker(this.registry, workerConfig);

      Logger.info("Hermes initialized successfully", {
        registered_jobs: this.registry.size(),
        job_names: this.registry.getAllJobNames(),
      });
    } catch (error) {
      Logger.error("Failed to initialize Hermes", getErrorMessage(error));
      throw error;
    }
  }

  async start(): Promise<void> {
    if (!this.worker) {
      throw new Error(
        "Hermes must be initialized before starting. Call initialize() first.",
      );
    }

    if (this.worker.isActive()) {
      throw new Error("Worker is already running");
    }

    try {
      await this.worker.start();
      Logger.info("Hermes worker started successfully");
    } catch (error) {
      Logger.error("Failed to start Hermes worker", getErrorMessage(error));
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.worker) {
      Logger.warn("No worker to stop - Hermes was not initialized");
      return;
    }

    if (!this.worker.isActive()) {
      Logger.warn("Worker is not running");
      return;
    }

    try {
      await this.worker.stop();
      Logger.info("Hermes worker stopped successfully");
    } catch (error) {
      Logger.error("Error stopping Hermes worker", getErrorMessage(error));
      throw error;
    }
  }

  getStatus(): {
    initialized: boolean;
    running: boolean;
    registeredJobs: number;
    activeJobs: number;
    jobNames: string[];
  } {
    return {
      initialized: this.worker !== null,
      running: this.worker?.isActive() ?? false,
      registeredJobs: this.registry.size(),
      activeJobs: this.worker?.getActiveJobCount() ?? 0,
      jobNames: this.registry.getAllJobNames(),
    };
  }

  private async loadManifest(): Promise<JobManifest> {
    if (Array.isArray(this.config.manifest)) {
      return this.config.manifest;
    }

    if (typeof this.config.manifest === "string") {
      ManifestLoader.validateManifestPath(this.config.manifest);
      return await ManifestLoader.loadFromModule(this.config.manifest);
    }

    throw new Error(
      "Manifest must be either an array of Job classes or a string path to a module",
    );
  }
}

export async function createHermes(config: HermesConfig): Promise<Hermes> {
  const hermes = new Hermes(config);
  await hermes.initialize();
  return hermes;
}

export async function createAndStartHermes(
  config: HermesConfig,
): Promise<Hermes> {
  const hermes = await createHermes(config);
  await hermes.start();
  return hermes;
}
