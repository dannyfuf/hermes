import { JobManifest, WorkerConfig } from "./types.ts";

export interface HermesConfig {
  manifest: JobManifest | string;
  worker?: WorkerConfig;
}

export class ConfigValidator {
  static validate(config: HermesConfig): void {
    if (!config) {
      throw new Error("Configuration is required");
    }

    if (!config.manifest) {
      throw new Error(
        "Configuration must include a manifest (array of Job classes or module path)",
      );
    }

    if (config.worker) {
      this.validateWorkerConfig(config.worker);
    }
  }

  private static validateWorkerConfig(config: WorkerConfig): void {
    if (config.includeQueues && config.excludeQueues) {
      throw new Error(
        "Cannot specify both includeQueues and excludeQueues. Use one or the other.",
      );
    }

    if (config.includeQueues && !Array.isArray(config.includeQueues)) {
      throw new Error("includeQueues must be an array of strings");
    }

    if (config.excludeQueues && !Array.isArray(config.excludeQueues)) {
      throw new Error("excludeQueues must be an array of strings");
    }

    if (config.includeQueues) {
      for (const queue of config.includeQueues) {
        if (typeof queue !== "string" || !queue.trim()) {
          throw new Error(
            "All queue names in includeQueues must be non-empty strings",
          );
        }
      }
    }

    if (config.excludeQueues) {
      for (const queue of config.excludeQueues) {
        if (typeof queue !== "string" || !queue.trim()) {
          throw new Error(
            "All queue names in excludeQueues must be non-empty strings",
          );
        }
      }
    }

    if (config.concurrency !== undefined) {
      if (!Number.isInteger(config.concurrency) || config.concurrency < 1) {
        throw new Error("concurrency must be a positive integer");
      }
    }

    if (config.gracefulShutdownTimeout !== undefined) {
      if (
        !Number.isInteger(config.gracefulShutdownTimeout) ||
        config.gracefulShutdownTimeout < 0
      ) {
        throw new Error(
          "gracefulShutdownTimeout must be a non-negative integer",
        );
      }
    }
  }

  static getDefaultWorkerConfig(): WorkerConfig & {
    concurrency: number;
    gracefulShutdownTimeout: number;
  } {
    return {
      concurrency: 1,
      gracefulShutdownTimeout: 30000, // 30 seconds
    };
  }
}
