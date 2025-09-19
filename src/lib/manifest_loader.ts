import { JobManifest } from "./types.ts";

export class ManifestLoader {
  static async loadFromModule(moduleSpecifier: string): Promise<JobManifest> {
    try {
      const module = await import(moduleSpecifier);

      if (!module.default && !module.jobs) {
        throw new Error(
          `Jobs manifest at "${moduleSpecifier}" must export either a default export or named export "jobs" containing an array of Job classes.`,
        );
      }

      const manifest = module.default || module.jobs;

      if (!Array.isArray(manifest)) {
        throw new Error(
          `Jobs manifest at "${moduleSpecifier}" must export an array of Job classes. Got: ${typeof manifest}`,
        );
      }

      return manifest;
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      if (errorMessage.includes("Module not found")) {
        throw new Error(
          `Jobs manifest not found at "${moduleSpecifier}". Ensure the file exists and exports an array of Job classes.`,
        );
      }
      throw error;
    }
  }

  static validateManifestPath(path: string): void {
    if (!path || typeof path !== "string") {
      throw new Error("Manifest path must be a non-empty string");
    }

    if (!path.endsWith(".ts") && !path.endsWith(".js")) {
      throw new Error("Manifest path must point to a .ts or .js file");
    }
  }
}
