import { join } from "@std/path";
import { toFileUrl } from "@std/path";

export class ManifestLoader {
  static async load(
    { manifestPath }: { manifestPath: string },
  ) {
    const resolved = this.resolvePath(manifestPath);
    const manifestModule = await import(resolved).catch(
      (error) => this.handleErrorNotFound(error, manifestPath),
    );
    this.validateManifestModule(manifestModule, manifestPath);

    const manifest = manifestModule.default || manifestModule.jobs;
    this.validateManifestType(manifest, manifestPath);

    return manifest;
  }

  private static validateManifestModule(
    // deno-lint-ignore no-explicit-any
    manifestModule: any,
    manifestPath: string,
  ): void {
    if (!manifestModule.default && !manifestModule.jobs) {
      throw new Error(
        `Job manifest at "${manifestPath}" ` +
          `must export either a default export or named export "jobs" ` +
          `containing an array of Job classes.`,
      );
    }
  }

  private static validateManifestType(
    // deno-lint-ignore no-explicit-any
    manifest: any,
    manifestPath: string,
  ): void {
    if (!Array.isArray(manifest)) {
      throw new Error(
        `Job manifest at "${manifestPath}" ` +
          `must export an array of Job classes. Got: ${typeof manifest}`,
      );
    }
  }

  private static resolvePath(manifestPath: string): string {
    if (
      manifestPath.startsWith("http://") ||
      manifestPath.startsWith("https://") || manifestPath.startsWith("file://")
    ) {
      return manifestPath;
    }

    if (
      manifestPath.startsWith("./") || manifestPath.startsWith("../") ||
      !manifestPath.startsWith("/")
    ) {
      const absolutePath = join(Deno.cwd(), manifestPath);
      return toFileUrl(absolutePath).href;
    }

    return toFileUrl(manifestPath).href;
  }

  private static handleErrorNotFound(
    error: Error,
    manifestPath: string,
  ): never {
    const errorMessage = error.message;
    if (errorMessage.includes("Module not found")) {
      throw new Error(
        `Job manifest not found at "${manifestPath}". ` +
          `Ensure the file exists and exports an array of Job classes.`,
      );
    }

    throw error;
  }
}
