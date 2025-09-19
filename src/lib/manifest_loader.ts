import { join } from "jsr:@std/path";
import { toFileUrl } from "jsr:@std/path";

export class ManifestLoader {
  static async load(
    { manifestPath }: { manifestPath: string },
  ) {
    const resolvedPath = this.resolvePath(manifestPath);
    const manifest_module = await import(resolvedPath).catch(
      this.handle_error_not_found,
    );
    this.validate_manifest_module(manifest_module, manifestPath);

    const manifest = manifest_module.default || manifest_module.jobs;
    this.validate_manifest_type(manifest, manifestPath);

    return manifest;
  }

  private static validate_manifest_module(
    // deno-lint-ignore no-explicit-any
    manifestModule: any,
    manifestPath: string,
  ) {
    if (!manifestModule.default && !manifestModule.jobs) {
      throw new Error(
        `Job manifest at "${manifestPath}" ` +
          `must export either a default export or named export "jobs" ` +
          `containing an array of Job classes.`,
      );
    }
  }

  // deno-lint-ignore no-explicit-any
  private static validate_manifest_type(manifest: any, manifestPath: string) {
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

  private static handle_error_not_found(error: Error) {
    const errorMessage = error.message;
    if (errorMessage.includes("Module not found")) {
      throw new Error(
        `Job manifest not found at "${error.message}". ` +
          `Ensure the file exists and exports an array of Job classes.`,
      );
    }

    throw error;
  }
}
