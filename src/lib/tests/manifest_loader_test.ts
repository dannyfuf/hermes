import { assertEquals, assertRejects } from "@std/assert";
import { ManifestLoader } from "../manifest_loader.ts";

Deno.test("ManifestLoader", async (t) => {
  await t.step("throws on missing file", async () => {
    await assertRejects(
      () =>
        ManifestLoader.load({
          manifestPath: "./nonexistent/path/manifest.ts",
        }),
      Error,
    );
  });

  await t.step("loads manifest with default export", async () => {
    const manifest = await ManifestLoader.load({
      manifestPath: "./src/lib/tests/helpers/fixtures/valid_manifest.ts",
    });

    assertEquals(Array.isArray(manifest), true);
    assertEquals(manifest.length, 2);
  });

  await t.step("loads manifest with named 'jobs' export", async () => {
    const manifest = await ManifestLoader.load({
      manifestPath: "./src/lib/tests/helpers/fixtures/valid_manifest_named.ts",
    });

    assertEquals(Array.isArray(manifest), true);
    assertEquals(manifest.length, 1);
  });

  await t.step("throws if export is not an array", async () => {
    await assertRejects(
      () =>
        ManifestLoader.load({
          manifestPath:
            "./src/lib/tests/helpers/fixtures/non_array_manifest.ts",
        }),
      Error,
      "must export an array of Job classes",
    );
  });

  await t.step(
    "throws if no default or jobs export",
    async () => {
      await assertRejects(
        () =>
          ManifestLoader.load({
            manifestPath:
              "./src/lib/tests/helpers/fixtures/no_export_manifest.ts",
          }),
        Error,
        "must export either a default export or named export",
      );
    },
  );
});
