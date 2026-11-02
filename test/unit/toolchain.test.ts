import { expect, test } from "bun:test";

test("runtime, CI and package manager use the same Bun release with native checking", async () => {
  const pkg = await Bun.file(new URL("../../package.json", import.meta.url)).json();
  const version = pkg.packageManager.replace(/^bun@/, "");
  const docker = await Bun.file(new URL("../../Dockerfile", import.meta.url)).text();
  const workflow = await Bun.file(new URL("../../.github/workflows/docker.yml", import.meta.url)).text();
  expect(docker).toContain(`FROM oven/bun:${version}`);
  expect(workflow).toContain(`bun-version: ${version}`);
  expect(workflow).toContain("needs: verify");
  expect(pkg.scripts.typecheck).toBe("bun check");
});
