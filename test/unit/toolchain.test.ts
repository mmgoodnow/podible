import { expect, test } from "bun:test";

type WorkflowEnv = Record<string, string | number | boolean>;
type Workflow = {
  env?: WorkflowEnv;
  jobs: Record<string, {
    env?: WorkflowEnv;
    needs?: string | string[];
    steps?: { uses?: string; env?: WorkflowEnv; with?: { "bun-version"?: string } }[];
  }>;
};

test("runtime, CI and package manager use the same Bun release with native checking", async () => {
  const pkg = await Bun.file(new URL("../../package.json", import.meta.url)).json();
  const version = pkg.packageManager.replace(/^bun@/, "");
  const docker = await Bun.file(new URL("../../Dockerfile", import.meta.url)).text();
  const workflow = Bun.YAML.parse(await Bun.file(new URL("../../.github/workflows/docker.yml", import.meta.url)).text()) as Workflow;
  const images = [...docker.matchAll(/^FROM oven\/bun:([^\s]+)/gm)];
  expect(images.length).toBeGreaterThan(0);
  for (const image of images) expect(image[1]).toBe(version);
  let setups = 0;
  for (const job of Object.values(workflow.jobs)) for (const step of job.steps ?? []) {
    if (!step.uses?.startsWith("oven-sh/setup-bun@")) continue;
    setups++;
    const env = { ...workflow.env, ...job.env, ...step.env };
    const configured = step.with?.["bun-version"];
    expect(typeof configured).toBe("string");
    const resolved = configured!.replace(/\$\{\{\s*env\.([a-zA-Z_][a-zA-Z_0-9]*)\s*\}\}/g, (_expression, name: string) => {
      expect(env[name]).toBeDefined();
      return String(env[name]);
    });
    expect(resolved).toBe(version);
  }
  expect(setups).toBeGreaterThan(0);
  const unitSteps = workflow.jobs.unit?.steps ?? [];
  const commands = unitSteps.map(step => "run" in step ? String(step.run).trim() : "");
  const buildIndex = commands.indexOf("bun run build");
  const testIndex = commands.indexOf("bun test");
  expect(buildIndex).toBeGreaterThanOrEqual(0);
  expect(testIndex).toBeGreaterThan(buildIndex);
  const needs = workflow.jobs.docker?.needs;
  expect(Array.isArray(needs) ? needs : [needs]).toEqual(["verify"]);
  expect(pkg.scripts.typecheck).toBe("bun check && bun check -p web");
});
