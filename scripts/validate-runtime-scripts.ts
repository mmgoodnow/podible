import { access, readFile } from "node:fs/promises";
import path from "node:path";

type PackageJson = {
  scripts?: Record<string, string>;
};

const packageJsonPath = path.resolve("package.json");
const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8")) as PackageJson;
const entrypoints = new Map<string, string[]>();

for (const [name, command] of Object.entries(packageJson.scripts ?? {})) {
  for (const match of command.matchAll(/(?:^|\s)bun run\s+([^\s]+\.(?:ts|js|mjs|cjs))(?=\s|$)/g)) {
    const entrypoint = match[1]!.replace(/^\.\//, "");
    entrypoints.set(entrypoint, [...(entrypoints.get(entrypoint) ?? []), name]);
  }
}

const missing: string[] = [];
for (const [entrypoint, scripts] of entrypoints) {
  try {
    await access(path.resolve(entrypoint));
  } catch {
    missing.push(`${entrypoint} (package scripts: ${scripts.join(", ")})`);
  }
}

if (missing.length > 0) {
  throw new Error(`Missing runtime package-script entrypoints:\n${missing.map((entry) => `- ${entry}`).join("\n")}`);
}

console.log(`Validated ${entrypoints.size} runtime package-script entrypoints.`);
