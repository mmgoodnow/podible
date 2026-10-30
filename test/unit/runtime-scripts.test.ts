import { expect, test } from "bun:test";
import path from "node:path";

test("runtime script validation rejects a removed local export without executing the script", async () => {
  const validator = path.resolve(import.meta.dir, "../../scripts/validate-runtime-scripts.ts");
  const child = Bun.spawn([process.execPath, "run", validator], {
    cwd: path.resolve(import.meta.dir, "../fixtures/invalid-runtime-script"),
    stdout: "pipe",
    stderr: "pipe",
  });
  const stderr = await new Response(child.stderr).text();
  expect(await child.exited).toBe(1);
  expect(stderr).toContain("No matching export");
  expect(stderr).toContain("removedExport");
});

test("dev-login loads successfully and prints usage without modifying the database", async () => {
  const child = Bun.spawn([process.execPath, "run", "scripts/dev-login.ts"], {
    cwd: path.resolve(import.meta.dir, "../.."), stdout: "pipe", stderr: "pipe",
  });
  const stderr = await new Response(child.stderr).text();
  expect(await child.exited).toBe(1);
  expect(stderr).toContain("Usage: bun run dev-login");
  expect(stderr).not.toContain("SyntaxError");
});
