import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

test("an asynchronous inventory step can precede older cached steps on replay", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-reflow-order-"));
  const fixture = path.resolve(import.meta.dir, "../fixtures/reflow-replay-order.ts");
  const worker = Bun.spawn([process.execPath, "run", fixture, path.join(root, "execution.sqlite")], { stderr: "pipe" });
  // The broken engine can hang on stop as well as on replay.
  const deadline = setTimeout(() => worker.kill("SIGKILL"), 10_000);
  try {
    const [exit, errors] = await Promise.all([worker.exited, new Response(worker.stderr).text()]);
    expect(errors).toBe("");
    expect(exit).toBe(0);
  } finally {
    clearTimeout(deadline);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
