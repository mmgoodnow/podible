import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";

async function waitFor(file: string, needle: string) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const value = await readFile(file, "utf8").catch(() => "");
    if (value.includes(needle)) return;
    await Bun.sleep(20);
  }
  throw new Error(`Timed out waiting for ${needle}`);
}

for (const boundary of ["after-search", "after-select", "after-external:a", "after-publication:a", "after-part:a"]) {
  test(`file-backed execution survives SIGKILL at ${boundary}`, async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-durable-"));
    const launch = (stop = "") => Bun.spawn([process.execPath, "run", "test/fixtures/durable-process.ts", root, stop], {
      cwd: path.resolve(import.meta.dir, "../.."), stdout: "pipe", stderr: "pipe",
    });
    const first = launch(boundary);
    let second: ReturnType<typeof launch> | undefined;
    try {
      await waitFor(`${root}/events`, boundary);
      first.kill("SIGKILL");
      await first.exited;
      await writeFile(`${root}/resume`, "yes");
      second = launch();
      const exit = await Promise.race([second.exited, Bun.sleep(10_000).then(() => -1)]);
      const stderr = exit === -1 ? "timeout" : await new Response(second.stderr).text();
      expect(stderr).toBe("");
      expect(exit).toBe(0);
      const db = new Database(`${root}/domain.sqlite`);
      expect(db.query("SELECT id FROM effects ORDER BY id").all()).toEqual([
        "asset:a", "asset:b", "search", "select", "torrent:a", "torrent:b",
      ].map((id) => ({ id })));
      expect(db.query("SELECT count(*) AS n FROM attempts WHERE id IN ('search', 'select')").get()).toEqual({ n: 2 });
      expect(db.query("SELECT count(*) AS n FROM attempts WHERE id = 'torrent:a'").get()).toEqual({
        n: boundary === "after-external:a" ? 2 : 1,
      });
      expect(db.query("SELECT count(*) AS n FROM attempts WHERE id = 'asset:a'").get()).toEqual({
        n: boundary === "after-publication:a" ? 2 : 1,
      });
      db.close();
      // Completed research must not run again; external/publication effects
      // must reconcile even when the engine never saved their result.
    } finally {
      first.kill();
      second?.kill();
      if (second) await second.exited;
      await rm(root, { recursive: true, force: true });
    }
  }, 15_000);
}
