import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { initializeManagedContent, reconcileLibraryView } from "../../src/media/library-view";
import { BooksRepo } from "../../src/repo";

const count = Number(process.argv[2] ?? 1500);
if (!Number.isSafeInteger(count) || count < 1 || count > 5000) throw new Error("Pass a file count between 1 and 5000");
const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "podible-view-timing-")));
using db = new Database(":memory:");
try {
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const contentRoot = path.join(root, "objects"), viewRoot = path.join(root, "views", "plex", "tv");
  await initializeManagedContent(contentRoot);
  const entries = [];
  for (let i = 0; i <= count; i++) {
    const section = String(Math.floor(i / 50));
    const contentPath = path.join(contentRoot, `fixture-${i}.mkv`);
    await Bun.write(contentPath, "synthetic");
    entries.push({ relativePath: `Fixture ${section} {tvdb-${section}}/Season 01/Fixture - S01E${i}.mkv`, contentPath });
  }
  let checks = 0;
  const assertActive = () => {
    checks++;
    const settings = repo.readSettings();
    if (!settings || repo.retention.get({ namespace: "tv-target", id: "1" })?.desiredPresence === "evicted")
      throw new Error("Fixture no longer permits publication");
  };
  const input = { contentRoot, viewRoot, assertActive };
  // Do not include library construction in the timed publication.
  await reconcileLibraryView({ ...input, entries: entries.slice(0, -1) });
  for (const [phase, planned] of [["add", entries], ["unchanged", entries], ["remove", entries.slice(0, -1)]] as const) {
    checks = 0;
    const start = performance.now();
    const result = await reconcileLibraryView({ ...input, entries: planned });
    console.log(JSON.stringify({ phase, files: planned.length, ms: Math.round(performance.now() - start), checks, ...result }));
  }
} finally {
  await fs.rm(root, { recursive: true, force: true });
}
