import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { CURRENT_OPENLIBRARY_METADATA_VERSION } from "../../src/library/hydration";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";

test("server starts durable media execution, serves its published asset, and drains on SIGTERM", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-server-execution-"));
  const db = new Database(path.join(root, "podible.sqlite"));
  db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.updateSettings(defaultSettings({ libraryRoot: path.join(root, "library") }));
  const book = repo.createBook({ title: "Acceptance PDF", author: "Fixture" });
  repo.updateBookMetadata(book.id, { openLibraryMetadataVersion: CURRENT_OPENLIBRARY_METADATA_VERSION });
  const release = repo.createRelease({
    bookId: book.id,
    provider: "fixture",
    title: book.title,
    mediaType: "ebook",
    infoHash: "a".repeat(40),
    url: "https://unused.invalid",
    status: "downloaded"
  });
  const source = path.join(root, "fixture.pdf");
  const bytes = "%PDF-1.4\nfixture\n%%EOF\n";
  await writeFile(source, bytes);
  const job = repo.createJob({ type: "import", bookId: book.id, releaseId: release.id, payload: { basePath: source } });
  const user = repo.upsertUser({ provider: "plex", providerUserId: "fixture", username: "fixture", isAdmin: true });
  repo.createSession(user.id, hashSessionToken("fixture-token"), new Date(Date.now() + 60_000).toISOString());
  const child = Bun.spawn([process.execPath, "run", "server.ts"], {
    cwd: path.resolve(import.meta.dir, "../.."),
    env: { ...process.env, CONFIG_DIR: root, PORT: "0", NODE_ENV: "test" },
    stdout: "pipe",
    stderr: "pipe"
  });
  let output = "";
  const readOutput = (async () => {
    const reader = child.stdout.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      output += new TextDecoder().decode(value);
    }
  })();
  try {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline && (repo.getJob(job.id)?.status !== "succeeded" || !output.includes("listening")))
      await Bun.sleep(30);
    expect(repo.getJob(job.id)?.status).toBe("succeeded");
    const base = /listening on (http:\/\/localhost:\d+)/.exec(output)?.[1];
    expect(base).toBeDefined();
    const asset = repo.listAssetsByBook(book.id)[0]!;
    const response = await fetch(`${base}/ebook/${asset.id}`, { headers: { cookie: "podible_session=fixture-token" } });
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(bytes);
    expect(await readFile(repo.getAssetFiles(asset.id)[0]!.path, "utf8")).toBe(bytes);
    child.kill("SIGTERM");
    const exit = await Promise.race([child.exited, Bun.sleep(5_000).then(() => -1)]);
    expect(exit).toBe(0);
    const storage = new SQLiteStorage(path.join(root, "execution.sqlite"));
    await storage.initialize();
    const link = repo.getJobExecution(job.id)!;
    expect((await storage.getRun(link.run_id))?.status).toBe("completed");
    expect(
      (await storage.getStepResults(link.run_id)).some(
        (step) => step.name === "import:deterministic:publish" && step.status === "completed"
      )
    ).toBe(true);
    await storage.close();
  } finally {
    child.kill("SIGKILL");
    await child.exited;
    await readOutput;
    db.close();
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
