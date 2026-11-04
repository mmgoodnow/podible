import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { FakeBookTorrentClient, bookTorrentMetainfo, startBookTorrentFixture } from "../mocks/book-torrent";
import { startMockTorznab } from "../mocks/torznab";

async function waitFor(file: string, needle: string) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if ((await readFile(file, "utf8").catch(() => "")).split("\n").includes(needle)) return;
    await Bun.sleep(20);
  }
  throw new Error(`Missing checkpoint ${needle}`);
}
const scenarios = [
  "audio:search",
  "audio:selection",
  "external-snatch",
  "audio:selection:0:part:0",
  "download:handoff",
  "asset-publication",
  "import:agent-first:publish"
].map((boundary) => ({ boundary, mode: "kill" }));
scenarios.push(
  { boundary: "audio:selection", mode: "takeover" },
  { boundary: "before-publication", mode: "takeover" },
  { boundary: "audio:selection", mode: "cancel" },
  { boundary: "audio:selection", mode: "cancel-retry" },
  { boundary: "asset-publication", mode: "cancel-retry" }
);
for (const { boundary, mode } of scenarios) {
  test(`real media pipeline survives ${mode} at ${boundary}`, async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-media-crash-"));
    const a = bookTorrentMetainfo("part-a.mp3"),
      b = bookTorrentMetainfo("part-b.mp3");
    const hashes = [a, b].map(infoHashFromTorrentBytes);
    const torznab = startMockTorznab({
      results: ["a", "b"].map((id, index) => ({
        title: `Red Rising Part ${index + 1} [MP3]`,
        torrentId: id,
        size: 10
      })),
      torrents: { a, b }
    });
    const bookTorrentClient = new FakeBookTorrentClient(path.join(root, "library"));
    const torrentServer = startBookTorrentFixture(bookTorrentClient);
    await writeFile(`${root}/book-torrent-url`, torrentServer.url);
    const db = new Database(path.join(root, "domain.sqlite"));
    db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.updateSettings(
      defaultSettings({
        torznab: [{ name: "mock", baseUrl: torznab.baseUrl }],
        rtorrent: { transport: "http-xmlrpc", url: "http://127.0.0.1:1/legacy-must-not-be-used", username: "", password: "" },
        agents: { apiKey: "fixture-not-a-real-key" },
        libraryRoot: path.join(root, "library"),
        polling: { rtorrentMs: 50 }
      })
    );
    const book = repo.createBook({ title: "Red Rising", author: "Pierce Brown" });
    repo.createJob({ type: "acquire", bookId: book.id, payload: { bookId: book.id, media: ["audio"] } });
    const launch = (stop: string) =>
      Bun.spawn([process.execPath, "run", "test/fixtures/media-execution-process.ts", root, stop], {
        cwd: path.resolve(import.meta.dir, "../.."),
        stdout: "pipe",
        stderr: "pipe"
      });
    const first = launch(boundary);
    let second: ReturnType<typeof launch> | undefined;
    try {
      await waitFor(`${root}/events`, boundary);
      if (boundary === "external-snatch") {
        expect(bookTorrentClient.adds).toEqual([hashes[0]!]);
        expect(repo.listReleasesByBook(book.id)).toHaveLength(0);
        expect(repo.bookTorrents.get(hashes[0]!)).toMatchObject({ book_id: book.id, release_id: null });
        expect(await bookTorrentClient.inspect({ clientId: bookTorrentClient.id, nativeId: hashes[0]! }))
          .toMatchObject({ state: "inactive", complete: false });
      }
      if (mode === "cancel" || mode === "cancel-retry") {
        const target =
          boundary === "asset-publication"
            ? repo
                .listJobsByType("import")
                .find((job) => job.release_id === repo.listAssetsByBook(book.id)[0]!.source_release_id)!.id
            : 1;
        repo.markJobCancelled(target, "User cancelled");
        await writeFile(`${root}/resume`, "yes");
        if (boundary === "asset-publication") first.kill("SIGCONT");
        const exit = await Promise.race([first.exited, Bun.sleep(5_000).then(() => -1)]);
        expect(exit).toBe(0);
        expect(repo.getJob(target)?.status).toBe("cancelled");
        if (boundary !== "asset-publication") {
          expect(repo.listReleasesByBook(book.id)).toHaveLength(0);
          expect(repo.listAssetsByBook(book.id)).toHaveLength(0);
          expect(bookTorrentClient.adds).toHaveLength(0);
        }
        if (mode === "cancel") return;
        repo.retryJob(target);
      }
      first.kill(mode === "takeover" ? "SIGSTOP" : "SIGKILL");
      if (mode !== "takeover") await first.exited;
      if (mode !== "takeover") await writeFile(`${root}/resume`, "yes");
      second = launch("");
      const exit = await Promise.race([second.exited, Bun.sleep(15_000).then(() => -1)]);
      if (exit !== 0) {
        if (exit === -1) second.kill("SIGKILL");
        throw new Error(await new Response(second.stderr).text());
      }
      if (mode === "takeover") {
        await writeFile(`${root}/resume`, "yes");
        first.kill("SIGCONT");
        expect(await Promise.race([first.exited, Bun.sleep(5_000).then(() => -1)])).toBe(0);
      }
      const researchCount = mode === "cancel-retry" && boundary === "audio:selection" ? 2 : 1;
      expect(torznab.searches).toHaveLength(researchCount);
      expect([...bookTorrentClient.adds].sort()).toEqual([...hashes].sort());
      if (boundary === "external-snatch")
        expect(bookTorrentClient.addAttempts.filter((hash) => hash === hashes[0])).toHaveLength(2);
      for (const hash of hashes) {
        const binding = repo.bookTorrents.get(hash);
        expect(binding).toMatchObject({ book_id: book.id, client_id: bookTorrentClient.id, relative_path: hash });
        const files = await bookTorrentClient.files({ clientId: bookTorrentClient.id, nativeId: hash });
        expect(files).toHaveLength(1);
        expect(files[0]!.path).toStartWith(path.join(binding!.root_path, hash) + path.sep);
      }
      expect(repo.listReleasesByBook(book.id)).toHaveLength(2);
      const manifestations = repo.listManifestationsByBook(book.id);
      expect(manifestations).toHaveLength(1);
      expect(
        repo.listAssetsByManifestation(manifestations[0]!.id).map((asset) => asset.sequence_in_manifestation)
      ).toEqual([0, 1]);
      expect(
        repo.listAssetsByManifestation(manifestations[0]!.id)
          .map((asset) => repo.getRelease(asset.source_release_id!)!.info_hash)
      ).toEqual(hashes);
      expect(repo.listJobsByType("import")).toHaveLength(2);
      expect(repo.listRecentJobs(100).every((job) => job.status === "succeeded")).toBe(true);
      const events = (await readFile(`${root}/events`, "utf8")).split("\n");
      expect(events.filter((event) => event === "audio:selection")).toHaveLength(researchCount);
    } finally {
      first.kill("SIGKILL");
      second?.kill("SIGKILL");
      await first.exited;
      if (second) await second.exited;
      torznab.stop();
      torrentServer.stop();
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  }, 25_000);
}
