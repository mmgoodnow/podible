import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { startMockRtorrent } from "../mocks/rtorrent";
import { startMockTorznab } from "../mocks/torznab";

async function waitFor(file: string, needle: string) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if ((await readFile(file, "utf8").catch(() => "")).split("\n").includes(needle)) return;
    await Bun.sleep(20);
  }
  throw new Error(`Missing checkpoint ${needle}`);
}
function torrent(name: string) {
  return new Uint8Array(
    Buffer.from(
      `d4:infod4:name${name.length}:${name}6:lengthi10e12:piece lengthi16384e6:pieces20:12345678901234567890ee`
    )
  );
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
    const a = torrent("part-a"),
      b = torrent("part-b");
    const hashes = [a, b].map(infoHashFromTorrentBytes);
    const files = [path.join(root, "a.mp3"), path.join(root, "b.mp3")];
    for (const file of files) await writeFile(file, "ID3-fixture");
    const torznab = startMockTorznab({
      results: ["a", "b"].map((id, index) => ({
        title: `Red Rising Part ${index + 1} [MP3]`,
        torrentId: id,
        size: 10
      })),
      torrents: { a, b }
    });
    const rtorrent = startMockRtorrent({
      byHash: Object.fromEntries(
        hashes.map((hash, index) => [
          hash,
          {
            name: `part-${index}`,
            basePath: files[index]!,
            sizeBytes: 10,
            completeAfterPolls: 2
          }
        ])
      )
    });
    const db = new Database(path.join(root, "domain.sqlite"));
    db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
    runMigrations(db);
    const repo = new BooksRepo(db);
    repo.updateSettings(
      defaultSettings({
        torznab: [{ name: "mock", baseUrl: torznab.baseUrl }],
        rtorrent: { transport: "http-xmlrpc", url: rtorrent.url, username: "", password: "" },
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
          expect(rtorrent.loadCalls).toHaveLength(0);
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
      expect(rtorrent.loadCalls.sort()).toEqual([...hashes].sort());
      expect(repo.listReleasesByBook(book.id)).toHaveLength(2);
      const manifestations = repo.listManifestationsByBook(book.id);
      expect(manifestations).toHaveLength(1);
      expect(
        repo.listAssetsByManifestation(manifestations[0]!.id).map((asset) => asset.sequence_in_manifestation)
      ).toEqual([0, 1]);
      expect(repo.listJobsByType("import")).toHaveLength(2);
      expect(repo.listRecentJobs(100).every((job) => job.status === "succeeded")).toBe(true);
      const events = (await readFile(`${root}/events`, "utf8")).split("\n");
      expect(events.filter((event) => event === "audio:selection")).toHaveLength(researchCount);
    } finally {
      first.kill("SIGKILL");
      second?.kill("SIGKILL");
      if (second) await second.exited;
      torznab.stop();
      rtorrent.stop();
      db.close();
      await rm(root, { recursive: true, force: true });
    }
  }, 25_000);
}
