import { expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { runSnatch, runSnatchGroup } from "../../src/library/service";
import { processAcquireJob } from "../../src/worker/acquire";
import { importReleaseFromPath } from "../../src/library/importer";
import type { TorznabResult } from "../../src/library/torznab";
import { selectedCopies } from "../../src/media/selected-copies";
import { releaseOfferFromResult } from "../../src/media/release-offer";
import { MockBookTorrentClient } from "../mocks/book-torrents";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-copies-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
  const settings = defaultSettings({ libraryRoot: path.join(root, "library"),
    torznab: [{ name: "fixture", baseUrl: "https://fixture.invalid/api" }], agents: { apiKey: "test-key" } });
  repo.ensureSettings(); repo.updateSettings(settings);
  const client = new MockBookTorrentClient(path.join(settings.libraryRoot, ".podible", "downloads"));
  const result = (suffix: string, seeders = 10): TorznabResult => ({ title: `Dune Frank Herbert ${suffix} [PDF]`,
    mediaType: "ebook", provider: "fixture", guid: suffix, url: `https://fixture.invalid/${suffix}`,
    sizeBytes: 10, infoHash: null, seeders, leechers: 0, raw: {} });
  const primary = result("primary"), copy = result("copy", 9);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async input => {
    const url = new URL(String(input));
    if (url.hostname !== "fixture.invalid") throw new Error("Unexpected network request");
    if (url.pathname === "/api") return new Response(`<rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>${[primary, copy].map(row =>
      `<item><title>${row.title}</title><guid>${row.guid}</guid><enclosure url="${row.url}" length="10"/><torznab:attr name="seeders" value="${row.seeders}"/></item>`).join("")}</channel></rss>`);
    return new Response(bencode.encode({ announce: "http://tracker.invalid/", info: {
      name: `${url.pathname.slice(1)}.pdf`, length: 10, "piece length": 16384, pieces: Buffer.alloc(20, 1),
    } }));
  }) as typeof fetch;
  return { db, repo, book, root, settings, client, primary, copy, result,
    request: (selected = primary) => ({ bookId: book.id, provider: selected.provider, providerGuid: selected.guid,
      title: selected.title, mediaType: selected.mediaType, url: selected.url, sizeBytes: selected.sizeBytes }),
    async close() { globalThis.fetch = originalFetch; db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

test("book snatch publishes retained copies atomically before its download job; rollback/retry is safe", async () => {
  const f = await fixture();
  try {
    const resume = f.client.resume.bind(f.client);
    const resumeCheck = spyOn(f.client, "resume").mockImplementation(async ref => {
      const key = `book-selected-intent:${f.book.id}:ebook:${ref.nativeId}:${releaseOfferFromResult(f.primary).key}`;
      expect(f.repo.getJsonState(key)).toMatchObject({ selected: f.primary, alternatives: [f.copy] });
      return resume(ref);
    });
    const original = f.repo.createJob.bind(f.repo);
    const inspected = spyOn(f.repo, "createJob").mockImplementation(input => {
      if (input.type === "download") {
        const release = f.repo.getRelease(input.releaseId!)!;
        const copies = selectedCopies(f.repo, { media: "ebook", targetId: f.book.id }, release.id, releaseOfferFromResult(f.primary).key);
        expect(copies.map(copy => copy.offer.url)).toEqual([f.copy.url]);
      }
      return original(input);
    });
    f.db.exec("CREATE TRIGGER fail_copy_download BEFORE INSERT ON jobs WHEN NEW.type='download' BEGIN SELECT RAISE(ABORT,'publication failed'); END");
    const options = { bookTorrentClient: f.client, selectedCopies: { selected: f.primary, alternatives: [f.copy] } };
    await expect(runSnatch(f.repo, f.settings, f.request(), options)).rejects.toThrow("publication failed");
    expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(0);
    expect(f.db.query("SELECT key FROM app_state WHERE key LIKE 'book-release-selection:%'").all()).toHaveLength(0);
    f.db.exec("DROP TRIGGER fail_copy_download");
    // Simulate a non-Reflow/internal caller retrying without its lost in-memory evidence.
    await runSnatch(f.repo, f.settings, f.request(), { bookTorrentClient: f.client });
    expect(f.repo.listManifestationsByBook(f.book.id)).toHaveLength(1);
    expect(f.client.adds).toHaveLength(1);
    expect(f.repo.listJobsByType("download")).toHaveLength(1);
    inspected.mockRestore();
    resumeCheck.mockRestore();
  } finally { await f.close(); }
});

test("canonical import queues book copies on replay after lost queue publication without duplicating assets", async () => {
  const f = await fixture();
  try {
    const snatch = await runSnatch(f.repo, f.settings, f.request(), { bookTorrentClient: f.client,
      selectedCopies: { selected: f.primary, alternatives: [f.copy] } });
    const source = path.join(f.root, "source.pdf"); await fs.writeFile(source, "%PDF-test!");
    f.db.exec("CREATE TRIGGER fail_reuse BEFORE INSERT ON jobs WHEN NEW.type='torrent_reuse' BEGIN SELECT RAISE(ABORT,'reuse queue failed'); END");
    await expect(importReleaseFromPath(f.repo, snatch.release, source, f.settings.libraryRoot, { importKey: "book-copy-import" }))
      .rejects.toThrow("reuse queue failed");
    expect(f.repo.listAssetsByBook(f.book.id)).toHaveLength(1);
    f.db.exec("DROP TRIGGER fail_reuse");
    const imported = await importReleaseFromPath(f.repo, snatch.release, source, f.settings.libraryRoot, { importKey: "book-copy-import" });
    await importReleaseFromPath(f.repo, snatch.release, source, f.settings.libraryRoot, { importKey: "book-copy-import" });
    expect(f.repo.listAssetsByBook(f.book.id)).toHaveLength(1);
    expect(f.repo.crossSeed.receipts()).toHaveLength(1);
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(1);
    expect(f.repo.crossSeed.receipts()[0]!.command.scope).toEqual({ media: "ebook", targetId: f.book.id });
    expect(await fs.readFile(imported.linkedFiles[0]!, "utf8")).toBe("%PDF-test!");
    expect(await fs.readFile(source, "utf8")).toBe("%PDF-test!");
  } finally { await f.close(); }
});

test("mismatched per-part or media evidence is rejected before torrent addition", async () => {
  const f = await fixture();
  try {
    await expect(runSnatch(f.repo, f.settings, f.request(), { bookTorrentClient: f.client,
      selectedCopies: { selected: f.result("wrong-part"), alternatives: [f.copy] } }))
      .rejects.toThrow("does not belong");
    await expect(runSnatch(f.repo, f.settings, f.request(), { bookTorrentClient: f.client,
      selectedCopies: { selected: f.primary, alternatives: [{ ...f.copy, mediaType: "audio" }] } }))
      .rejects.toThrow("does not belong");
    expect(f.client.adds).toHaveLength(0);
    expect(f.repo.listReleasesByBook(f.book.id)).toHaveLength(0);
    expect(f.repo.listJobsByType("download")).toHaveLength(0);
  } finally { await f.close(); }
});

test("ordered book parts retain only their own equivalent candidates; manual snatches invent none", async () => {
  const f = await fixture();
  try {
    const selected = [f.result("part1"), f.result("part2")], copies = [f.result("copy1"), f.result("copy2")];
    const group = await runSnatchGroup(f.repo, f.settings, { bookId: f.book.id, mediaType: "ebook",
      manifestation: { label: null, editionNote: null }, parts: selected.map(row => ({
        provider: row.provider, providerGuid: row.guid, title: row.title, url: row.url,
      })) }, { bookTorrentClient: f.client, selectedCopiesByPart: selected.map((row, index) => ({ selected: row, alternatives: [copies[index]!] })) });
    expect(group.results).toHaveLength(2);
    for (const [index, snatch] of group.results.entries()) {
      expect(selectedCopies(f.repo, { media: "ebook", targetId: f.book.id }, snatch.release.id, releaseOfferFromResult(selected[index]!).key)
        .map(copy => copy.offer.url)).toEqual([copies[index]!.url]);
    }
    const manual = f.result("manual");
    const snatch = await runSnatch(f.repo, f.settings, f.request(manual), { bookTorrentClient: f.client });
    expect(f.repo.getJsonState(`book-release-selection:${snatch.release.id}`)).toBeNull();
    expect(f.repo.listJobsByType("torrent_reuse")).toHaveLength(0);
  } finally { await f.close(); }
});

for (const mode of ["search", "rss"] as const) test(`automatic ${mode} selection carries server-owned copy evidence into book snatch`, async () => {
  const f = await fixture();
  try {
    const modelProvider: ModelProvider = { getModel: () => ({
      getResponse: async () => ({ usage: new Usage(), output: [{ type: "message" as const, role: "assistant" as const,
        status: "completed" as const, content: [{ type: "output_text" as const, text: JSON.stringify({
          selections: [{ parts: [0], matchingIndexes: [[1]] }], confidence: 0.95, reason: "Equivalent copies of the same PDF",
        }) }] }] }),
      getStreamedResponse: async function* () { throw new Error("No streaming"); },
    }) };
    let ids: number[] | undefined;
    if (mode === "rss") {
      f.repo.releaseInbox.ensureFeed("copy-feed", "fixture", "ebook", new Date().toISOString());
      ids = f.repo.releaseInbox.ingest("copy-feed", [f.primary, f.copy], new Date().toISOString());
    }
    const job = f.repo.createJob({ type: "acquire", bookId: f.book.id, payload: { bookId: f.book.id,
      media: ["ebook"], forceAgent: true, ...(ids ? { feedCandidateIds: ids } : {}) } });
    await processAcquireJob({ repo: f.repo, getSettings: () => f.settings, modelProvider, bookTorrentClient: f.client }, job);
    const [release] = f.repo.listReleasesByBook(f.book.id);
    expect(release).toBeDefined();
    // Search normalizes GUIDs; the persisted association is deliberately the authoritative key.
    const keys = f.repo.getJsonState<string[]>(`book-release-selection:${release!.id}`)!;
    expect(keys).toHaveLength(1);
    expect(selectedCopies(f.repo, { media: "ebook", targetId: f.book.id }, release!.id, keys[0]!).map(copy => copy.offer.url)).toEqual([f.copy.url]);
  } finally { await f.close(); }
});
