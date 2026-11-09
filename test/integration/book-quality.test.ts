import { expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { promises as fs } from "node:fs";
import { writeFileSync, renameSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { adoptBookContent } from "../../src/library/book-storage";
import { assessBookQuality, type BookQualityAssessment } from "../../src/library/quality";
import { assessBookUpgrade } from "../../src/library/quality-agent";
import { currentBookQualityBaseline, stageBookUpgrade, finalizeBookUpgrade, bookManualSelectionKey, bookUpgradeKey } from "../../src/library/quality-state";
import { selectPreferredAudioManifestation, selectPreferredDownloadableEbookAsset } from "../../src/library/asset-selection";
import { inspectMetainfo } from "../../src/media/metainfo";
import { torrentCacheKeyFor } from "../../src/library/torrent-cache";
import { feedIdentity } from "../../src/media/release-inbox";
import { processAcquireJob } from "../../src/worker/acquire";
import { processDownloadJob } from "../../src/worker/downloads";
import { processImportJob } from "../../src/worker/imports";
import { MockBookTorrentClient } from "../mocks/book-torrents";
import type { MediaType } from "../../src/app-types";
import { libraryRouter } from "../../src/rpc/library-router";
import { RtorrentClient } from "../../src/rtorrent";
import * as service from "../../src/library/service";

const good: BookQualityAssessment = {
  requirements: { work: "compatible", language: "compatible", translation: "compatible", narration: "compatible", abridgement: "compatible", edition: "compatible" },
  improvement: "better", reason: "Same complete English edition and performance, better useful presentation.",
};
function model(replies: unknown[] | ((request: ModelRequest) => unknown)) {
  const calls: ModelRequest[] = [];
  const provider: ModelProvider = { getModel: () => ({
    async getResponse(request) {
      calls.push(request);
      const reply = typeof replies === "function" ? replies(request) : replies.shift();
      if (!reply) throw new Error("Unexpected reviewer call");
      return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed", content: [{ type: "output_text", text: JSON.stringify(reply) }] }] };
    }, async *getStreamedResponse() { throw new Error("No streaming"); },
  }) };
  return { provider, calls };
}
async function fixture(media: MediaType = "audio") {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-quality-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), settings = defaultSettings({ libraryRoot: root });
  settings.agents.apiKey = "fake";
  repo.ensureSettings(); repo.updateSettings(settings);
  const book = repo.createBook({ title: "A complete book", author: "Author" });
  let n = 0;
  const manifestation = () => repo.addManifestation({ bookId: book.id, kind: media, label: "English original edition", language: "en" });
  async function add(id: number, mime: string, sequence = 0) {
    const ext = mime === "audio/mp4" ? ".m4b" : mime === "audio/mpeg" ? ".mp3" : mime === "application/pdf" ? ".pdf" : ".epub";
    const source = path.join(root, `source-${++n}${ext}`);
    await fs.writeFile(source, "%PDF-1.4\nVerified fixture bytes.");
    const file = await adoptBookContent(root, source);
    const size = (await fs.stat(file)).size;
    const release = repo.createRelease({ bookId: book.id, mediaType: media, title: `A complete book English original narrator Unabridged ${ext}`,
      provider: "fixture", url: `https://fixture.invalid/${n}`, infoHash: n.toString(16).padStart(40, "0"), status: "imported" });
    return repo.addAsset({ bookId: book.id, manifestationId: id, sequenceInManifestation: sequence, kind: "single", mime,
      totalSize: size, durationMs: media === "audio" ? 120000 : null, sourceReleaseId: release.id,
      files: [{ path: file, sourcePath: source, size, start: 0, end: size - 1, durationMs: media === "audio" ? 120000 : 0 }] });
  }
  async function stage(parts = 1, preflight = good) {
    const baseline = currentBookQualityBaseline(repo, book.id, media)!;
    const candidate = manifestation();
    stageBookUpgrade(repo, { bookId: book.id, media, baseline, candidateId: candidate.id, parts, preflight });
    return candidate;
  }
  function chosen() { return media === "ebook" ? selectPreferredDownloadableEbookAsset(repo.listAssetsByBook(book.id))?.manifestation_id :
    selectPreferredAudioManifestation(repo.listManifestationsByBook(book.id).map(manifestation => ({ manifestation, containers: repo.listAssetsByManifestation(manifestation.id) })))?.manifestation.id; }
  return { root, db, repo, settings, book, manifestation, add, stage, chosen, async close() { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("book upgrades require every semantic requirement; format preference cannot override uncertainty or equal quality", () => {
  expect(assessBookQuality(good).upgrade).toBe(true);
  for (const requirement of Object.keys(good.requirements) as Array<keyof typeof good.requirements>) {
    for (const value of ["unknown", "incompatible"] as const)
      expect(assessBookQuality({ ...good, requirements: { ...good.requirements, [requirement]: value } }).upgrade).toBe(false);
  }
  for (const improvement of ["equal", "worse", "unknown"] as const)
    expect(assessBookQuality({ ...good, improvement }).upgrade).toBe(false);
  expect(assessBookQuality(good, false).upgrade).toBe(false);
});

test("staged multipart audio is invisible until every verified part passes the actual SDK reviewer; old manifestation survives", async () => {
  const f = await fixture();
  try {
    const old = f.manifestation(); await f.add(old.id, "audio/mpeg");
    const candidate = await f.stage(2); await f.add(candidate.id, "audio/mp4", 0);
    const review = model([good]);
    expect(f.chosen()).toBe(old.id);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("waiting");
    expect(review.calls).toHaveLength(0);
    await f.add(candidate.id, "audio/mp4", 1);
    expect(f.chosen()).toBe(old.id);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("active");
    expect(f.chosen()).toBe(candidate.id);
    expect(f.repo.listAssetsByManifestation(old.id)).toHaveLength(1);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("active");
    expect(review.calls).toHaveLength(1);
    expect(JSON.stringify(review.calls[0])).toContain("translation/translator");
    expect(JSON.stringify(review.calls[0])).toContain("transcoding alone does not improve sound quality");
    expect(JSON.stringify(f.repo.getJsonState(bookUpgradeKey(candidate.id)))).toContain("compatible");
  } finally { await f.close(); }
});

test("visual ebook PDF can beat EPUB, but only a verified compatible judgment promotes it", async () => {
  const f = await fixture("ebook");
  try {
    const old = f.manifestation(); await f.add(old.id, "application/epub+zip");
    const candidate = await f.stage(); await f.add(candidate.id, "application/pdf");
    expect(f.chosen()).toBe(old.id);
    const review = model([{ ...good, requirements: { ...good.requirements, narration: "not-applicable" }, reason: "The visual cookbook PDF preserves the same edition's diagrams and layout." }]);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("active");
    expect(f.chosen()).toBe(candidate.id);
    expect(JSON.stringify(review.calls[0])).toContain("visual cookbook");
    expect(f.repo.listAssetsByManifestation(old.id)).toHaveLength(1);
  } finally { await f.close(); }
});

test("manual choices and concurrent incumbent changes fence both staging and promotion", async () => {
  const f = await fixture();
  try {
    const old = f.manifestation(); await f.add(old.id, "audio/mpeg");
    const candidate = await f.stage(); await f.add(candidate.id, "audio/mp4");
    const review = model(() => { f.repo.setJsonState(bookManualSelectionKey(f.book.id, "audio"), { manual: true }); return good; });
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("retained");
    expect(f.chosen()).toBe(old.id);
    expect(() => stageBookUpgrade(f.repo, { bookId: f.book.id, media: "audio", baseline: currentBookQualityBaseline(f.repo, f.book.id, "audio")!,
      candidateId: f.manifestation().id, parts: 1, preflight: good })).toThrow("manual choice");
  } finally { await f.close(); }
  const g = await fixture();
  try {
    const old = g.manifestation(); await g.add(old.id, "audio/mpeg");
    const candidate = await g.stage(); await g.add(candidate.id, "audio/mp4");
    const review = model(() => { g.repo.setManifestationPreference(old.id, 5); return good; });
    expect(await finalizeBookUpgrade(g.repo, g.settings, candidate.id, review.provider)).toBe("retained");
    expect(g.chosen()).toBe(old.id);
  } finally { await g.close(); }
});

test("bad file sizes never invoke the reviewer or activate a candidate", async () => {
  const f = await fixture();
  try {
    const old = f.manifestation(); await f.add(old.id, "audio/mpeg");
    const candidate = await f.stage(); const asset = await f.add(candidate.id, "audio/mp4");
    const file = f.repo.getAssetFiles(asset.id)[0]!;
    await fs.truncate(file.path, 1);
    const review = model([good]);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("retained");
    expect(review.calls).toHaveLength(0); expect(f.chosen()).toBe(old.id);
  } finally { await f.close(); }
});

test("same-size canonical replacement during the awaited reviewer cannot activate an upgrade", async () => {
  const f = await fixture();
  try {
    const old = f.manifestation(); await f.add(old.id, "audio/mpeg");
    const candidate = await f.stage(); const asset = await f.add(candidate.id, "audio/mp4");
    const file = f.repo.getAssetFiles(asset.id)[0]!;
    const review = model(() => {
      writeFileSync(file.path + ".replacement", Buffer.alloc(file.size, 120));
      renameSync(file.path + ".replacement", file.path);
      return good;
    });
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("retained");
    expect(review.calls).toHaveLength(1); expect(f.chosen()).toBe(old.id);
    expect(JSON.stringify(f.repo.getJsonState(bookUpgradeKey(candidate.id)))).toContain("bytes changed");
  } finally { await f.close(); }
});

test("missing imported files retain the staged candidate rather than promoting or failing the primary import", async () => {
  const f = await fixture("ebook");
  try {
    const old = f.manifestation(); await f.add(old.id, "application/pdf");
    const candidate = await f.stage(); const asset = await f.add(candidate.id, "application/epub+zip");
    await fs.unlink(f.repo.getAssetFiles(asset.id)[0]!.path);
    const review = model([good]);
    expect(await finalizeBookUpgrade(f.repo, f.settings, candidate.id, review.provider)).toBe("retained");
    expect(f.chosen()).toBe(old.id); expect(review.calls).toHaveLength(0);
  } finally { await f.close(); }
});

test("quality reviewer without credentials declines safely instead of trusting a container ranking", async () => {
  const settings = defaultSettings(); settings.agents.apiKey = "";
  expect(await assessBookUpgrade(settings, { title: "Book", author: "Author", description: null,
    media: "audio", editionPreference: "", current: {}, candidate: {}, phase: "release" })).toBeNull();
});

test("an invalid preferred ebook does not hide the remaining valid upgrade baseline", async () => {
  const f = await fixture("ebook");
  try {
    const valid = f.manifestation(); await f.add(valid.id, "application/pdf");
    const invalid = f.manifestation(), invalidAsset = await f.add(invalid.id, "application/epub+zip");
    f.repo.setManifestationPreference(invalid.id, 10);
    f.repo.setReleaseStatus(invalidAsset.source_release_id!, "failed", "Wrong imported ebook");
    expect(currentBookQualityBaseline(f.repo, f.book.id, "ebook")?.id).toBe(valid.id);
  } finally { await f.close(); }
});

test("RSS acquisition stages an ebook upgrade, download/import verifies and promotes it without changing audio or deleting old files", async () => {
  const f = await fixture("ebook");
  try {
    const old = f.manifestation(); await f.add(old.id, "application/epub+zip");
    const audio = f.repo.addManifestation({ bookId: f.book.id, kind: "audio" });
    const bytes = Buffer.from("%PDF-1.4\nSame visual edition, intact pages.");
    const torrent = bencode.encode({ info: { name: "illustrated.pdf", length: bytes.length, "piece length": 16384, pieces: Buffer.alloc(20) } });
    const metadata = inspectMetainfo(torrent);
    const offer = { title: "A complete book Author English Original PDF", provider: "Fixture", guid: "quality-offer", url: "https://fixture.invalid/torrent",
      mediaType: "ebook" as const, sizeBytes: bytes.length, infoHash: metadata.infoHash, seeders: 5, leechers: 0, raw: {} };
    f.repo.putTorrentCache({ key: torrentCacheKeyFor({ provider: offer.provider, providerGuid: offer.guid, url: offer.url, infoHash: offer.infoHash }),
      provider: offer.provider, providerGuid: offer.guid, url: offer.url, infoHash: offer.infoHash, torrentBytes: torrent });
    const feed = feedIdentity({ name: "Fixture", baseUrl: "https://fixture.invalid/api" }, "ebook");
    f.repo.releaseInbox.ensureFeed(feed, "Fixture", "ebook", new Date().toISOString());
    const ids = f.repo.releaseInbox.ingest(feed, [offer], new Date().toISOString());
    const job = f.repo.createJob({ type: "acquire", bookId: f.book.id, payload: { bookId: f.book.id, media: ["ebook"], feedCandidateIds: ids, considerUpgrade: true } });
    const client = new MockBookTorrentClient(path.join(f.root, ".podible", "downloads"));
    const review = model([{ selections: [{ parts: [0], manifestation: { label: "Same illustrated edition", editionNote: null, language: "en" } }], confidence: 1, reason: "Same complete edition" }, good,
      { selectedIndices: [0], confidence: 1, reason: "Complete PDF" }, good]);
    const ctx = { repo: f.repo, getSettings: () => f.settings, modelProvider: review.provider, bookTorrentClient: client, onLog() {} };
    await processAcquireJob(ctx, job);
    expect(client.adds).toHaveLength(1); expect(f.chosen()).toBe(old.id);
    const record = client.records.get(metadata.infoHash)!;
    await fs.writeFile(record.files[0]!.path, bytes);
    Object.assign(record.snapshot, { complete: true, state: "seeding", bytesDone: bytes.length });
    await processDownloadJob(ctx, f.repo.listJobsByType("download")[0]!);
    await processImportJob(ctx, f.repo.listJobsByType("import")[0]!);
    expect(f.chosen()).not.toBe(old.id);
    expect(f.repo.listAssetsByManifestation(old.id)).toHaveLength(1);
    expect(f.repo.getManifestation(audio.id)?.preferred_score).toBe(0);
    expect(review.calls).toHaveLength(4);
  } finally { await f.close(); }
});

test("user-reported wrong-file repair can replace a held manual edition without equating it to the invalid original", async () => {
  const f = await fixture("ebook");
  let legacy: ReturnType<typeof spyOn> | undefined, search: ReturnType<typeof spyOn> | undefined;
  try {
    const old = f.manifestation(), oldAsset = await f.add(old.id, "application/pdf");
    f.db.query("UPDATE manifestations SET edition_note='Wrong translator and wrong language' WHERE id=?").run(old.id);
    const hold = { selectedAt: "original-manual-choice" };
    f.repo.setJsonState(bookManualSelectionKey(f.book.id, "ebook"), hold);
    const file = f.repo.getAssetFiles(oldAsset.id)[0]!;
    legacy = spyOn(RtorrentClient.prototype, "getImportSource").mockResolvedValue({ basePath: file.source_path!, selectedPaths: [file.source_path!] });
    const report = await libraryRouter.routes.reportImportIssue.handler({ repo: f.repo, startTime: 0,
      request: new Request("http://fixture.invalid"), session: null }, {
      bookId: f.book.id, mediaType: "ebook", releaseId: oldAsset.source_release_id!, manifestationId: old.id,
    });
    const bytes = Buffer.from("%PDF-1.4\nCorrect English edition.");
    const torrent = bencode.encode({ info: { name: "correct.pdf", length: bytes.length, "piece length": 16384, pieces: Buffer.alloc(20) } });
    const metadata = inspectMetainfo(torrent);
    const offer = { title: "A complete book Author English Correct PDF", provider: "Repair", guid: "repair", url: "https://fixture.invalid/repair",
      mediaType: "ebook" as const, sizeBytes: bytes.length, infoHash: metadata.infoHash, seeders: 5, leechers: 0, raw: {} };
    f.repo.putTorrentCache({ key: torrentCacheKeyFor({ provider: offer.provider, providerGuid: offer.guid, url: offer.url, infoHash: offer.infoHash }),
      provider: offer.provider, providerGuid: offer.guid, url: offer.url, infoHash: offer.infoHash, torrentBytes: torrent });
    search = spyOn(service, "runSearchBatch").mockResolvedValue({ results: [offer], failures: [] });
    const client = new MockBookTorrentClient(path.join(f.root, ".podible", "downloads"));
    const review = model([{ primaryQuery: "A complete book Author", alternateQuery: null },
      { selections: [{ parts: [0], manifestation: { label: "Correct edition", editionNote: null, language: "en" } }], confidence: 1, reason: "Actual requested book" },
      { selectedIndices: [0], confidence: 1, reason: "Complete correct PDF" }]);
    const ctx = { repo: f.repo, getSettings: () => f.settings, modelProvider: review.provider, bookTorrentClient: client, onLog() {} };
    await processImportJob(ctx, f.repo.getJob(report.jobId)!);
    const acquire = f.repo.listJobsByType("acquire")[0]!;
    expect(JSON.parse(acquire.payload_json!).repairReviewJobId).toBe(report.jobId);
    await processAcquireJob(ctx, acquire);
    expect(client.adds).toHaveLength(1);
    const record = client.records.get(metadata.infoHash)!;
    await fs.writeFile(record.files[0]!.path, bytes);
    Object.assign(record.snapshot, { complete: true, state: "seeding", bytesDone: bytes.length });
    await processDownloadJob(ctx, f.repo.listJobsByType("download")[0]!);
    const imported = f.repo.listJobsByType("import").find(job => job.id !== report.jobId)!;
    await processImportJob(ctx, imported);
    expect(f.chosen()).not.toBe(old.id);
    expect(f.repo.listAssetsByManifestation(old.id)).toHaveLength(1);
    expect(await fs.readFile(file.path, "utf8")).toContain("Verified fixture bytes");
    expect(f.repo.getJsonState<unknown>(bookManualSelectionKey(f.book.id, "ebook"))).toEqual(hold);
    expect(review.calls).toHaveLength(3); // Query plan, selection and import, never invalid-incumbent equivalence.
    const ordinaryRetry = f.repo.createJob({ type: "acquire", bookId: f.book.id, payload: { bookId: f.book.id, media: ["ebook"], priorFailure: true } });
    await processAcquireJob(ctx, ordinaryRetry);
    expect(client.adds).toHaveLength(1); expect(review.calls).toHaveLength(3);
    const fakeRepair = f.repo.createJob({ type: "acquire", bookId: f.book.id,
      payload: { bookId: f.book.id, media: ["ebook"], repairReviewJobId: imported.id } });
    await expect(processAcquireJob(ctx, fakeRepair)).rejects.toThrow("repair provenance");
  } finally { legacy?.mockRestore(); search?.mockRestore(); await f.close(); }
});

test("replaying a pre-staging wrong-file publication reuses its asset without creating or rewiring a manifestation", async () => {
  const f = await fixture("ebook");
  let source: ReturnType<typeof spyOn> | undefined;
  try {
    const old = f.manifestation(), asset = await f.add(old.id, "application/pdf");
    const job = f.repo.createJob({ type: "import", bookId: f.book.id, releaseId: asset.source_release_id,
      payload: { userReportedIssue: true, rejectedSourcePaths: [] } });
    f.db.query("UPDATE assets SET import_key=? WHERE id=?").run(`import:${job.id}`, asset.id);
    const directory = path.join(f.root, "old-review"); await fs.mkdir(directory);
    await fs.writeFile(path.join(directory, "a.pdf"), "old review choice");
    await fs.writeFile(path.join(directory, "b.pdf"), "other choice");
    source = spyOn(RtorrentClient.prototype, "getImportSource").mockResolvedValue({ basePath: directory, selectedPaths: [path.join(directory, "a.pdf")] });
    const review = model([{ selectedIndices: [0], confidence: 1, reason: "Persisted alternative selection" }]);
    await processImportJob({ repo: f.repo, getSettings: () => f.settings, modelProvider: review.provider, onLog() {},
      execution: { key: "old-review", assertActive() {}, async sleep() {},
        async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
          if (name === "import:review:publish") return { assetId: asset.id, linkedFiles: f.repo.getAssetFiles(asset.id).map(file => file.path) } as T;
          return handler();
        } } }, job);
    expect(f.repo.listManifestationsByBook(f.book.id)).toHaveLength(1);
    expect(f.repo.findAssetByImportKey(`import:${job.id}`)?.manifestation_id).toBe(old.id);
    expect(f.repo.getManifestation(old.id)?.preferred_score).toBe(0);
  } finally { source?.mockRestore(); await f.close(); }
});
