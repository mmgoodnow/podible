import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { PENDING_AUDIO_EDITION_SCORE, type AudioEdition } from "../../src/library/audio-editions";
import { streamAudioManifestation } from "../../src/library/media";
import { bookManualSelectionKey } from "../../src/library/quality-state";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { BooksRepo } from "../../src/repo";
import { appRouter } from "../../src/trpc";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { bookTorrentMetainfo } from "../mocks/book-torrent";
import { MockBookTorrentClient } from "../mocks/book-torrents";
import { startMockTorznab } from "../mocks/torznab";

const query = "Piranesi Susanna Clarke";
const releases = [
  { id: "drama1", name: "part1.mp3", title: `${query} GraphicAudio Part 1 of 2 MP3` },
  { id: "drama2", name: "part2.mp3", title: `${query} GraphicAudio Part 2 of 2 MP3` },
  { id: "reading", name: "reading.mp3", title: `${query} Read by Jane Reader MP3` },
];

function matching(prompt: string, edition: AudioEdition, titles: string[]) {
  const parts = titles.map((title) => Number(prompt.split("\n").find((line) => line.includes(`| ${title} |`))?.split("|")[1]));
  return { selections: [{ audioEdition: edition, parts }], confidence: 0.99, reason: "Fixture performance evidence" };
}

async function fixture(reply: (prompt: string) => unknown, options: { stalled?: string; badImport?: string } = {}) {
  const audio = Bun.spawnSync(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=220:duration=0.3",
    "-c:a", "libmp3lame", "-f", "mp3", "pipe:1"]);
  if (audio.exitCode !== 0) throw new Error(`Fixture audio encoding failed: ${audio.stderr}`);
  const audioBytes = audio.stdout;
  const root = await realpath(await mkdtemp(path.join(os.tmpdir(), "book-audio-editions-")));
  const db = new Database(path.join(root, "domain.sqlite"));
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  settings.libraryRoot = path.join(root, "library");
  settings.polling.rtorrentMs = 500;
  settings.agents.apiKey = "never-expose-fixture-key";
  settings.agents.editionPreference = "global preference must not appear";
  const torrents = Object.fromEntries(releases.map(({ id, name }) => [id,
    bookTorrentMetainfo(id === options.badImport ? "readme.txt" : name, audioBytes)]));
  const torznab = startMockTorznab({ results: releases.map(({ id, title }) => ({ title, torrentId: id, size: audioBytes.length })), torrents });
  settings.torznab = [{ name: "fixture", baseUrl: torznab.baseUrl, apiKey: "never-expose-indexer-key" }];
  repo.updateSettings(settings);
  const prompts: string[] = [];
  const modelProvider: ModelProvider = { getModel: () => ({
    async getResponse(request: ModelRequest) {
      const prompt = JSON.stringify(request.input).replaceAll("\\n", "\n");
      const selection = !prompt.includes("Required audiobook performance")
        ? { primaryQuery: query, alternateQuery: null }
        : (prompts.push(prompt), reply(prompt));
      return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
        content: [{ type: "output_text", text: JSON.stringify(selection) }] }] };
    }, async *getStreamedResponse() { throw new Error("Unexpected streaming"); },
  }) };
  const torrentClient = new MockBookTorrentClient(path.join(settings.libraryRoot, ".podible", "downloads"));
  const book = repo.createBook({ title: "Piranesi", author: "Susanna Clarke" });
  const reader = repo.upsertUser({ provider: "local", providerUserId: "edition-reader", username: "edition-reader", isAdmin: false });
  repo.createSession(reader.id, hashSessionToken("edition-reader"), null, "browser");
  const context = { repo, request: new Request("http://localhost/"), startTime: Date.now(), bookTorrentClient: torrentClient };
  const caller = appRouter.createCaller({ ...context, session: repo.getSessionByTokenHash(hashSessionToken("edition-reader")) });
  const anonymous = appRouter.createCaller({ ...context, session: null });
  const logs: string[] = [];
  const runtime = await createJobExecutionRuntime({ ...context, getSettings: () => repo.getSettings(), modelProvider, onLog: (line) => logs.push(line) },
    path.join(root, "execution.sqlite"), { retryDelayMs: 1 });

  async function complete(id: string) {
    const record = torrentClient.records.get(infoHashFromTorrentBytes(torrents[id]!))!;
    const file = record.files[0]!.path;
    if (options.stalled === id) { record.snapshot.message = "private tracker error"; return; }
    await mkdir(path.dirname(file), { recursive: true });
    await writeFile(file, audioBytes);
    Object.assign(record.snapshot, { complete: true, state: "seeding", bytesDone: audioBytes.length });
  }
  async function until(check: () => boolean | Promise<boolean>) {
    const deadline = Date.now() + 6000;
    while (!(await check())) {
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for audiobook acquisition:\n${logs.slice(-30).join("\n")}`);
      await runtime.reconcile();
      await Bun.sleep(30);
    }
  }
  const params = { bookId: book.id };
  const status = async (edition: AudioEdition) => (await caller.book.audioEditionRequests(params)).requests.find((entry) => entry.edition === edition)!;
  const queue = (edition: AudioEdition) => caller.book.requestAudioEdition({ ...params, edition });
  const playback = async () => (await caller.library.get(params)).book.playback;
  return { repo, book, caller, anonymous, params, status, queue, playback, complete, until, prompts, torrentClient, torrents,
    async close() { await runtime.stop(); torznab.stop(); db.close(); await rm(root, { recursive: true, force: true }); } };
}

test("reader requests both performances, publishes ordered parts, and preserves the current audiobook and manual hold", async () => {
  const f = await fixture((prompt) => prompt.includes("**narrated**")
    ? matching(prompt, "narrated", [releases[2]!.title]) : matching(prompt, "dramatized", releases.slice(0, 2).map((r) => r.title)));
  try {
    const narrated = await f.queue("narrated");
    await f.until(() => f.repo.getJobStatus(narrated.jobId!) === "succeeded");
    await f.complete("reading");
    await f.until(async () => (await f.status("narrated")).status === "ready");
    const current = (await f.playback()).audio!;
    expect(current.label).toBe("Narrated audiobook");
    f.repo.setManifestationPreference(current.manifestationId, 7);
    const hold = bookManualSelectionKey(f.book.id, "audio");
    f.repo.setJsonState(hold, { selectedAt: "manual fixture" });

    const [drama, repeat] = await Promise.all([f.queue("dramatized"), f.queue("dramatized")]);
    expect(repeat.jobId).toBe(drama.jobId);
    await f.until(() => f.repo.getJobStatus(drama.jobId!) === "succeeded");
    const staged = f.repo.listManifestationsByBook(f.book.id).find((row) => row.preferred_score === PENDING_AUDIO_EDITION_SCORE)!;
    expect(staged.label).toBe("Dramatized / GraphicAudio");
    await f.complete("drama2");
    await f.until(() => f.repo.listAssetsByManifestation(staged.id).length === 1);
    expect((await f.status("dramatized")).status).toBe("importing");
    expect((await f.queue("dramatized")).jobId).toBe(drama.jobId);
    expect((await f.playback()).audioOptions.map((row) => row.manifestationId)).toEqual([current.manifestationId]);
    expect((await f.caller.book.editions(f.params)).audio).toHaveLength(1);
    await expect(f.caller.book.chapters({ ...f.params, manifestationId: staged.id })).rejects.toMatchObject({ code: "NOT_FOUND" });
    const pending = f.repo.getManifestationWithContainers(staged.id)!;
    const stream = new Request(`http://localhost/stream/m/${staged.id}.mp3`, { headers: { range: "bytes=0-31" } });
    expect((await streamAudioManifestation(stream, f.repo, pending.manifestation, pending.containers)).status).toBe(404);

    await f.complete("drama1");
    await f.until(async () => (await f.status("dramatized")).status === "ready");
    const playback = await f.playback();
    expect(playback.audio).toEqual(current);
    expect(playback.audioOptions.map((row) => row.manifestationId)).toEqual([current.manifestationId, staged.id]);
    expect(f.repo.getManifestation(current.manifestationId)?.preferred_score).toBe(7);
    expect(f.repo.getJsonState<{ selectedAt: string }>(hold)).toEqual({ selectedAt: "manual fixture" });
    expect(f.repo.listAssetsByManifestation(staged.id).map((asset) => asset.sequence_in_manifestation)).toEqual([0, 1]);
    const ready = f.repo.getManifestationWithContainers(staged.id)!;
    const range = await streamAudioManifestation(stream, f.repo, ready.manifestation, ready.containers);
    expect(range.status).toBe(206);
    expect((await range.arrayBuffer()).byteLength).toBe(32);
    expect(await f.queue("dramatized")).toMatchObject({ status: "ready", manifestationId: staged.id, jobId: drama.jobId });
    expect(f.torrentClient.adds).toHaveLength(3);
    expect(f.prompts).toHaveLength(2);
    expect(f.prompts[0]).not.toContain("GraphicAudio Part");
    expect(f.prompts.join("\n")).not.toContain("global preference must not appear");
  } finally { await f.close(); }
}, 15_000);

test("only the fixed authenticated edition request is accepted; uncertain or mismatched selections never snatch", async () => {
  let attempt = 0;
  const f = await fixture((prompt) => ({ ...matching(prompt, attempt === 0 ? "narrated" : "dramatized", [releases[0]!.title]), confidence: attempt === 1 ? 0.4 : 1 }));
  try {
    await expect(f.anonymous.book.requestAudioEdition({ ...f.params, edition: "dramatized" })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
    await expect(f.caller.book.requestAudioEdition({ bookId: 99999, edition: "dramatized" })).rejects.toMatchObject({ code: "NOT_FOUND" });
    const extra = { ...f.params, edition: "dramatized" as const, url: "https://untrusted.invalid", preferredScore: 1000 };
    await expect(f.caller.book.requestAudioEdition(extra)).rejects.toMatchObject({ code: "BAD_REQUEST" });
    const settings = f.repo.getSettings();
    f.repo.updateSettings({ ...settings, agents: { ...settings.agents, apiKey: "" } });
    expect((await f.caller.book.audioEditionRequests(f.params)).enabled).toBe(false);
    await expect(f.queue("dramatized")).rejects.toMatchObject({ code: "BAD_REQUEST" });
    f.repo.updateSettings(settings);

    const first = await f.queue("dramatized");
    await f.until(async () => (await f.status("dramatized")).status === "no_match");
    attempt = 1;
    const second = await f.queue("dramatized");
    expect(second.jobId).not.toBe(first.jobId);
    await f.until(async () => (await f.status("dramatized")).status === "no_match");
    attempt = 2;
    await f.queue("dramatized");
    await f.until(async () => (await f.status("dramatized")).status === "no_match");
    expect(f.torrentClient.adds).toEqual([]);
    expect(f.repo.listManifestationsByBook(f.book.id)).toEqual([]);
  } finally { await f.close(); }
}, 15_000);

test("a provider alias for an existing torrent cannot reparent it or mark a new edition ready", async () => {
  const f = await fixture((prompt) => matching(prompt, "narrated", [releases[2]!.title]));
  try {
    const release = f.repo.createRelease({ bookId: f.book.id, mediaType: "audio", title: "Prior release", provider: "previous",
      url: "https://fixture.invalid/old-url", infoHash: infoHashFromTorrentBytes(f.torrents.reading!), status: "imported" });
    await f.queue("narrated");
    await f.until(async () => (await f.status("narrated")).status === "failed");
    expect(f.repo.listReleasesByBook(f.book.id).map((row) => row.id)).toEqual([release.id]);
    expect(f.torrentClient.adds).toEqual([]);
    expect(f.repo.listJobsByType("download")).toEqual([]);
  } finally { await f.close(); }
}, 15_000);

test("targeted import failure does not fall back to another performance or publish audio", async () => {
  const f = await fixture((prompt) => matching(prompt, "narrated", [releases[2]!.title]), { badImport: "reading" });
  try {
    const request = await f.queue("narrated");
    await f.until(() => f.repo.getJobStatus(request.jobId!) === "succeeded");
    await f.complete("reading");
    await f.until(async () => (await f.status("narrated")).status === "failed");
    expect(f.repo.listJobsByType("import").map((job) => job.status)).toEqual(["failed"]);
    expect(f.repo.listJobsByType("acquire").map((job) => job.id)).toEqual([request.jobId!]);
    expect((await f.playback()).audioOptions).toEqual([]);
  } finally { await f.close(); }
}, 15_000);

test("targeted download failure cannot enqueue generic recovery or leak private provider errors", async () => {
  const f = await fixture((prompt) => matching(prompt, "narrated", [releases[2]!.title]), { stalled: "reading" });
  try {
    const request = await f.queue("narrated");
    await f.until(() => f.repo.getJobStatus(request.jobId!) === "succeeded");
    await f.complete("reading");
    await f.until(async () => (await f.status("narrated")).status === "failed");
    expect(f.repo.listJobsByType("acquire").map((job) => job.id)).toEqual([request.jobId!]);
    expect(f.repo.listJobsByType("import")).toEqual([]);
    const response = JSON.stringify(await f.caller.book.audioEditionRequests(f.params));
    expect(response).not.toMatch(/private tracker|never-expose|\.torrent/);
    expect((await f.playback()).audioOptions).toEqual([]);
  } finally { await f.close(); }
}, 15_000);
