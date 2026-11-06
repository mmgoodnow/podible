import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import bencode from "bencode";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { inspectMetainfo } from "../../src/media/metainfo";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { acquireTv, tvAcquisitionBatch } from "../../src/tv/acquisition";
import { PlexTvLibrary } from "../../src/tv/plex";
import { tvRoots, tvViewEntries, type TvPublishedFile } from "../../src/tv/publication";
import type { TvServices } from "../../src/tv/runtime";
import type { TvCatalogEntry } from "../../src/tv/types";
import type { DownloadLocation, TorrentRef, TorrentSnapshot } from "../../src/media/torrent-client";
import { tvSourceSchema } from "../../src/tv/source-reuse";
import type { WorkerContext } from "../../src/worker/context";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createFixtureMovieServices, fixtureMovie } from "../fixtures/movie-services";
import { RecoveryService } from "../../src/media/recovery";
import { randomUUID } from "node:crypto";
import { crossSeedOffer } from "../../src/media/cross-seed";

function catalog(): TvCatalogEntry {
  return {
    key: "anidb:11826", tvdbId: 307375, tmdbId: null, title: "Anime Work Two", aliases: ["Anime Season 2"], year: 2019,
    anime: true, source: "fixture:explicit-anime-work", relations: [],
    episodes: [201, 202, 203, 204].map((id, index) => ({
      key: `tvdb:${id}`, title: `Episode ${index + 1}`, airDate: index === 3 ? "2099-01-01" : "2019-01-01", runtimeMinutes: 24,
      numbering: [
        { scheme: "anidb:episode", season: 1, episode: index + 1, absolute: index + 1, source: "anidb:11826:explicit-map" },
        { scheme: "tvdb:aired", season: 2, episode: index + 1, absolute: 13 + index, source: "tvdb:307375:aired" },
      ],
    })),
  };
}

function fixtureServices(root: string) {
  const base = createFixtureMovieServices(root);
  const coverage = new Map<string, string[]>();
  const rejected = new Set<string>();
  const selections: { ids: number[]; keys: string[]; absolute: (number | null)[] }[] = [];
  const inspections: { key: string; ids: number[] }[] = [];
  const completed = new Map<string, TorrentSnapshot>();
  const inspectedHashes: string[] = [];
  const packs = new Map<string, { offer: ReturnType<typeof base.addOffer>; metainfo: Uint8Array; bytes: Buffer[]; keys: string[]; location?: DownloadLocation }>();
  let packOffers: ReturnType<typeof base.addOffer>[] = [];
  let resumes = 0;
  let sourceAdds = 0;
  const services: TvServices = {
    search: async () => { const found = await base.services.search(fixtureMovie); return { ...found, offers: [...found.offers, ...packOffers] }; },
    client: { ...base.services.client, ensureAdded: async (input) => {
      expect(input.location.rootId).toBe("tv");
      sourceAdds++;
      const metadata = inspectMetainfo(input.metainfo), pack = packs.get(metadata.infoHash);
      if (pack) {
        if (!pack.location) {
          base.stats.snatches++; pack.location = input.location;
          for (const [i, file] of metadata.files.entries()) {
            const destination = path.join(root, "downloads", input.location.relativePath, file.path);
            await fs.mkdir(path.dirname(destination), { recursive: true });
            await fs.writeFile(destination, pack.bytes[i]!);
          }
        }
        return { clientId: base.services.client.id, nativeId: metadata.infoHash };
      }
      return base.services.client.ensureAdded(input);
    }, inspect: async (ref) => {
      inspectedHashes.push(ref.nativeId);
      const pack = packs.get(ref.nativeId);
      if (pack?.location) {
        const size = inspectMetainfo(pack.metainfo).size;
        return { ref, location: pack.location, state: "seeding", complete: true, bytesDone: size, sizeBytes: size,
          downloadBytesPerSecond: 0, message: null, observedAt: new Date().toISOString() };
      }
      const prior = completed.get(ref.nativeId);
      if (prior) return { ...prior, observedAt: new Date(Date.parse(prior.observedAt) + 1000).toISOString() };
      const observation = await base.services.client.inspect(ref);
      if (observation.complete) completed.set(ref.nativeId, observation);
      return observation;
    }, files: async (ref) => {
      const pack = packs.get(ref.nativeId);
      return pack?.location ? inspectMetainfo(pack.metainfo).files.map((file, index) => ({ index,
        path: path.join(root, "downloads", pack.location!.relativePath, file.path) })) : base.services.client.files(ref);
    }, resume: async (ref) => { resumes++; return base.services.client.resume(ref); } },
    library: { scan: () => base.services.library.scan(),
      verify: (_entry, files) => base.services.library.verify(fixtureMovie, files.map((file) => file.serverPath)) },
    fetchMetainfo: (offer) => packs.has(offer.infoHash!) ? Promise.resolve(packs.get(offer.infoHash!)!.metainfo) : base.services.fetchMetainfo(offer),
    probe: async (file) => {
      if ([...packs.values()].some((pack) => file.includes(path.sep + pack.offer.key + path.sep))) {
        base.stats.probes++;
        return { format: { duration: "1440" }, streams: [{ codec_type: "video", codec_name: "h264", width: 1280, height: 720, color_transfer: "bt709" },
          { codec_type: "audio", codec_name: "aac", tags: { language: "eng" } }] };
      }
      return base.services.probe(file);
    },
    timing: { pollMs: 1, stallMs: 1500, plexAttempts: 2, plexRetryMs: 1 },
    decider: () => ({
      select: async ({ episodes, offers }) => {
        selections.push({ ids: episodes.map((ep) => ep.id), keys: episodes.map((ep) => ep.key),
          absolute: episodes.map((ep) => ep.numbering.find((n) => n.scheme === "tvdb:aired")!.absolute) });
        return { offer: offers.find((offer) => coverage.get(offer.key)?.some((key) => episodes.some((ep) => ep.key === key))) ?? null,
          reason: "Fixture selection by explicit stable episode-key membership" };
      },
      inspect: async ({ offer, episodes, files }) => {
        inspections.push({ key: offer.key, ids: episodes.map((episode) => episode.id) });
        const pack = [...packs.values()].find((pack) => pack.offer.key === offer.key);
        if (pack) return { files: files.flatMap((file) => {
          const episode = episodes.find((episode) => episode.key === pack.keys[file.index]);
          return episode ? [{ fileIndex: file.index, episodeIds: [episode.id], facts: { identityVerified: true, cut: null, source: "web", proper: false }, reason: "Exact fixture pack file membership" }] : [];
        }), reason: "Independently mapped only requested whole episodes" };
        const episodeIds = episodes.filter((ep) => coverage.get(offer.key)?.includes(ep.key)).map((ep) => ep.id);
        return { files: !files.length || rejected.has(offer.key) || !episodeIds.length ? [] : [{
          fileIndex: files[0]!.index, episodeIds,
          facts: { identityVerified: true, cut: "theatrical", source: "web", proper: false },
          reason: "Explicit synthetic complete-episode coverage",
        }], reason: rejected.has(offer.key) ? "Downloaded content is not the requested episodes" : "Fixture episode identity reviewed" };
      },
    }),
  };
  return {
    ...base, services, selections, inspections, inspectedHashes, packs, get sourceAdds() { return sourceAdds; }, get resumes() { return resumes; },
    clearOffers: () => { base.clearOffers(); packOffers = []; },
    addPack: (key: string, episodeKeys: string[]) => {
      const bytes = episodeKeys.map((episode) => Buffer.from(`synthetic whole episode ${episode}`));
      const combined = Buffer.concat(bytes), pieceLength = 16384;
      const pieces = Buffer.concat(Array.from({ length: Math.ceil(combined.length / pieceLength) }, (_, index) =>
        createHash("sha1").update(combined.subarray(index * pieceLength, (index + 1) * pieceLength)).digest()));
      const metainfo = new Uint8Array(bencode.encode({ announce: "https://tracker.invalid/private-passkey",
        info: { name: key, files: bytes.map((value, index) => ({ length: value.length, path: [`episode-${index + 1}.mkv`] })), "piece length": pieceLength, pieces } }));
      const hash = inspectMetainfo(metainfo).infoHash;
      const offer = { key, title: `Fixture season ${key}`, provider: "Fixture", sizeBytes: combined.length, seeders: 1,
        url: `https://fixture.invalid/${key}`, infoHash: hash, attributes: {} };
      packs.set(hash, { offer, metainfo, bytes, keys: episodeKeys }); packOffers.push(offer); coverage.set(key, episodeKeys);
      return offer;
    },
    addRelease: (key: string, episodeKeys: string[], options: { stalled?: boolean; invalid?: boolean } = {}) => {
      coverage.set(key, episodeKeys);
      if (options.invalid) rejected.add(key);
      return base.addOffer(key, 1280, options.stalled);
    },
  };
}

async function setup(keys = ["tvdb:201", "tvdb:202", "tvdb:203", "tvdb:204"], input = catalog()) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "tv-acquire-")));
  const appPath = path.join(root, "app.sqlite");
  let db = new Database(appPath);
  runMigrations(db);
  let repo = new BooksRepo(db);
  const settings = defaultSettings();
  settings.agents.apiKey = "";
  settings.tv = { ...settings.tv, managedRoot: root, plexViewPath: "/data/views/plex/tv" };
  repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "local", providerUserId: "tv-acquisition", username: "viewer", isAdmin: false });
  const request = repo.tv.request(user.id, input, defaultMovieQualityPolicy, keys);
  const fixture = fixtureServices(root);
  const context: WorkerContext = { get repo() { return repo; }, getSettings: () => repo.getSettings(), onLog() {}, tvServices: fixture.services };
  const enqueue = (reason: "initial" | "recovery" = "initial") => repo.tv.startAcquisition(request.targetId, reason, (id) =>
    repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id }, maxAttempts: 1 }).id, request.requestId).acquisition;
  return {
    root, request, fixture, context, enqueue, roots: tvRoots(root, settings.tv.plexViewPath),
    get repo() { return repo; }, get db() { return db; },
    reopen: () => { db.close(); db = new Database(appPath); repo = new BooksRepo(db); },
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

type Runtime = Awaited<ReturnType<typeof createJobExecutionRuntime>>;
test("TV completion retains researched copies and queues them before publication, including restart", async () => {
  const s = await setup(["tvdb:201"]);
  try {
    const primary = s.fixture.addRelease("primary", ["tvdb:201"]);
    const copy = { ...primary, key: "research-copy", provider: "Other", url: "https://other.invalid/private" };
    const decider = s.fixture.services.decider;
    s.fixture.services.decider = entry => {
      const original = decider(entry);
      return { ...original, select: async input => ({ ...await original.select(input), matchingOffers: [copy] }) };
    };
    s.fixture.setPlexVerified(false);
    const acquisition = s.enqueue();
    const job = s.repo.getJob(acquisition.job_id!)!;
    await expect(acquireTv(s.context, job, s.fixture.services)).rejects.toThrow("Plex did not verify");
    const [receipt] = s.repo.crossSeed.receipts();
    expect(receipt).toBeDefined();
    expect(crossSeedOffer(s.repo, receipt!.command).key).toBe(copy.key);
    expect(s.fixture.stats.snatches).toBe(1);
    s.reopen();
    s.fixture.setPlexVerified(true);
    await acquireTv(s.context, s.repo.getJob(job.id)!, s.fixture.services);
    expect(s.repo.crossSeed.receipts()).toHaveLength(1);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(1);
  } finally { await s.close(); }
});
async function until(runtime: Runtime, predicate: () => boolean, description: () => string) {
  const deadline = Date.now() + 8000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`TV Reflow did not finish: ${description()}`);
    await runtime.reconcile();
    await Bun.sleep(15);
  }
}
async function assertOwnedViews(s: Awaited<ReturnType<typeof setup>>) {
  const views = tvViewEntries(s.repo.tv, s.roots);
  for (const view of views) {
    const link = path.join(s.roots.viewRoot, view.relativePath);
    expect((await fs.lstat(link)).isSymbolicLink()).toBe(true);
    expect(await fs.realpath(link)).toBe(view.contentPath);
    expect(view.contentPath.startsWith(`${s.roots.contentRoot}/files/`)).toBe(true);
    expect(await fs.readFile(link)).toEqual(await fs.readFile(view.contentPath));
  }
  return views;
}

function extendRequest(s: Awaited<ReturnType<typeof setup>>, keys: string[]) {
  const request = s.repo.tv.requestById(s.request.requestId)!;
  const episodes = s.repo.tv.entry(s.request.entryId)!.episodes;
  return s.repo.tv.requestEpisodes(request.user_id, s.request.entryId, defaultMovieQualityPolicy,
    episodes.filter((episode) => keys.includes(episode.key)).map((episode) => episode.id));
}

function seasonCatalog(count: number, future = false) {
  const entry = catalog();
  entry.episodes = Array.from({ length: count }, (_, index) => ({ ...structuredClone(entry.episodes[0]!), key: `tvdb:${201 + index}`,
    title: `Episode ${index + 1}`, airDate: future && index === count - 1 ? "2099-01-01" : "2019-01-01",
    numbering: [{ scheme: "tvdb:aired", season: 2, episode: index + 1, absolute: index + 13, source: "explicit-fixture" }] }));
  return entry;
}

test("large-series execution is bounded by season without truncating durable requested coverage", async () => {
  const entry = seasonCatalog(130);
  for (const [index, episode] of entry.episodes.entries()) {
    episode.numbering[0]!.season = index < 65 ? 2 : 3;
    episode.numbering[0]!.episode = index % 65 + 1;
  }
  const s = await setup(entry.episodes.map(ep => ep.key), entry);
  try {
    const primary = s.fixture.addPack("large-series", entry.episodes.map(ep => ep.key));
    const copy = { ...primary, key: "same-pack-other-tracker", provider: "Other" };
    const decider = s.fixture.services.decider;
    s.fixture.services.decider = entry => {
      const original = decider(entry);
      return { ...original, select: async input => ({ ...await original.select(input), matchingOffers: [copy] }) };
    };
    const batches: number[] = [];
    while (s.repo.tv.missingEpisodes(s.request.requestId).length) {
      const next = s.enqueue();
      await acquireTv(s.context, s.repo.getJob(next.job_id!)!, s.fixture.services);
      batches.push(s.repo.tv.activeEpisodes(s.request.targetId).length);
      const receipts = s.repo.crossSeed.receipts();
      expect(receipts).toHaveLength(1);
      expect(crossSeedOffer(s.repo, receipts[0]!.command).key).toBe(copy.key);
    }
    expect(batches).toEqual([64, 65, 129, 130]);
    expect(s.repo.tv.requestedEpisodes(s.request.requestId)).toHaveLength(130);
    expect(s.fixture.selections).toHaveLength(1);
    expect(s.fixture.selections[0]!.ids).toHaveLength(64);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.fixture.inspections.every(call => call.ids.length <= 64)).toBe(true);
    expect(await assertOwnedViews(s)).toHaveLength(130);
    expect(tvAcquisitionBatch([])).toEqual([]);
    const all = s.repo.tv.requestedEpisodes(s.request.requestId);
    expect(tvAcquisitionBatch([...all].reverse()).map(ep => ep.id)).toEqual(all.slice(0, 64).map(ep => ep.id));
  } finally { await s.close(); }
});

test("canonical loss invalidates only its exact TV file coverage and reimports a retained pack without replacing surviving episodes", async () => {
  const entry = seasonCatalog(3), s = await setup(entry.episodes.map(ep => ep.key), entry);
  let runtime: Runtime | undefined;
  try {
    const key = { namespace: "tv-target" as const, id: String(s.request.targetId) };
    s.repo.retention.request(key, { expectedRevision: 0, reason: "Explicit recovery test demand" });
    s.fixture.addPack("loss-season", entry.episodes.map(ep => ep.key));
    const first = s.enqueue();
    runtime = await createJobExecutionRuntime(s.context, path.join(s.root, "execution.sqlite"), { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(first.job_id!)?.status === "succeeded", () => "Initial TV publication");
    const version = s.repo.tv.versionSources(s.request.targetId)[0]!.version, missing = version.files[1]!;
    const survivor = path.join(s.roots.contentRoot, version.files[0]!.relativePath), before = await fs.stat(survivor);
    await fs.unlink(path.join(s.roots.contentRoot, missing.relativePath));
    const recovery = new RecoveryService(s.repo), resource = recovery.preview({ targets: [key] }).resources[0]!;
    expect(resource.action, JSON.stringify(resource.reasons)).toBe("queue-acquisition");
    const result = recovery.recover({ operationId: randomUUID(), targets: [{ key, expectedRevision: resource.revision, action: "queue-acquisition" }] });
    expect(result.results[0]!.status).toBe("queued");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(2);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map(ep => ep.id)).toEqual(missing.episodeIds);
    await until(runtime, () => result.results[0]!.jobIds.every(id => s.repo.getJob(id)?.status === "succeeded"), () => "TV loss recovery");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(3);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toHaveLength(0);
    const after = await fs.stat(survivor);
    expect([after.ino, after.mtimeMs]).toEqual([before.ino, before.mtimeMs]);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.fixture.stats.searches).toBe(1);
    await assertOwnedViews(s);
    expect(() => s.repo.tv.stagePublication(s.request.targetId, version.id)).toThrow("confirmed missing");
  } finally { await runtime?.stop(); await s.close(); }
});

test("a successful season pack among failed sibling bindings supplies all newly approved aired episodes without another search or snatch", async () => {
  const input = seasonCatalog(53, true), keys = input.episodes.map((episode) => episode.key);
  const s = await setup([keys[0]!], input);
  try {
    s.fixture.addRelease("wrong-pack", [keys[0]!], { invalid: true });
    s.fixture.addRelease("stalled-pack", [keys[0]!], { stalled: true });
    const offer = s.fixture.addPack("complete-season", keys);
    await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    const first = s.repo.tv.versionSources(s.request.targetId)[0]!.version;
    const source = tvSourceSchema.parse((first.facts as { source: unknown }).source);
    expect(source.ref.nativeId).toBe(offer.infoHash!);
    expect(source.manifest.files).toHaveLength(53);
    expect(JSON.stringify(source)).not.toContain("private-passkey");
    const original = (await assertOwnedViews(s))[0]!;
    const counters = { searches: s.fixture.stats.searches, snatches: s.fixture.stats.snatches, adds: s.fixture.sourceAdds, resumes: s.fixture.resumes };
    expect(counters.snatches).toBe(3);
    extendRequest(s, keys);
    s.fixture.inspectedHashes.length = 0;
    await acquireTv(s.context, s.repo.getJob(s.enqueue("recovery").job_id!)!, s.fixture.services);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(52);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map((episode) => episode.key)).toEqual([keys[52]!]);
    expect(s.fixture.inspections.at(-1)!.ids).toHaveLength(51);
    expect(s.fixture.inspectedHashes.every((hash) => hash === offer.infoHash)).toBe(true);
    expect({ searches: s.fixture.stats.searches, snatches: s.fixture.stats.snatches, adds: s.fixture.sourceAdds, resumes: s.fixture.resumes }).toEqual(counters);
    const views = await assertOwnedViews(s);
    expect(views.find((view) => view.relativePath === original.relativePath)?.contentPath).toBe(original.contentPath);
    expect(s.repo.tv.versionSources(s.request.targetId)).toHaveLength(2);
  } finally { await s.close(); }
});

test("nine fully accounted single-file sources do not cause probe or agent passes when episode ten is missing", async () => {
  const input = seasonCatalog(10), keys = input.episodes.map((episode) => episode.key);
  const s = await setup([keys[0]!], input);
  try {
    for (let index = 0; index < 9; index++) {
      extendRequest(s, keys.slice(0, index + 1));
      s.fixture.addRelease(`single-${index}`, [keys[index]!]);
      await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    }
    const before = { probes: s.fixture.stats.probes, inspections: s.fixture.inspections.length };
    extendRequest(s, keys);
    s.fixture.addRelease("single-ten", [keys[9]!]);
    s.fixture.inspectedHashes.length = 0;
    await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    expect(s.fixture.stats.probes - before.probes).toBe(1);
    expect(s.fixture.inspections.length - before.inspections).toBe(1);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    expect(s.repo.tv.events(s.request.targetId).some((event) => event.message.includes("Skipped 9 verified source(s)"))).toBe(true);
    expect(new Set(s.fixture.inspectedHashes)).toHaveLength(1);
  } finally { await s.close(); }
});

test.each(["probe:1", "adopt:1"])("a replaced pack source cannot replay cached %s evidence and falls back without replacing active links", async (checkpoint) => {
  const s = await setup(["tvdb:201"]);
  try {
    const offer = s.fixture.addPack("changed-pack", ["tvdb:201", "tvdb:202"]);
    await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    const original = (await assertOwnedViews(s))[0]!;
    extendRequest(s, ["tvdb:201", "tvdb:202"]);
    const recovery = s.enqueue("recovery"), job = s.repo.getJob(recovery.job_id!)!;
    const saved = new Map<string, unknown>(); let crash = true;
    const execution: NonNullable<WorkerContext["execution"]> = { key: "source-replay", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>) => {
        if (saved.has(name)) return saved.get(name) as T;
        const value = await action(); saved.set(name, value);
        if (crash && name === `tv:reuse:0:${checkpoint}`) { crash = false; throw new Error("fixture stopped after receipt"); }
        return value;
      } };
    await expect(acquireTv({ ...s.context, execution }, job, s.fixture.services)).rejects.toThrow("fixture stopped after receipt");
    expect(saved.has("tv:reuse:0:source-evidence")).toBe(true);
    const files = await s.fixture.services.client.files({ clientId: s.fixture.services.client.id, nativeId: offer.infoHash! });
    const changed = files[1]!.path, bytes = await fs.readFile(changed);
    await fs.writeFile(changed, Buffer.alloc(bytes.length, "x"));
    s.fixture.addRelease("fresh-second", ["tvdb:202"]);
    await acquireTv({ ...s.context, execution }, job, s.fixture.services);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    const views = await assertOwnedViews(s);
    expect(views.find((view) => view.relativePath === original.relativePath)?.contentPath).toBe(original.contentPath);
    expect((await fs.readFile(views.find((view) => view.contentPath !== original.contentPath)!.contentPath, "utf8"))).toContain("fresh-second");
    expect(s.fixture.sourceAdds).toBe(2);
    expect(s.repo.tv.versionSources(s.request.targetId)).toHaveLength(2);
    expect(s.repo.tv.events(s.request.targetId).some((event) => event.message.includes("no longer safely reusable"))).toBe(true);
  } finally { await s.close(); }
});

test("an old cached workflow retries delayed Plex without regenerating its plan, then season recovery proves the exact legacy pack", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    const offer = s.fixture.addPack("legacy-season", ["tvdb:201", "tvdb:202"]);
    const decider = s.fixture.services.decider;
    let explanations = 0;
    s.fixture.services.decider = (entry) => ({ ...decider(entry), inspect: async (input) => {
      const plan = await decider(entry).inspect(input);
      return { ...plan, reason: `Nondeterministic explanation ${++explanations}` };
    } });
    s.fixture.setPlexVerified(false);
    const initial = s.enqueue(), executionPath = path.join(s.root, "execution.sqlite");
    const inspect = s.fixture.services.client.inspect; let crash = true;
    s.fixture.services.client.inspect = async (ref) => { if (crash) { crash = false; throw new Error("legacy interruption after snatch"); } return inspect(ref); };
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(initial.job_id!)?.status === "failed", () => "waiting for legacy interruption");
    const runId = s.repo.getJobExecution(initial.job_id!)!.run_id;
    await runtime.stop(); runtime = undefined;
    const journal = new Database(executionPath);
    try {
      const row = journal.query("SELECT output FROM workflow_steps WHERE run_id=? AND name='tv:target-snapshot'").get(runId) as { output: string };
      const prior = JSON.parse(row.output);
      journal.query("UPDATE workflow_steps SET output=? WHERE run_id=? AND name='tv:target-snapshot'")
        .run(JSON.stringify({ episodeIds: prior.episodeIds, priorKeys: prior.priorKeys, policy: prior.policy }), runId);
    } finally { journal.close(); }
    extendRequest(s, ["tvdb:201", "tvdb:202"]);
    s.fixture.clearOffers();
    s.repo.retryJob(initial.job_id!);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => s.repo.getJob(initial.job_id!)?.status === "failed", () => "waiting for legacy delayed Plex");
    expect(s.repo.getJob(initial.job_id!)?.error).toContain("Plex did not verify");
    expect(explanations).toBe(1);
    const recordedVersion = s.repo.tv.publications()[0]!.version_id;
    await runtime.stop(); runtime = undefined;
    s.fixture.setPlexVerified(true);
    s.repo.retryJob(initial.job_id!);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => {
      const failed = s.repo.tv.acquisitions(s.request.targetId).find((row) => row.job_id && s.repo.getJob(row.job_id)?.status === "failed");
      if (failed) throw new Error(`Legacy recovery failed: ${s.repo.getJob(failed.job_id!)?.error}`);
      return s.repo.tv.missingEpisodes(s.request.requestId).length === 0 && s.repo.tv.acquisitions(s.request.targetId).every((row) => !!row.finished_at);
    }, () => "waiting for legacy pack continuation");
    const versions = s.repo.tv.versionSources(s.request.targetId);
    expect(versions).toHaveLength(2);
    expect(versions[1]!.version.id).toBe(recordedVersion);
    expect(explanations).toBe(2); // One initial inspection, one for the new missing episode; none for recorded-version replay.
    expect(versions[1]!.version.facts).not.toHaveProperty("source");
    expect(tvSourceSchema.parse((versions[0]!.version.facts as { source: unknown }).source).ref.nativeId).toBe(offer.infoHash!);
    const status = await runtime.engine.getRunStatus(runId);
    expect(status!.steps.some((step) => step.name === "tv:attempt:0:metainfo:request:0" && step.status === "completed")).toBe(true);
    expect(status!.steps.some((step) => step.name.includes("source-evidence") || step.name.startsWith("tv:reuse:"))).toBe(false);
    expect(s.fixture.stats.searches).toBe(1);
    expect(s.fixture.sourceAdds).toBe(1);
    await assertOwnedViews(s);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test.each(["before-adopt", "after-adopt"])("legacy cached verification cannot authorize replaced bytes after interruption %s", async (checkpoint) => {
  const s = await setup(["tvdb:201"]);
  try {
    const offer = s.fixture.addRelease(`legacy-${checkpoint}`, ["tvdb:201"]);
    const acquisition = s.enqueue(), job = s.repo.getJob(acquisition.job_id!)!;
    const saved = new Map<string, unknown>([["tv:target-snapshot", { episodeIds: s.request.episodeIds, priorKeys: [], policy: defaultMovieQualityPolicy }]]);
    let crash = true, probes = 0;
    const probe = s.fixture.services.probe;
    s.fixture.services.probe = async (file) => {
      probes++;
      if (checkpoint === "before-adopt" && (await fs.readFile(file))[0] === 88) throw new Error("Fresh probe rejects replacement");
      return probe(file);
    };
    const execution: NonNullable<WorkerContext["execution"]> = { key: "legacy-source-replay", assertActive() {}, sleep: async () => {},
      run: async <T>(name: string, action: () => T | Promise<T>) => {
        if (saved.has(name)) return saved.get(name) as T;
        if (crash && checkpoint === "before-adopt" && name === "tv:attempt:0:adopt:0") { crash = false; throw new Error("legacy test interruption"); }
        const value = await action(); saved.set(name, value);
        if (crash && checkpoint === "after-adopt" && name === "tv:attempt:0:adopt:0") { crash = false; throw new Error("legacy test interruption"); }
        return value;
      } };
    await expect(acquireTv({ ...s.context, execution }, job, s.fixture.services)).rejects.toThrow("legacy test interruption");
    const source = (await s.fixture.services.client.files({ clientId: s.fixture.services.client.id, nativeId: offer.infoHash! }))[0]!.path;
    const oldBytes = await fs.readFile(source);
    await fs.writeFile(source, Buffer.alloc(oldBytes.length, 88));
    if (checkpoint === "after-adopt") {
      await expect(acquireTv({ ...s.context, execution }, job, s.fixture.services)).rejects.toThrow("different bytes");
      const stored = saved.get("tv:attempt:0:adopt:0") as { relativePath: string };
      expect(await fs.readFile(path.join(s.roots.contentRoot, stored.relativePath))).toEqual(oldBytes);
    } else await acquireTv({ ...s.context, execution }, job, s.fixture.services);
    expect(probes).toBe(2);
    expect(s.fixture.inspections).toHaveLength(2);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([]);
    expect(s.repo.tv.versionSources(s.request.targetId)).toEqual([]);
    expect(s.fixture.sourceAdds).toBe(1);
    expect([...saved.keys()].some((name) => name.includes("source-evidence"))).toBe(false);
  } finally { await s.close(); }
});

test.each(["replaced-copies", "missing-receipt"])("legacy recorded publication requires the original adoption digest: %s", async (failure) => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    const offer = s.fixture.addPack(`legacy-proof-${failure}`, ["tvdb:201", "tvdb:202"]);
    s.fixture.setPlexVerified(false);
    const initial = s.enqueue(), executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1,
      processJob: (ctx, job) => acquireTv({ ...ctx, execution: { ...ctx.execution!,
        run: <T>(name: string, action: () => T | Promise<T>) => ctx.execution!.run(name, async () => {
          const value = await action();
          if (name !== "tv:target-snapshot") return value;
          const snapshot = value as { episodeIds: number[]; priorKeys: string[]; policy: unknown };
          return { episodeIds: snapshot.episodeIds, priorKeys: snapshot.priorKeys, policy: snapshot.policy } as T;
        }),
      } }, job, s.fixture.services),
    });
    await until(runtime, () => s.repo.getJob(initial.job_id!)?.status === "failed", () => "waiting for legacy staged publication");
    expect(s.repo.getJob(initial.job_id!)?.error).toContain("Plex did not verify");
    const staged = s.repo.tv.publications()[0]!, version = s.repo.tv.version(staged.version_id)!;
    const runId = s.repo.getJobExecution(initial.job_id!)!.run_id;
    await runtime.stop(); runtime = undefined;
    const counters = { probes: s.fixture.stats.probes, inspections: s.fixture.inspections.length, adds: s.fixture.sourceAdds };
    if (failure === "replaced-copies") {
      const source = (await s.fixture.services.client.files({ clientId: s.fixture.services.client.id, nativeId: offer.infoHash! }))[0]!.path;
      const replacement = Buffer.alloc((await fs.stat(source)).size, "x");
      await fs.writeFile(source, replacement);
      const canonical = path.join(s.roots.contentRoot, version.files[0]!.relativePath);
      await fs.chmod(canonical, 0o600);
      await fs.writeFile(canonical, replacement);
      await fs.chmod(canonical, 0o444);
    } else {
      const journal = new Database(executionPath);
      try { journal.query("DELETE FROM workflow_steps WHERE run_id=? AND name='tv:attempt:0:adopt:0'").run(runId); }
      finally { journal.close(); }
    }
    s.fixture.setPlexVerified(true);
    s.repo.retryJob(initial.job_id!);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => s.repo.getJob(initial.job_id!)?.status === "failed", () => "waiting for legacy digest rejection");
    expect(s.repo.getJob(initial.job_id!)?.error).toContain(failure === "replaced-copies" ? "original verified digest" : "Cannot prove");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([]);
    expect(s.repo.tv.publications()).toEqual([staged]);
    expect({ probes: s.fixture.stats.probes, inspections: s.fixture.inspections.length, adds: s.fixture.sourceAdds }).toEqual(counters);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("real Reflow restart publishes an already recorded reused pack version without extra searches, probes, sources or versions", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    s.fixture.addPack("replayed-season", ["tvdb:201", "tvdb:202"]);
    const initial = s.enqueue(), executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(initial.job_id!)?.status === "succeeded", () => "waiting for first episode");
    const original = (await assertOwnedViews(s))[0]!;
    extendRequest(s, ["tvdb:201", "tvdb:202"]);
    s.fixture.setPlexVerified(false);
    const recovery = s.enqueue("recovery");
    await until(runtime, () => s.repo.getJob(recovery.job_id!)?.status === "failed", () => "waiting for staged reused version");
    const staged = s.repo.tv.publications()[0]!;
    const before = { searches: s.fixture.stats.searches, probes: s.fixture.stats.probes, adds: s.fixture.sourceAdds, versions: s.repo.tv.versionSources(s.request.targetId).length };
    expect((s.repo.tv.version(staged.version_id)!.facts as { attemptKey: string }).attemptKey).toBe("tv:reuse:0");
    await runtime.stop(); runtime = undefined;
    s.reopen(); s.fixture.setPlexVerified(true); s.repo.retryJob(recovery.job_id!);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(recovery.job_id!)?.status === "succeeded", () => "waiting for recorded-source replay");
    expect({ searches: s.fixture.stats.searches, probes: s.fixture.stats.probes, adds: s.fixture.sourceAdds, versions: s.repo.tv.versionSources(s.request.targetId).length }).toEqual(before);
    expect(s.repo.tv.publications()).toEqual([]);
    expect(s.repo.tv.activeEpisodes(s.request.targetId).some((episode) => episode.version_id === staged.version_id)).toBe(true);
    expect((await assertOwnedViews(s)).find((view) => view.relativePath === original.relativePath)?.contentPath).toBe(original.contentPath);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("native Reflow publishes anime partial packs by stable IDs, continues only missing aired episodes and preserves future intent", async () => {
  const s = await setup();
  let runtime: Runtime | undefined;
  try {
    const firstOffer = s.fixture.addRelease("absolute-13-14", ["tvdb:201", "tvdb:202"]);
    s.fixture.addRelease("absolute-15", ["tvdb:203"]);
    const initial = s.enqueue();
    runtime = await createJobExecutionRuntime(s.context, path.join(s.root, "execution.sqlite"), { retryDelayMs: 1 });
    await until(runtime, () => {
      const failed = s.repo.tv.acquisitions(s.request.targetId).find((a) => a.job_id && s.repo.getJob(a.job_id)?.status === "failed");
      if (failed) throw new Error(`Native TV job failed: ${s.repo.getJob(failed.job_id!)?.error}`);
      return s.repo.tv.missingEpisodes(s.request.requestId).length === 1 &&
        s.repo.tv.acquisitions(s.request.targetId).every((a) => a.job_id && s.repo.getJob(a.job_id)?.status === "succeeded");
    }, () => JSON.stringify(s.repo.tv.acquisitions(s.request.targetId).map((a) => s.repo.getJob(a.job_id!))));
    expect(s.repo.getJob(initial.job_id!)!.status).toBe("succeeded");
    expect(s.fixture.selections.map((selection) => selection.keys)).toEqual([["tvdb:201", "tvdb:202", "tvdb:203"], ["tvdb:203"]]);
    expect(s.fixture.selections[0]!.absolute).toEqual([13, 14, 15]);
    expect(s.fixture.selections[0]!.ids).toEqual(s.request.episodeIds.slice(0, 3));
    expect(s.repo.tv.requestedEpisodeIds(s.request.requestId)).toEqual(s.request.episodeIds);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:204"]);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toHaveLength(3);
    expect(s.repo.tv.acquisitions(s.request.targetId)).toHaveLength(2);
    expect(s.fixture.sourceAdds).toBe(2);
    const views = await assertOwnedViews(s);
    expect(views.map((view) => view.relativePath).some((name) => name.includes("S02E01-E02"))).toBe(true);
    expect(views.map((view) => view.relativePath).some((name) => name.includes("S02E03"))).toBe(true);
    const metainfo = await s.fixture.services.fetchMetainfo(firstOffer);
    const metadata = inspectMetainfo(metainfo);
    const download = s.fixture.downloads.get(metadata.infoHash)!;
    const source = await fs.readFile(path.join(s.root, "downloads", download.location.relativePath, metadata.name));
    const decoded = bencode.decode(Buffer.from(metainfo)) as { info: { pieces: Buffer } };
    expect(source.length).toBe(metadata.size);
    expect(createHash("sha1").update(source).digest()).toEqual(Buffer.from(decoded.info.pieces));
    expect(await fs.readFile(path.join(s.roots.contentRoot, s.repo.tv.version(s.repo.tv.activeEpisodes(s.request.targetId)[0]!.version_id)!.files[0]!.relativePath))).toEqual(source);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("invalid TV import and stalled source are retained while a different complete release is reacquired", async () => {
  const s = await setup(["tvdb:201", "tvdb:202"]);
  try {
    s.fixture.addRelease("wrong-episodes", ["tvdb:201", "tvdb:202"], { invalid: true });
    s.fixture.addRelease("stalled-pack", ["tvdb:201", "tvdb:202"], { stalled: true });
    s.fixture.addRelease("valid-pack", ["tvdb:201", "tvdb:202"]);
    const acquisition = s.enqueue();
    await acquireTv(s.context, s.repo.getJob(acquisition.job_id!)!, s.fixture.services);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    expect(s.fixture.sourceAdds).toBe(3);
    expect(s.fixture.stats.quiesces).toBe(1);
    expect(s.fixture.stats.removals).toBe(0);
    expect(s.fixture.downloads.size).toBe(3);
    const invalid = [...s.fixture.downloads.values()][0]!;
    expect(await fs.readFile(path.join(s.root, "downloads", invalid.location.relativePath, "wrong-episodes.mkv"), "utf8")).toContain("wrong-episodes");
    const views = await assertOwnedViews(s);
    expect(await fs.readFile(views[0]!.contentPath, "utf8")).toContain("valid-pack");
    const events = s.repo.tv.events(s.request.targetId);
    expect(events.some((event) => event.stage === "replacing" && event.message.includes("not the requested"))).toBe(true);
    expect(events.some((event) => event.stage === "replacing" && event.message.includes("stalled"))).toBe(true);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
  } finally { await s.close(); }
});

test("wrong Plex episode GUID blocks activation despite correct aired ordinals and retains old playable links", async () => {
  const s = await setup(["tvdb:201", "tvdb:202"]);
  try {
    s.fixture.addRelease("old-first-episode", ["tvdb:201"]);
    await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
    const before = s.repo.tv.activeEpisodes(s.request.targetId);
    const original = (await assertOwnedViews(s))[0]!;
    const oldLink = path.join(s.roots.viewRoot, original.relativePath);
    const bytes = await fs.readFile(oldLink);
    s.fixture.addRelease("new-second-episode", ["tvdb:202"]);
    let stagedFiles: TvPublishedFile[] = [];
    const plex = new PlexTvLibrary({ url: "http://plex.invalid", token: "fixture-only", sectionId: "9" }, (async (input) => {
      const url = new URL(String(input));
      if (url.pathname === "/library/sections") return Response.json({ MediaContainer: { Directory: [{ key: "9", type: "show" }] } });
      if (url.pathname.endsWith("/refresh")) return new Response("{}");
      if (url.pathname === "/library/sections/9/all")
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: "show", Guid: [{ id: "tvdb://307375" }] }] } });
      if (url.pathname === "/library/metadata/show/allLeaves")
        return Response.json({ MediaContainer: { Metadata: stagedFiles.flatMap((file) => file.episodes.map((ep) => ({
          ratingKey: `wrong-${ep.id}`, parentIndex: 2, index: 2,
          Guid: [{ id: "tvdb://999999" }], Media: [{ Part: [{ file: file.serverPath }] }],
        }))) } });
      throw new Error(`Unexpected Plex request ${url.pathname}`);
    }) as typeof fetch);
    s.fixture.services.library = { scan: () => plex.scan(), verify: (entry, files) => { stagedFiles = files; return plex.verify(entry, files); } };
    const recovery = s.enqueue("recovery");
    await expect(acquireTv(s.context, s.repo.getJob(recovery.job_id!)!, s.fixture.services)).rejects.toThrow("Plex did not verify");
    expect(stagedFiles.flatMap((file) => file.episodes.map((ep) => ep.key))).toEqual(["tvdb:202"]);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual(before);
    expect(s.repo.tv.missingEpisodes(s.request.requestId).map((ep) => ep.key)).toEqual(["tvdb:202"]);
    expect(s.repo.tv.publications()).toHaveLength(1);
    expect(await fs.readFile(oldLink)).toEqual(bytes);
    expect(await fs.realpath(oldLink)).toBe(original.contentPath);
    expect(s.fixture.sourceAdds).toBe(2);
    expect(s.fixture.stats.removals).toBe(0);
  } finally { await s.close(); }
});

test("real Reflow restart replays a checkpointed TV snatch without extra source adds or content versions", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    s.fixture.addRelease("replayed", ["tvdb:201"]);
    const acquisition = s.enqueue();
    const jobId = acquisition.job_id!;
    const inspect = s.fixture.services.client.inspect;
    let interrupted = false;
    s.fixture.services.client.inspect = async (ref) => {
      if (!interrupted) { interrupted = true; throw new Error("fixture process lost after saved snatch"); }
      return inspect(ref);
    };
    const executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(jobId)?.status === "failed", () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJob(jobId)!.error).toContain("fixture process lost");
    const runId = s.repo.getJobExecution(jobId)!.run_id;
    expect(JSON.stringify((await runtime.engine.getRunStatus(runId))?.steps)).toContain("tv:attempt:0:snatch");
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 0 });
    await runtime.stop(); runtime = undefined;
    s.reopen();
    s.repo.retryJob(jobId);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => {
      const current = s.repo.getJob(jobId);
      if (current?.status === "failed") throw new Error(`Replayed native TV job failed: ${current.error}`);
      return current?.status === "succeeded";
    }, () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJobExecution(jobId)!.run_id).toBe(runId);
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.fixture.stats.snatches).toBe(1);
    expect(s.repo.tv.missingEpisodes(s.request.requestId)).toEqual([]);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
    expect(s.db.query("SELECT COUNT(*) AS n FROM managed_content_files").get()).toEqual({ n: 1 });
    await assertOwnedViews(s);
    await runtime.stop(); runtime = undefined;
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("real Reflow retry rechecks caught-up Plex using the same staged TV version without another source add", async () => {
  const s = await setup(["tvdb:201"]);
  let runtime: Runtime | undefined;
  try {
    s.fixture.addRelease("plex-catching-up", ["tvdb:201"]);
    s.fixture.setPlexVerified(false);
    const acquisition = s.enqueue();
    const jobId = acquisition.job_id!;
    const executionPath = path.join(s.root, "execution.sqlite");
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await until(runtime, () => s.repo.getJob(jobId)?.status === "failed", () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.repo.getJob(jobId)!.error).toContain("Plex did not verify");
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([]);
    const staged = s.repo.tv.publications()[0]!;
    expect(staged).toBeDefined();
    const runId = s.repo.getJobExecution(jobId)!.run_id;
    const scansBeforeRetry = s.fixture.stats.scans;
    await runtime.stop(); runtime = undefined;
    s.reopen();
    s.fixture.setPlexVerified(true);
    s.repo.retryJob(jobId);
    runtime = await createJobExecutionRuntime(s.context, executionPath, { retryDelayMs: 1 });
    await runtime.reconcile();
    await until(runtime, () => {
      const current = s.repo.getJob(jobId);
      if (current?.status === "failed")
        throw new Error(`Caught-up Plex was not rechecked (scans ${scansBeforeRetry} -> ${s.fixture.stats.scans}): ${current.error}`);
      return current?.status === "succeeded";
    }, () => JSON.stringify(s.repo.getJob(jobId)));
    expect(s.fixture.stats.scans).toBeGreaterThan(scansBeforeRetry);
    expect(s.repo.getJobExecution(jobId)!.run_id).toBe(runId);
    expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual([{ episode_id: s.request.episodeIds[0]!, version_id: staged.version_id }]);
    expect(s.repo.tv.publications()).toEqual([]);
    expect(s.fixture.sourceAdds).toBe(1);
    expect(s.db.query("SELECT COUNT(*) AS n FROM tv_versions").get()).toEqual({ n: 1 });
    await assertOwnedViews(s);
  } finally { await runtime?.stop(); await s.close(); }
}, 15_000);

test("withdrawal and intentional eviction fence queued recovery without repairing missing links or snatching again", async () => {
  for (const intent of ["withdrawn", "evicted"] as const) {
    const s = await setup(["tvdb:201", "tvdb:202"]);
    try {
      s.fixture.addRelease(`initial-${intent}`, ["tvdb:201"]);
      await acquireTv(s.context, s.repo.getJob(s.enqueue().job_id!)!, s.fixture.services);
      const view = (await assertOwnedViews(s))[0]!;
      const before = s.repo.tv.activeEpisodes(s.request.targetId);
      const recovery = s.enqueue("recovery");
      await fs.unlink(path.join(s.roots.viewRoot, view.relativePath));
      if (intent === "withdrawn") s.repo.tv.withdrawRequest(s.request.requestId);
      else s.repo.retention.evict({ namespace: "tv-target", id: String(s.request.targetId) }, { expectedRevision: 0, reason: "User intentionally evicted TV content" });
      s.fixture.addRelease(`repair-${intent}`, ["tvdb:202"]);
      await expect(acquireTv(s.context, s.repo.getJob(recovery.job_id!)!, s.fixture.services)).rejects.toThrow(intent === "withdrawn" ? "withdrawn" : "intentionally evicted");
      expect(s.fixture.sourceAdds).toBe(1);
      expect(s.repo.tv.activeEpisodes(s.request.targetId)).toEqual(before);
      await expect(fs.lstat(path.join(s.roots.viewRoot, view.relativePath))).rejects.toMatchObject({ code: "ENOENT" });
      expect((await fs.stat(view.contentPath)).isFile()).toBe(true);
      expect(s.fixture.stats.removals).toBe(0);
    } finally { await s.close(); }
  }
});
