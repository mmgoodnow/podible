import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { startFixturePeer } from "./fixture-peer";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createPodibleFetchHandler } from "../../src/http";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { RtorrentAdapter } from "../../src/media/rtorrent-client";
import { hashSessionToken } from "../../src/auth";
import { MovieIndexers, fetchMovieMetainfo } from "../../src/movies/offers";
import { probeMovie } from "../../src/movies/probe";
import {
  assessMovieUpgrade,
  evaluateMovieQuality,
} from "../../src/movies/quality";
import type { MovieServices } from "../../src/movies/operations";
import type { Movie } from "../../src/movies/types";
import { inspectMetainfo } from "../../src/media/metainfo";
import { createMovieDecider } from "../../src/movies/agents";
import { PlexMovieLibrary } from "../../src/media/plex-library";
import { setTracingDisabled } from "@openai/agents";
import {
  Usage,
  type ModelProvider,
  type ModelResponse,
} from "@openai/agents-core";
setTracingDisabled(true);

// stdout is a private HTTP bridge, not a log stream or public API.
console.log = (...args) => console.error(...args);
const send = (value: unknown) =>
  process.stdout.write(JSON.stringify(value) + "\n");
const runId = randomUUID();
const root = `/data/movie-poc-${runId}`;
await fs.mkdir(root);
const movie: Movie = {
  id: 1,
  tmdbId: 101,
  title: "A Fixture Film",
  originalTitle: "A Fixture Film",
  year: 2024,
  overview:
    "A twelve-second synthetic short film with English audio, created for a media-library regression test.",
  runtimeMinutes: 0.2,
  originalLanguage: "en",
  posterUrl: null,
  imdbId: null,
};
const lowerBytes = Buffer.from(
  await Bun.file("/fixtures/movie-720.mp4").arrayBuffer(),
);
const higherBytes = Buffer.from(
  await Bun.file("/fixtures/movie-1080.mp4").arrayBuffer(),
);
const seeds = [
  await startFixturePeer({
    bytes: higherBytes,
    name: `A.Fixture.Film.2024.ReleaseA.${runId}.1080p.WEB.H264.SDR.English.Theatrical.mp4`,
    stalled: true,
  }),
  await startFixturePeer({
    bytes: lowerBytes,
    name: `A.Fixture.Film.2024.${runId}.720p.WEB.H264.SDR.English.Theatrical.mp4`,
  }),
  await startFixturePeer({
    bytes: higherBytes,
    name: `A.Fixture.Film.2024.Upgrade.${runId}.1080p.WEB.H264.SDR.English.Theatrical.mp4`,
  }),
];
let upgrade = false;
let upstreamSearches = 0;
const escape = (value: string) =>
  value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
const tracker = Bun.serve({
  hostname: "0.0.0.0",
  port: 0,
  fetch(request): Response {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/download/")) {
      const seed = seeds[Number(url.pathname.split("/").at(-1))];
      return seed
        ? new Response(seed.metainfo)
        : new Response("", { status: 404 });
    }
    if (url.searchParams.get("t") === "caps")
      return new Response(
        `<?xml version="1.0"?><caps><server version="1.0" title="POC Fixture"/><limits max="100" default="100"/><searching><search available="yes" supportedParams="q"/><movie-search available="yes" supportedParams="q,imdbid,tmdbid"/></searching><categories><category id="2000" name="Movies"><subcat id="2040" name="Movies/HD"/></category></categories></caps>`,
        { headers: { "Content-Type": "application/xml" } },
      );
    upstreamSearches++;
    const items = seeds
      .slice(0, upgrade ? 3 : 2)
      .map(
        (seed, index) =>
          `<item><title>${escape(seed.name)}</title><guid>poc-${runId}-${index}</guid><link>http://plex:${tracker.port}/download/${index}</link><pubDate>${new Date().toUTCString()}</pubDate><category>2040</category><size>${seed.bytes.length}</size><enclosure url="http://plex:${tracker.port}/download/${index}" length="${seed.bytes.length}" type="application/x-bittorrent"/><torznab:attr name="category" value="2040"/><torznab:attr name="seeders" value="1"/><torznab:attr name="infohash" value="${inspectMetainfo(seed.metainfo).infoHash}"/></item>`,
      )
      .join("");
    return new Response(
      `<?xml version="1.0"?><rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel><title>POC Fixture</title><description>Offline fixture tracker</description>${items}</channel></rss>`,
      { headers: { "Content-Type": "application/rss+xml" } },
    );
  },
});
const key = /<ApiKey>([^<]+)<\/ApiKey>/.exec(
  await Bun.file("/prowlarr/config.xml").text(),
)?.[1];
if (!key) throw new Error("Lab Prowlarr credential missing");
async function prowlarr(route: string, method = "GET", body?: unknown) {
  const response = await fetch(`http://prowlarr:9696/api/v1${route}`, {
    method,
    headers: { "X-Api-Key": key!, "Content-Type": "application/json" },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!response.ok) {
    const errors = await response.json().catch(() => []);
    throw new Error(
      `Lab Prowlarr ${route}: ${response.status} ${JSON.stringify(Array.isArray(errors) ? errors.map((entry) => ({ field: entry.propertyName, message: entry.errorMessage })) : {})}`,
    );
  }
  return response.json() as Promise<any>;
}
const schema = ((await prowlarr("/indexer/schema")) as any[]).find(
  (entry) => entry.name === "Generic Torznab",
);
if (!schema) throw new Error("Generic Torznab missing from lab Prowlarr");
const profiles = await prowlarr("/appprofile");
const definition = {
  ...schema,
  id: 0,
  name: `Podible POC ${runId}`,
  enableRss: true,
  enableAutomaticSearch: true,
  enableInteractiveSearch: true,
  appProfileId: profiles[0].id,
  fields: schema.fields.map((field: any) => ({
    ...field,
    value:
      field.name === "baseUrl"
        ? `http://plex:${tracker.port}`
        : field.name === "apiPath"
          ? "/api"
          : field.name === "apiKey"
            ? "fixture-private"
            : field.value,
  })),
};
const indexer = await prowlarr("/indexer", "POST", definition);
async function plex(route: string, method = "GET") {
  const response = await fetch(`http://127.0.0.1:32400${route}`, {
    method,
    headers: { Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Lab Plex: ${response.status}`);
  return response;
}
const viewRoot = `${root}/views/plex/movies`;
await fs.mkdir(viewRoot, { recursive: true });
const params = new URLSearchParams({
  name: `Podible POC ${runId}`,
  type: "movie",
  agent: "com.plexapp.agents.none",
  scanner: "Plex Video Files Scanner",
  language: "xn",
  location: viewRoot,
});
await plex(`/library/sections?${params}`, "POST");
const sections = ((await (await plex("/library/sections")).json()) as any)
  .MediaContainer.Directory;
const sectionId = String(
  sections.find((entry: any) => entry.title === `Podible POC ${runId}`).key,
);
const db = new Database(`${root}/app.sqlite`);
runMigrations(db);
const repo = new BooksRepo(db);
const settings = repo.ensureSettings();
settings.movies = {
  ...settings.movies,
  managedRoot: root,
  downloadGroupId: 1000,
  plexViewPath: viewRoot,
  automaticUpgrades: true,
};
settings.rtorrent = { transport: "http-xmlrpc", url: "http://rtorrent:8000" };
settings.torznab = [
  {
    name: "Prowlarr POC",
    baseUrl: `http://prowlarr:9696/${indexer.id}`,
    apiKey: key,
  },
];
repo.updateSettings(settings);
const indexes = new MovieIndexers(settings.torznab);
const probeHashes = new Set([
  createHash("sha256").update(lowerBytes).digest("hex"),
  createHash("sha256").update(higherBytes).digest("hex"),
]);
let plexPaths: string[] = [];
const plexLibrary = new PlexMovieLibrary({
  url: "http://127.0.0.1:32400",
  token: "",
  sectionId,
});
const services: MovieServices = {
  catalog: {
    search: async () => [movie],
    get: async (id) => {
      if (id !== movie.tmdbId) throw new Error("Unknown synthetic movie");
      return movie;
    },
  },
  search: (target, query) =>
    indexes.search(
      target,
      query ?? (upgrade ? `${target.title} ${target.year} Upgrade` : undefined),
    ),
  decider: {
    select: async ({ offers, policy, current }) => {
      const ranked = offers
        .map((offer) => {
          const index = seeds.findIndex(
            (seed) =>
              inspectMetainfo(seed.metainfo).infoHash === offer.infoHash,
          );
          const width = index === 1 ? 1280 : 1920;
          return {
            offer,
            facts: {
              identityVerified: index >= 0,
              source: "web" as const,
              cut: "theatrical",
              width,
              height: width === 1280 ? 720 : 1080,
              videoCodec: "h264",
              dynamicRange: "sdr" as const,
              audioLanguages: ["eng"],
              proper: false,
            },
          };
        })
        .filter(
          (r) =>
            evaluateMovieQuality(policy, r.facts).eligibility === "eligible" &&
            (!current || assessMovieUpgrade(policy, current, r.facts).upgrade),
        );
      ranked.sort(
        (a, b) =>
          evaluateMovieQuality(policy, b.facts).tier! -
          evaluateMovieQuality(policy, a.facts).tier!,
      );
      return {
        offerKey: ranked[0]?.offer.key ?? null,
        facts: ranked[0]?.facts ?? {
          identityVerified: false,
          cut: null,
          source: null,
          width: null,
          height: null,
          videoCodec: null,
          dynamicRange: null,
          audioLanguages: [],
          proper: null,
        },
        reason:
          "Offline fixture identity pinned to generated media; production uses the movie agents",
      };
    },
    inspect: async ({ files }) => ({
      fileIndex: files[0]?.index ?? null,
      facts: {
        identityVerified: true,
        source: "web",
        cut: "theatrical",
        proper: false,
      },
      reason: "Synthetic fixture import verified with actual ffprobe streams",
    }),
  },
  client: new RtorrentAdapter({
    clientId: "lab-movie-client",
    settings: settings.rtorrent,
    ownershipTag: "Podible Movie POC",
    roots: { movies: `${root}/downloads` },
  }),
  library: {
    scan: async (paths) => {
      await plexLibrary.scan();
    },
    verify: async (_movie, expected) => {
      // Offline fixture identity is the actual generated payload hash, not a fake TMDB match.
      const listing =
        (
          (await (
            await plex(`/library/sections/${sectionId}/all`)
          ).json()) as any
        ).MediaContainer.Metadata ?? [];
      const parts = listing
        .flatMap((item: any) => item.Media ?? [])
        .flatMap((media: any) => media.Part ?? []);
      for (const file of expected) {
        const part = parts.find((entry: any) => entry.file === file);
        if (!part) return false;
        if (
          !probeHashes.has(
            createHash("sha256")
              .update(Buffer.from(await Bun.file(file).arrayBuffer()))
              .digest("hex"),
          )
        )
          throw new Error("Plex part does not contain the synthetic movie");
        const read = await fetch(`http://127.0.0.1:32400${part.key}`, {
          headers: { Range: "bytes=0-31" },
        });
        if (read.status !== 206 || (await read.arrayBuffer()).byteLength !== 32)
          return false;
      }
      plexPaths = expected;
      return expected.length > 0;
    },
  },
  fetchMetainfo: fetchMovieMetainfo,
  probe: probeMovie,
  timing: { pollMs: 250, stallMs: 2000, plexRetryMs: 1000, plexAttempts: 300 },
};
const pendingModels = new Map<
  number,
  (value: { response?: ModelResponse; error?: string }) => void
>();
let modelSequence = 0;
if (process.env.POC_LIVE_AGENT === "1") {
  const relay: ModelProvider = {
    getModel(model) {
      return {
        getResponse: async (request) => {
          const id = ++modelSequence;
          const pending = new Promise<{
            response?: ModelResponse;
            error?: string;
          }>((resolve) => pendingModels.set(id, resolve));
          send({ kind: "model", id, model, request });
          const value = await pending;
          if (value.error || !value.response)
            throw new Error(value.error ?? "Empty lab model response");
          return {
            ...value.response,
            usage: Object.assign(new Usage(), value.response.usage),
          };
        },
        getStreamedResponse: async function* () {
          throw new Error("Lab relay is non-streaming");
        },
      };
    },
  };
  services.decider = createMovieDecider(
    { ...settings, agents: { ...settings.agents, timeoutMs: 60_000 } },
    relay,
  );
}
const user = repo.upsertUser({
  provider: "local",
  providerUserId: "lab",
  username: "lab-reader",
  isAdmin: false,
});
repo.createSession(
  user.id,
  hashSessionToken("movie-stack-fixture"),
  null,
  "browser",
);
const runtime = await createJobExecutionRuntime(
  {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: services,
    onLog() {},
  },
  `${root}/execution.sqlite`,
);
let pumping = false;
const interval = setInterval(async () => {
  if (pumping) return;
  pumping = true;
  try {
    await runtime.reconcile();
  } finally {
    pumping = false;
  }
}, 50);
const handler = createPodibleFetchHandler(repo, Date.now(), null, {
  movieServices: services,
});
const app = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  idleTimeout: 60,
  async fetch(request) {
    const pathname = new URL(request.url).pathname;
    if (request.method === "POST" && pathname === "/__fixture/upgrade") {
      upgrade = true;
      return Response.json({ ok: true });
    }
    if (pathname === "/__fixture/report")
      return Response.json({
        runId,
        upstreamSearches,
        plexPaths,
        sectionId,
        torrents: repo.movies
          .targets()
          .flatMap((t) => repo.movies.downloads(t.id)).length,
        views: repo.movies.targets().map((t) => ({
          activeVersionId: t.active_version_id,
          versions: repo.movies.acquisitions(t.id).length,
        })),
        peerRequests: seeds.map((seed) => seed.stats().requests),
        identity: "synthetic fixture SHA-256; no online TMDB matching",
        liveMovieAgent: process.env.POC_LIVE_AGENT === "1",
      });
    return handler(request);
  },
});
send({ ready: true });
const lines = createInterface({ input: process.stdin });
lines.on("line", (line) => {
  void (async () => {
    const parsed = JSON.parse(line);
    if (parsed.kind === "model-result") {
      pendingModels.get(parsed.id)?.(parsed);
      pendingModels.delete(parsed.id);
      return;
    }
    const input = JSON.parse(line) as {
      id: number;
      path: string;
      method: string;
      headers: Record<string, string>;
      body: string;
    };
    try {
      const headers = new Headers(input.headers);
      headers.delete("host");
      if (headers.has("origin")) headers.set("origin", app.url.origin);
      const response = await fetch(new URL(input.path, app.url), {
        method: input.method,
        headers,
        ...(input.body ? { body: Buffer.from(input.body, "base64") } : {}),
      });
      send({
        id: input.id,
        status: response.status,
        headers: Object.fromEntries(response.headers),
        body: Buffer.from(await response.arrayBuffer()).toString("base64"),
      });
    } catch (error) {
      send({ id: input.id, error: (error as Error).message });
    }
  })().catch((error) => console.error(error));
});
process.on("SIGTERM", async () => {
  clearInterval(interval);
  app.stop(true);
  tracker.stop(true);
  await runtime.stop();
  for (const target of repo.movies.targets())
    for (const download of repo.movies.downloads(target.id)) {
      try {
        await services.client.quiesce({
          clientId: download.client_id,
          nativeId: download.native_id,
        });
      } catch {}
    }
  for (const seed of seeds) await seed.close();
  await prowlarr(`/indexer/${indexer.id}`, "DELETE").catch(() => {});
  db.close();
  process.exit(0);
});
