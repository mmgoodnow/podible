import { expect, test } from "bun:test";
import { Hono } from "hono";
import { hashSessionToken } from "../../src/auth";
import { openDatabase } from "../../src/db";
import { createRequestContextMiddleware, type HttpEnv } from "../../src/http/middleware";
import { createTrailerRoutes } from "../../src/http/trailer-routes";
import { BooksRepo } from "../../src/repo";

const row = { kind: "movie", tmdbId: "10378", parent: "1", extra: "2" };
const machineIdentifier = "a".repeat(40);
const allow = (...titles: typeof row[]) => JSON.stringify({ machineIdentifier, titles });
const key = "/library/parts/7/0/file.mp4";
const file = "/library/Trailers/Local fixture.mp4";
const guid = "plex://movie/exact";
const bytes = Buffer.from("\0\0\0\x18ftypisom");
const parent = { ratingKey: "1", librarySectionID: 3, type: "movie", guid, Guid: [{ id: "tmdb://10378" }] };
const clip = {
  ratingKey: "2", type: "clip", subtype: "trailer", primaryGuid: guid, guid: "file:///library/Trailers/Local%20fixture.mp4",
  duration: 12000, Media: [{ container: "mp4", videoCodec: "h264", audioCodec: "aac", width: 640, height: 360,
    Part: [{ id: 7, key, file, container: "mp4", size: bytes.length }] }],
};
const container = (...Metadata: unknown[]) => Response.json({ MediaContainer: { size: Metadata.length, Metadata } });

function fixture(options: {
  manifest?: string; machine?: string; parent?: unknown; extras?: unknown[];
  media?: (probe: boolean) => Response;
} = {}) {
  const calls: { path: string; range: string | null }[] = [];
  const fetcher = (async (input, init) => {
    const url = new URL(String(input));
    const headers = new Headers(init?.headers);
    expect(url.origin).toBe("http://plex.test");
    expect(headers.get("X-Plex-Token")).toBe("fixture-plex-token");
    expect(init?.redirect).toBe("error");
    const range = headers.get("Range");
    calls.push({ path: url.pathname, range });
    if (url.pathname === "/identity") return Response.json({ MediaContainer: { machineIdentifier: options.machine ?? machineIdentifier } });
    if (url.pathname === "/library/metadata/1") return container(options.parent ?? parent);
    if (url.pathname === "/library/metadata/1/extras") return container(...(options.extras ?? [clip]));
    if (url.pathname === key) {
      expect(url.search).toBe("?download=1");
      const probe = range === "bytes=0-7";
      return options.media?.(probe) ?? new Response(probe ? bytes.subarray(0, 8) : bytes, {
        status: probe ? 206 : 200, headers: { "Content-Type": "video/mp4",
          "Content-Length": String(probe ? 8 : bytes.length),
          ...(probe ? { "Content-Range": `bytes 0-7/${bytes.length}` } : {}) },
      });
    }
    throw new Error("Unexpected Plex path");
  }) as typeof fetch;
  const db = openDatabase(":memory:");
  const repo = new BooksRepo(db);
  const settings = repo.getSettings();
  repo.updateSettings({ ...settings, movies: {
    ...settings.movies, plexUrl: "http://plex.test", plexToken: "fixture-plex-token", plexSectionId: "3",
  } });
  const user = repo.upsertUser({ provider: "local", providerUserId: "viewer", username: "viewer", isAdmin: false });
  const session = repo.createSession(user.id, hashSessionToken("fixture-viewer-token"), null, "app");
  const app = new Hono<HttpEnv>();
  app.use("*", createRequestContextMiddleware(repo));
  app.route("/api/trailers", createTrailerRoutes(repo, options.manifest ?? allow(row), fetcher));
  return { calls, close: () => db.close(), revoke: () => db.query("DELETE FROM sessions WHERE id = ?").run(session.id),
    get: (suffix = "", authenticated = true, method = "GET") => app.request(
      `https://podible.test/api/trailers/movie/10378${suffix}`,
      { method, headers: authenticated ? { Authorization: "Bearer fixture-viewer-token" } : {} },
    ) };
}

test("a non-admin can discover and download only explicitly shared, verified trailer bytes", async () => {
  const f = fixture();
  try {
    const availability = await f.get();
    expect(await availability.json()).toEqual({ available: true });
    expect(f.calls.at(-1)).toEqual({ path: key, range: "bytes=0-7" });
    const head = await f.get("/file", true, "HEAD");
    expect(head.status).toBe(200);
    expect(f.calls.at(-1)?.range).toBe("bytes=0-7");
    const download = await f.get("/file");
    expect(download.status).toBe(200);
    expect(download.headers.get("content-type")).toBe("video/mp4");
    expect(download.headers.get("x-podible-trailer-bytes")).toBe(String(bytes.length));
    expect(download.headers.get("cache-control")).toBe("private, no-store");
    expect(download.headers.get("location")).toBeNull();
    expect(Buffer.from(await download.arrayBuffer())).toEqual(bytes);
    expect(f.calls.at(-1)).toEqual({ path: key, range: null });
    f.revoke();
    expect((await f.get("/file")).status).toBe(401);
  } finally { f.close(); }
});

test("unapproved, ambiguous or unauthenticated requests never reach Plex", async () => {
  const f = fixture();
  const duplicate = fixture({ manifest: allow(row, { ...row, extra: "4" }) });
  const disabled = fixture({ manifest: "" });
  try {
    expect((await f.get("/file", false)).status).toBe(401);
    expect((await f.get("/anything")).status).toBe(404);
    expect((await duplicate.get()).status).toBe(404);
    expect((await disabled.get()).status).toBe(404);
    expect([...f.calls, ...duplicate.calls, ...disabled.calls]).toEqual([]);
  } finally { f.close(); duplicate.close(); disabled.close(); }
});

test("approvals cannot follow reused rating keys to a different Plex server", async () => {
  const f = fixture({ machine: "b".repeat(40) });
  try {
    expect((await f.get("/file")).status).toBe(404);
    expect(f.calls).toEqual([{ path: "/identity", range: null }]);
  } finally { f.close(); }
});

test("wrong titles, sections, non-local extras, other clips and untrusted part keys are rejected", async () => {
  const badPart = (override: object) => [{ ...clip, Media: [{ ...clip.Media[0],
    Part: [{ ...clip.Media[0]!.Part[0], ...override }] }] }];
  const cases = [
    { parent: { ...parent, Guid: [...parent.Guid, { id: "tmdb://5" }] } },
    { parent: { ...parent, librarySectionID: 4 } },
    { parent: { ...parent, type: "show" } },
    { extras: [] },
    { extras: [clip, clip] },
    { extras: [{ ...clip, primaryGuid: "plex://movie/different" }] },
    { extras: [{ ...clip, guid: "https://www.youtube.com/watch?v=abc", Media: [] }] },
    { extras: [{ ...clip, subtype: "interview" }] },
    { extras: [{ ...clip, Media: [{ ...clip.Media[0], videoCodec: "hevc" }] }] },
    { extras: badPart({ key: "//different.test/library/parts/7/0/file.mp4" }) },
    { extras: badPart({ size: 33 * 1024 * 1024 }) },
  ];
  for (const options of cases) {
    const f = fixture(options);
    try {
      expect((await f.get()).status).toBe(404);
      expect((await f.get("/file")).status).toBe(404);
      expect(f.calls.every(({ path }) => path === "/identity" || path.startsWith("/library/metadata/"))).toBe(true);
    } finally { f.close(); }
  }
}, 20_000);

test("a redirect, metadata page or bad range is not playable and is not forwarded", async () => {
  const responses = [
    () => new Response(null, { status: 302, headers: { Location: "https://other.test/secret" } }),
    () => new Response("<html>Oops</html>", { headers: { "Content-Type": "text/html" } }),
    () => new Response("badbytes", { status: 206, headers: { "Content-Type": "video/mp4",
      "Content-Length": "8", "Content-Range": `bytes 0-7/${bytes.length}` } }),
  ];
  for (const media of responses) {
    const f = fixture({ media });
    try {
      const response = await f.get();
      expect(response.status).toBe(502);
      expect(response.headers.get("location")).toBeNull();
      expect(await response.text()).toBe("");
      expect((await f.get("/file")).status).toBe(502);
    } finally { f.close(); }
  }
});
