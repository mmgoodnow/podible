import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { initializeManagedContent } from "../../src/media/library-view";
import { publishMovieVersion } from "../../src/movies/publication";
import { defaultMovieQualityPolicy as policy } from "../../src/movies/quality";
import { BooksRepo } from "../../src/repo";
import { deliverNextNotification } from "../../src/notifications/delivery";
import type { NotificationFetch } from "../../src/notify";
import { defaultSettings } from "../../src/settings";
import { PlexTvLibrary } from "../../src/tv/plex";
import { publishTvVersion, tvRoots, tvViewEntries } from "../../src/tv/publication";
import { requestTvSelection } from "../../src/tv/service";
import type { TvCatalogEntry } from "../../src/tv/types";
import { fixtureTvCatalog } from "../fixtures/tv-catalog";

async function fixture() {
  const root = await realpath(await mkdtemp(path.join(os.tmpdir(), "podible-notifications-")));
  const database = path.join(root, "app.sqlite");
  const db = new Database(database);
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = (name: string, isAdmin = false) => repo.upsertUser({ provider: "local", providerUserId: name, username: name, isAdmin });
  const roots = { contentRoot: path.join(root, "objects"), viewRoot: path.join(root, "movies"), serverViewRoot: "/plex/movies" };
  await initializeManagedContent(roots.contentRoot);
  const file = async () => {
    const id = randomUUID(), relativePath = `files/${id.slice(0, 2)}/${id}.mkv`;
    const destination = path.join(roots.contentRoot, relativePath);
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, "media", { mode: 0o444 });
    return { id, relativePath, sizeBytes: 5, probe: {} };
  };
  return { db, database, repo, user, root, roots, file, close: async () => { db.close(); await rm(root, { recursive: true, force: true }); } };
}

const movie = { tmdbId: 406, title: "Private Movie", originalTitle: "Private Movie", year: 2024, overview: "", originalLanguage: "en", imdbId: null, posterUrl: null };
const verified = { scan: async () => {}, verify: async () => true };
const addressFor = (user: { id: number }) => `user-${user.id}@example.test`;

function configureDelivery(repo: BooksRepo, users: { id: number }[]) {
  repo.updateSettings(defaultSettings({ notifications: {
    delivery: "live",
    email: { host: "smtp.example.test", from: "podible@example.test", recipients: users.map(user => `${user.id}=${addressFor(user)}`).join("\n") },
    pushover: { enabled: true, lifecycleEnabled: true, apiToken: "fake", userKey: "owner-key" },
  } }));
}

function pendingDeliveries(db: Database) {
  return db.query("SELECT key FROM app_state WHERE key GLOB 'notifications:delivery:v1:pending:*'").all();
}

async function drainDeliveries(repo: BooksRepo, count: number) {
  const emails: { to: string[]; subject: string; text: string }[] = [], pushes: URLSearchParams[] = [];
  const fetcher: NotificationFetch = async (url, request) => {
    expect(url).toBe("https://api.pushover.net/1/messages.json");
    pushes.push(new URLSearchParams(String(request?.body)));
    return Response.json({ status: 1 });
  };
  const tick = () => deliverNextNotification(repo.notifications.delivery, { fetcher, async smtp(target, message) {
    emails.push({ to: [target.to], subject: message.subject, text: message.text });
    return true;
  } });
  for (let i = 0; i < count; i++) expect(await tick()).toEqual({ state: "accepted" });
  expect(await tick()).toBeNull();
  return { emails, pushes };
}

test("notification destinations are admin-only and survive a legacy settings update", async () => {
  const f = await fixture();
  try {
    const admin = f.user("admin", true), user = f.user("user");
    configureDelivery(f.repo, [user]);
    for (const account of [admin, user]) f.repo.createSession(account.id, hashSessionToken(`notify-${account.username}`), null, "browser");
    const handler = createPodibleFetchHandler(f.repo, Date.now(), null);
    const rpc = async (method: string, params: object, username: string) => {
      const response = await handler(new Request("http://localhost/rpc", { method: "POST",
        headers: { "Content-Type": "application/json", Cookie: `podible_session=notify-${username}` },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }));
      return await response.json() as { result?: ReturnType<typeof defaultSettings>; error?: { code: number } };
    };
    expect((await rpc("settings.get", {}, "user")).result).toBeUndefined();
    const { result: saved } = await rpc("settings.get", {}, "admin");
    if (!saved) throw new Error("admin settings unavailable");
    expect(saved.notifications.email.recipients).toBe(`${user.id}=${addressFor(user)}`);
    const legacy = { ...saved, notifications: { pushover: { enabled: true, apiToken: "fake", userKey: "owner-key" } } };
    const { result: updated } = await rpc("settings.update", { settings: legacy }, "admin");
    expect(updated?.notifications).toEqual(saved.notifications);
    const invalid = { ...saved, notifications: { ...saved.notifications,
      email: { ...saved.notifications.email, recipients: "1=a@example.test\n1=b@example.test" } } };
    expect((await rpc("settings.update", { settings: invalid }, "admin")).error?.code).toBe(-32602);
    expect(f.repo.getSettings().notifications).toEqual(saved.notifications);
  } finally { await f.close(); }
});

test("movie publication atomically delivers once, including a watch, without exposing other users' inboxes", async () => {
  const f = await fixture();
  const { db, repo } = f;
  try {
    const owner = f.user("owner"), watcher = f.user("watcher"), stranger = f.user("stranger", true);
    const withdrawn = f.user("withdrawn"), imported = f.user("imported");
    configureDelivery(repo, [owner, watcher, stranger, withdrawn, imported]);
    for (const user of [owner, watcher, stranger]) repo.createSession(user.id, hashSessionToken(`notify-${user.username}`), null, "browser");
    const handler = createPodibleFetchHandler(repo, Date.now(), null);
    const rpc = async (method: string, params: object, username?: string) => {
      const response = await handler(new Request("http://localhost/rpc", { method: "POST",
        headers: { "Content-Type": "application/json", ...(username ? { Cookie: `podible_session=notify-${username}` } : {}) },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: `notifications.${method}`, params }) }));
      expect(response.headers.get("Cache-Control")).toBe("no-store");
      return await response.json() as { result?: unknown; error?: { code: number } };
    };
    const title = { kind: "movie", tmdbId: movie.tmdbId } as const;
    expect((await rpc("list", {})).error?.code).toBe(-32001);
    expect((await rpc("setWatch", { ...title, enabled: true }, "watcher")).result).toEqual({ status: "unavailable" });
    const request = repo.movies.request(owner.id, movie, policy);
    const dropped = repo.movies.request(withdrawn.id, movie, policy);
    repo.movies.withdrawRequest(dropped.requestId);
    const legacy = repo.movies.request(imported.id, movie, policy);
    repo.movieSeerr.start("fixture", 0, "0");
    db.query("INSERT INTO movie_request_origins VALUES ('fixture',1,?,'{}','fixture')").run(legacy.requestId);
    expect((await rpc("watchStatus", title, "watcher")).result).toEqual({ status: "eligible" });
    expect((await rpc("setWatch", { ...title, enabled: true }, "watcher")).result).toEqual({ status: "watching" });
    expect((await rpc("setWatch", { ...title, enabled: true, userId: stranger.id }, "watcher")).error?.code).toBe(-32602);
    expect(repo.movies.requests(watcher.id)).toEqual([]);

    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "reconcile" }).id).acquisition;
    const versionId = repo.movies.recordVersion(acquisition.id, { identityVerified: true, cut: "theatrical", source: "web",
      width: 1920, height: 1080, videoCodec: "h264", dynamicRange: "sdr", audioLanguages: ["en"], proper: false }, [await f.file()]);
    const input = { repo: repo.movies, roots: f.roots, library: verified, targetId: request.targetId, versionId };
    expect(await publishMovieVersion({ ...input, pendingIsExpected: true, library: { ...verified, verify: async () => false } })).toBe(false);
    expect(repo.notifications.list(owner.id).items).toEqual([]);
    expect(repo.notifications.list(watcher.id).items).toEqual([]);
    expect(pendingDeliveries(db)).toEqual([]);
    db.exec(`CREATE TRIGGER fail_delivery_write BEFORE INSERT ON app_state
      WHEN NEW.key GLOB 'notifications:delivery:v1:pending:*' BEGIN SELECT RAISE(ABORT, 'outbox write failed'); END`);
    await expect(publishMovieVersion(input)).rejects.toThrow("outbox write failed");
    expect(repo.movies.target(request.targetId)?.active_version_id).toBeNull();
    expect(repo.notifications.watchStatus(watcher.id, title)).toBe("watching");
    expect(repo.notifications.list(owner.id).items).toEqual([]);
    expect(repo.notifications.list(watcher.id).items).toEqual([]);
    expect(pendingDeliveries(db)).toEqual([]);
    db.exec("DROP TRIGGER fail_delivery_write");
    await expect(publishMovieVersion({ ...input, afterPromotion: () => { throw new Error("interrupted"); } })).rejects.toThrow("interrupted");

    const ownerNotice = repo.notifications.list(owner.id).items[0]!;
    const watcherNotice = repo.notifications.list(watcher.id).items[0]!;
    expect(ownerNotice).toMatchObject({ title: movie.title, href: `/movies/${movie.tmdbId}`, readAt: null });
    expect(watcherNotice.id).not.toBe(ownerNotice.id);
    expect(repo.notifications.watchStatus(watcher.id, title)).toBe("unavailable");
    expect(repo.notifications.list(withdrawn.id).items).toEqual([]);
    expect(repo.notifications.list(imported.id).items).toEqual([]);
    expect((await rpc("list", { before: ownerNotice.id }, "stranger")).result).toEqual({ items: [], next: null });
    expect((await rpc("markRead", { id: ownerNotice.id }, "stranger")).result).toEqual({ ok: false });
    expect((await rpc("markRead", { id: ownerNotice.id }, "owner")).result).toEqual({ ok: true });

    await publishMovieVersion(input);
    const reopened = new Database(f.database);
    try {
      expect(new BooksRepo(reopened).notifications.list(owner.id).items).toEqual([
        { ...ownerNotice, readAt: expect.any(String) },
      ]);
      expect(new BooksRepo(reopened).notifications.list(watcher.id).items).toEqual([watcherNotice]);
      const { emails, pushes } = await drainDeliveries(new BooksRepo(reopened), 3);
      expect(emails.map(email => email.to[0]).sort()).toEqual([addressFor(owner), addressFor(watcher)].sort());
      for (const email of emails) expect(email.subject).toContain(movie.title);
      expect(pushes).toHaveLength(1);
      expect(pushes[0]?.get("user")).toBe("owner-key");
      expect(pushes[0]?.get("message")).toBe(movie.title);
    } finally { reopened.close(); }
    await publishMovieVersion(input);
    expect(pendingDeliveries(db)).toEqual([]);

    // An explicit new watch after losing availability is not a replay of the first delivery.
    db.query("UPDATE movie_targets SET active_version_id=NULL,revision=revision+1 WHERE id=?").run(request.targetId);
    expect(repo.notifications.setWatch(watcher.id, title, true)).toBe("watching");
    await publishMovieVersion(input);
    const history = repo.notifications.list(watcher.id).items;
    expect(history).toHaveLength(2);
    expect(new Set(history.map(item => item.id)).size).toBe(2);
  } finally { await f.close(); }
});

const show: TvCatalogEntry = {
  key: "tvdb:520", tvdbId: 520, tmdbId: 500, title: "Private Series", aliases: [], year: 2024, anime: false, relations: [],
  episodes: [0, 1, 2].map(season => ({ key: `tvdb:${52001 + season}`, title: `Episode ${season}`, airDate: "2024-01-01",
    runtimeMinutes: 25, numbering: [{ scheme: "tvdb:aired", season, episode: 1, absolute: null }] })),
};

test("an individual native special notifies only its owner after exact Plex verification", async () => {
  const f = await fixture();
  try {
    const { repo } = f;
    const owner = f.user("special-owner"), other = f.user("different-special-owner"), admin = f.user("unrelated-admin", true);
    const fixture = fixtureTvCatalog(), entry = fixture.entry();
    const first = entry.episodes[0]!;
    entry.episodes.push({ ...first, key: "tvdb:7010", title: "Another Special",
      refs: [{ provider: "tvdb", id: "7010", source: "verified-catalog" }],
      numbering: [{ ...first.numbering[0]!, episode: 2 }] });
    fixture.setEntry(entry);
    const one = await requestTvSelection(repo, fixture.catalog, owner.id, { tmdbId: 70, specialEpisodeKeys: ["tvdb:7000"] });
    const two = await requestTvSelection(repo, fixture.catalog, other.id, { tmdbId: 70, specialEpisodeKeys: ["tvdb:7010"] });
    expect(repo.tv.requestById(one.requestId)?.request_key).toBe('native-seasons:70:[]:["tvdb:7000"]');
    expect(repo.tv.requestedEpisodes(one.requestId).map(ep => ep.key)).toEqual(["tvdb:7000"]);
    expect(two.targetId).toBe(one.targetId);

    const acquisition = repo.tv.acquisitions(one.targetId).find(a => a.request_id === one.requestId)!;
    const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ ...await f.file(), episodeIds: one.episodeIds }]);
    const roots = tvRoots(f.root, "/plex/tv");
    let plexEpisode = "7010", wrongPath = false;
    const library = new PlexTvLibrary({ url: "https://plex.invalid", token: "fixture", sectionId: "3" }, (async input => {
      const route = new URL(String(input)).pathname;
      if (route === "/library/sections/3/all")
        return Response.json({ MediaContainer: { Metadata: [{ ratingKey: "show", Guid: [{ id: "tvdb://700" }] }] } });
      expect(route).toBe("/library/metadata/show/allLeaves");
      const file = path.posix.join(roots.serverViewRoot, tvViewEntries(repo.tv, roots)[0]!.relativePath);
      return Response.json({ MediaContainer: { Metadata: [{ ratingKey: "special", parentIndex: 0, index: 1,
        Guid: [{ id: `tvdb://${plexEpisode}` }], Media: [{ Part: [{ file: wrongPath ? `${file}.wrong` : file }] }] }] } });
    }) as typeof fetch);
    const publish = () => publishTvVersion({ repo: repo.tv, roots, targetId: one.targetId, versionId, library, scan: false, pendingIsExpected: true });

    expect(await publish()).toBe(false); // Right aired coordinate and path, wrong episode GUID.
    expect(repo.notifications.list(owner.id).items).toEqual([]);
    plexEpisode = "7000";
    wrongPath = true;
    expect(await publish()).toBe(false);
    expect(repo.notifications.list(owner.id).items).toEqual([]);
    wrongPath = false;
    expect(await publish()).toBe(true);
    expect(repo.notifications.list(owner.id).items).toEqual([expect.objectContaining({
      title: entry.title, episode: "S00E01: Special", href: `/tv/library/${one.entryId}`, readAt: null,
    })]);
    expect(repo.notifications.list(other.id).items).toEqual([]);
    expect(repo.notifications.list(admin.id).items).toEqual([]);
  } finally { await f.close(); }
});

test("TV notices match exact episode membership, keep remaining watches, and never enqueue work", async () => {
  const f = await fixture();
  try {
    const { repo } = f;
    const specialOwner = f.user("special"), regularOwner = f.user("regular"), laterOwner = f.user("later");
    const watcher = f.user("watcher"), cancelled = f.user("cancelled");
    const [special, regular, later] = show.episodes.map(ep => ep.key) as [string, string, string];
    const one = repo.tv.requestNativeSelection(specialOwner.id, show, policy, [], [special], [special]);
    const two = repo.tv.requestNativeSelection(regularOwner.id, show, policy, [1], [], [regular]);
    const title = { kind: "tv", tmdbId: show.tmdbId! } as const;
    expect(repo.notifications.setWatch(watcher.id, title, true)).toBe("watching");
    expect(repo.notifications.setWatch(watcher.id, title, true)).toBe("watching");
    expect(repo.notifications.setWatch(cancelled.id, title, true)).toBe("watching");
    expect(repo.notifications.setWatch(cancelled.id, title, false)).toBe("eligible");
    expect(repo.tv.requests(watcher.id)).toEqual([]);
    expect(f.db.query("SELECT count(*) AS n FROM jobs").get()).toEqual({ n: 0 });
    const three = repo.tv.requestNativeSelection(laterOwner.id, show, policy, [2], [], [later]);
    const roots = tvRoots(f.root, "/plex/tv");
    const prepare = async (request: typeof one) => {
      const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "reconcile" }).id,
        request.requestId, request.episodeIds).acquisition;
      const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ ...await f.file(), episodeIds: request.episodeIds }]);
      repo.tv.finishAcquisition(acquisition.id);
      return { repo: repo.tv, roots, targetId: request.targetId, versionId, library: verified };
    };
    await publishTvVersion(await prepare(three));
    expect(repo.notifications.list(laterOwner.id).items[0]?.episode).toBe("S02E01: Episode 2");
    expect(repo.notifications.list(watcher.id).items).toEqual([]);
    const specialVersion = await prepare(one);
    expect(await publishTvVersion({ ...specialVersion, pendingIsExpected: true, library: { ...verified, verify: async () => false } })).toBe(false);
    expect(repo.notifications.list(specialOwner.id).items).toEqual([]);
    await publishTvVersion(specialVersion);
    expect(repo.notifications.list(specialOwner.id).items[0]?.episode).toBe("S00E01: Episode 0");
    expect(repo.notifications.list(regularOwner.id).items).toEqual([]);
    expect(repo.notifications.watchStatus(watcher.id, title)).toBe("watching");
    await publishTvVersion(await prepare(two));
    expect(repo.notifications.list(regularOwner.id).items[0]?.episode).toBe("S01E01: Episode 1");
    expect(repo.notifications.list(watcher.id).items.map(n => n.episode).sort()).toEqual(["S00E01: Episode 0", "S01E01: Episode 1"]);
    expect(repo.notifications.list(cancelled.id).items).toEqual([]);
    expect(repo.notifications.watchStatus(watcher.id, title)).toBe("unavailable");
  } finally { await f.close(); }
});

test("a TV publication batches notice and watch statements across recipients and episodes", async () => {
  const f = await fixture();
  try {
    const { repo } = f;
    const owner = f.user("owner"), otherOwner = f.user("other-owner"), later = f.user("later-owner");
    const watchers = [f.user("watcher-a"), f.user("watcher-b")];
    configureDelivery(repo, [owner, otherOwner, later, ...watchers]);
    const [special, regular, future] = show.episodes.map(ep => ep.key) as [string, string, string];
    const request = repo.tv.requestNativeSelection(owner.id, show, policy, [1], [special], [special, regular]);
    repo.tv.requestNativeSelection(otherOwner.id, show, policy, [1], [], [regular]);
    const title = { kind: "tv", tmdbId: show.tmdbId! } as const;
    expect(repo.notifications.setWatch(owner.id, title, true)).toBe("watching");
    repo.tv.requestNativeSelection(later.id, show, policy, [2], [], [future]);
    for (const watcher of watchers) expect(repo.notifications.setWatch(watcher.id, title, true)).toBe("watching");

    const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "reconcile" }).id,
      request.requestId, request.episodeIds).acquisition;
    const files = await Promise.all(request.episodeIds.map(async id => ({ ...await f.file(), episodeIds: [id] })));
    const versionId = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, files);
    const query = spyOn(f.db, "query");
    try {
      await publishTvVersion({ repo: repo.tv, roots: tvRoots(f.root, "/plex/tv"), targetId: request.targetId, versionId, library: verified });
      const writes = query.mock.calls.map(([sql]) => sql).filter(sql => /^(INSERT INTO|UPDATE|DELETE FROM) app_state\b/.test(sql));
      expect(writes.map(sql => sql.split(" app_state")[0])).toEqual(["INSERT INTO", "INSERT INTO", "INSERT INTO", "DELETE FROM", "UPDATE"]);
    } finally { query.mockRestore(); }

    const notices = repo.notifications.list(owner.id).items;
    const revision = repo.tv.target(request.targetId)!.revision;
    expect(notices.map(n => n.id).sort()).toEqual(request.episodeIds.map(id =>
      createHash("sha256").update(`${owner.id}:tv:${request.targetId}:${revision}:${id}`).digest("hex")).sort());
    expect(repo.notifications.watchStatus(owner.id, title)).toBe("eligible");
    expect(repo.notifications.list(otherOwner.id).items.map(n => n.episode)).toEqual(["S01E01: Episode 1"]);
    expect(repo.notifications.list(later.id).items).toEqual([]);
    for (const watcher of watchers) {
      expect(repo.notifications.list(watcher.id).items.map(n => n.episode).sort()).toEqual(["S00E01: Episode 0", "S01E01: Episode 1"]);
      expect(repo.notifications.watchStatus(watcher.id, title)).toBe("watching");
    }

    expect(repo.notifications.markRead(owner.id, notices[0]!.id)).toBe(true);
    const read = repo.notifications.list(owner.id).items;
    const { emails, pushes } = await drainDeliveries(repo, 8);
    const specialEmails = emails.filter(email => email.text.includes("S00E01"));
    const regularEmails = emails.filter(email => email.text.includes("S01E01"));
    expect(specialEmails.flatMap(email => email.to).sort()).toEqual([owner, ...watchers].map(addressFor).sort());
    expect(regularEmails.flatMap(email => email.to).sort()).toEqual([owner, otherOwner, ...watchers].map(addressFor).sort());
    expect(pushes.map(push => push.get("message"))).toEqual([`${show.title}: S00E01: Episode 0, S01E01: Episode 1`]);
    repo.transaction(() => repo.notifications.tvPublished(request.targetId, revision, request.episodeIds));
    expect(repo.notifications.list(owner.id).items).toEqual(read);
    expect(pendingDeliveries(f.db)).toEqual([]);
  } finally { await f.close(); }
});
