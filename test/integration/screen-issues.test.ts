import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { hashSessionToken } from "../../src/auth";
import { openDatabase } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { MAX_SCREEN_ISSUE_DESCRIPTION, screenIssuePageSchema, screenIssueSchema } from "../../src/issues/contracts";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { BooksRepo } from "../../src/repo";
import type { TvCatalogEntry } from "../../src/tv/types";
import { fixtureMovie } from "../fixtures/movie-services";

const movie = { media: "movie", tmdbId: fixtureMovie.tmdbId } as const;
const report = (subject: Record<string, unknown> = movie, description = "Playback stops at 0:42.") =>
  ({ subject, description, category: "playback" });
const trpcData = async (response: Response) => ((await response.json()) as { result: { data: unknown } }).result.data;

async function setup() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-screen-issues-"));
  const file = path.join(root, "app.sqlite");
  const db = openDatabase(file);
  const repo = new BooksRepo(db);
  const user = (name: string, isAdmin = false) => {
    const row = repo.upsertUser({ provider: "local", providerUserId: name, username: name, isAdmin });
    repo.createSession(row.id, hashSessionToken(name), null, "browser");
    return row.id;
  };
  const one = user("one"), two = user("two"), admin = user("admin", true);
  const handle = createPodibleFetchHandler(repo, Date.now());
  const headers = (token: string | null): Record<string, string> => token ? { cookie: `podible_session=${token}` } : {};
  const trpc = (method: string, params: unknown = {}, token: string | null = "one", write = false, origin?: string) =>
    handle(new Request(`http://localhost/api/trpc/issues.${method}${write ? "" : `?input=${encodeURIComponent(JSON.stringify(params))}`}`, {
      ...(write ? { method: "POST", body: JSON.stringify(params) } : {}),
      headers: { ...headers(token), ...(write ? { "content-type": "application/json" } : {}), ...(origin ? { origin } : {}) },
    }));
  const rpc = async (method: string, params: unknown = {}, token: string | null = "one", origin?: string) => {
    const response = await handle(new Request("http://localhost/rpc", { method: "POST",
      headers: { ...headers(token), "content-type": "application/json", ...(origin ? { origin } : {}) },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: `issues.${method}`, params }),
    }));
    return response.json() as Promise<{ result?: unknown; error?: { code: number; message: string } }>;
  };
  return { db, file, repo, one, two, admin, trpc, rpc,
    close: async () => { db.close(); await rm(root, { recursive: true, force: true }); } };
}

function showCatalog(id: number): TvCatalogEntry {
  return { key: `tvdb:${id}`, tvdbId: id, tmdbId: id, title: `Show ${id}`, aliases: [], year: 2020,
    posterUrl: null, anime: false, relations: [], episodes: [
      { key: `tvdb:${id}01`, title: "First episode", airDate: null, runtimeMinutes: null,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null }] },
      { key: `tvdb:${id}02`, title: "A verified special", airDate: null, runtimeMinutes: null,
        numbering: [{ scheme: "tvdb:aired", season: 0, episode: 1, absolute: null }] },
      { key: `ephemeral:tvdb:${id}:1:2`, title: "Unverified slot", airDate: null, runtimeMinutes: null,
        numbering: [{ scheme: "tvdb:aired", season: 1, episode: 2, absolute: null }] },
    ] };
}

test("authenticated transport, ownership, bounded text and safe durable readback", async () => {
  const s = await setup();
  try {
    s.repo.movies.request(s.one, fixtureMovie, defaultMovieQualityPolicy);
    expect((await s.trpc("create", report(), null, true)).status).toBe(401);
    expect((await s.trpc("listMine", {}, null)).status).toBe(401);
    expect((await s.trpc("inbox", {}, "two")).status).toBe(403);
    expect((await s.trpc("create", report(), "one", true, "https://elsewhere.invalid")).status).toBe(403);
    expect((await s.trpc("create", report(), "one")).status).toBe(405);
    expect((await s.rpc("create", report(), null)).error?.code).toBe(-32001);
    expect((await s.rpc("create", report(), "one", "https://elsewhere.invalid")).error?.code).toBe(-32003);
    expect((await s.rpc("inbox", {}, "two")).error?.code).toBe(-32003);
    const unknown = await s.rpc("create", report({ ...movie, tmdbId: 999999 }));
    const privateTitle = await s.rpc("create", report(), "two");
    expect(privateTitle.error).toEqual(unknown.error);
    expect(unknown.error?.code).toBe(-32000);

    for (const params of [report(movie, "  \n "), report(movie, "a".repeat(MAX_SCREEN_ISSUE_DESCRIPTION + 1)),
      report(movie, "bad\u0000text"), { ...report(), userId: s.two }, { ...report(), status: "resolved" },
      report({ ...movie, episodeId: 1 }), report({ ...movie, title: "Untrusted override" })]) {
      expect((await s.rpc("create", params)).error?.code).toBe(-32602);
    }
    const description = "<script>not markup</script>\nPlayback stopped.";
    const response = await s.trpc("create", report(movie, ` ${description} `), "one", true);
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    const raw = await trpcData(response);
    const saved = screenIssueSchema.parse(raw);
    expect(raw).toEqual(saved);
    expect(saved).toMatchObject({ subject: movie, title: fixtureMovie.title, episodeTitle: null, description, status: "open",
      triage: { finding: "no-publication-recorded", checkedAt: saved.createdAt } });
    expect((await s.rpc("listMine", { title: movie }, "two")).result).toEqual({ reports: [], nextBeforeId: null });
    expect((await s.rpc("listMine", { title: movie, userId: s.one }, "two")).error?.code).toBe(-32602);
    expect((await s.rpc("listMine", {}, "admin")).result).toEqual({ reports: [], nextBeforeId: null });
    expect((await s.rpc("inbox", {}, "admin")).result).toEqual({ reports: [saved], nextBeforeId: null });

    using reopened = openDatabase(s.file);
    const reader = createPodibleFetchHandler(new BooksRepo(reopened), Date.now());
    const read = await reader(new Request("http://localhost/api/trpc/issues.listMine?input=%7B%7D", { headers: { cookie: "podible_session=one" } }));
    expect(read.status).toBe(200);
    expect(read.headers.get("cache-control")).toBe("no-store");
    expect(await trpcData(read)).toEqual({ reports: [saved], nextBeforeId: null });
  } finally { await s.close(); }
});

test("TV reports bind canonical membership, including exact specials, and never modify request or media state", async () => {
  const s = await setup();
  try {
    const entry = s.repo.tv.upsertEntry(showCatalog(70));
    const other = s.repo.tv.upsertEntry(showCatalog(80));
    const [first, special, ephemeral] = entry.episodes;
    s.repo.tv.requestEpisodes(s.one, entry.id, {}, [first!.id, special!.id]);
    const subject = { media: "tv", entryId: entry.id, episodeId: special!.id };
    const snapshot = () => ["tv_requests", "tv_targets", "tv_acquisitions", "tv_active_episodes", "tv_events", "jobs", "job_commands", "managed_content_files"]
      .map(table => s.db.query(`SELECT * FROM ${table}`).all());
    const before = snapshot();
    expect((await s.rpc("create", report({ ...subject, episodeId: other.episodes[0]!.id }))).error?.code).toBe(-32000);
    expect((await s.rpc("create", report({ ...subject, episodeId: ephemeral!.id }))).error?.code).toBe(-32000);
    expect((await s.rpc("create", report(subject), "two")).error?.code).toBe(-32000);
    const saved = screenIssueSchema.parse((await s.rpc("create", report(subject))).result);
    expect(saved).toMatchObject({ subject, title: entry.title, episodeTitle: "A verified special", status: "open" });
    const show = { media: "tv", entryId: entry.id };
    const second = screenIssueSchema.parse((await s.rpc("create", report(show, "a".repeat(MAX_SCREEN_ISSUE_DESCRIPTION)))).result);
    expect(second).toMatchObject({ subject: show, episodeTitle: null });
    expect(snapshot()).toEqual(before);
    expect((await s.rpc("listMine", { title: movie })).result).toEqual({ reports: [], nextBeforeId: null });
    expect((await s.rpc("listMine", { limit: 51 })).error?.code).toBe(-32602);
    const page = screenIssuePageSchema.parse((await s.rpc("listMine", { title: show, limit: 1 })).result);
    expect(page).toEqual({ reports: [second], nextBeforeId: second.id });
    expect((await s.rpc("listMine", { title: show, beforeId: page.nextBeforeId, limit: 1 })).result)
      .toEqual({ reports: [saved], nextBeforeId: null });
    expect(() => s.db.query(`UPDATE screen_issues SET tv_episode_id=? WHERE id=?`).run(other.episodes[0]!.id, saved.id)).toThrow();
  } finally { await s.close(); }
});

test("triage distinguishes exact published episodes and historical loss without exposing another user's request evidence", async () => {
  const s = await setup();
  try {
    const show = s.repo.tv.upsertEntry(showCatalog(70));
    const [active, lost, untouched] = show.episodes.map(episode => episode.id);
    const own = s.repo.tv.requestEpisodes(s.one, show.id, {}, [active!, lost!]);
    const sharedTarget = s.repo.tv.requestEpisodes(s.two, show.id, {}, [active!]);
    expect(sharedTarget.targetId).toBe(own.targetId);
    const now = "2026-01-01T00:00:00.000Z";
    const acquisition = s.db.query<{ id: number }, [number, string]>(`INSERT INTO tv_acquisitions(target_id,reason,created_at)
      VALUES(?,'initial',?) RETURNING id`).get(own.targetId, now)!;
    const version = s.db.query<{ id: number }, [number, number, number, string]>(`INSERT INTO tv_versions(entry_id,target_id,acquisition_id,facts_json,created_at)
      VALUES(?,?,?,'{}',?) RETURNING id`).get(show.id, own.targetId, acquisition.id, now)!;
    s.db.query(`INSERT INTO managed_content_files(id,relative_path,size_bytes,probe_json,created_at)
      VALUES('lost-file','private/path.mkv',5,'{}',?),('active-file','private/available.mkv',5,'{}',?)`).run(now, now);
    s.db.query(`INSERT INTO tv_version_files(version_id,entry_id,file_id,sequence) VALUES(?,?,'lost-file',0)`).run(version.id, show.id);
    s.db.query(`INSERT INTO tv_version_file_episodes(version_id,file_id,entry_id,episode_id) VALUES(?,'lost-file',?,?)`)
      .run(version.id, show.id, lost!);
    s.db.query(`INSERT INTO tv_version_files(version_id,entry_id,file_id,sequence) VALUES(?,?,'active-file',1)`).run(version.id, show.id);
    s.db.query(`INSERT INTO tv_version_file_episodes(version_id,file_id,entry_id,episode_id) VALUES(?,'active-file',?,?)`)
      .run(version.id, show.id, active!);
    s.db.query(`INSERT INTO media_content_losses(file_id,namespace,target_id,operation_id,observed_at)
      VALUES('lost-file','tv-target',?,'private-operation',?)`).run(own.targetId, now);
    s.db.query(`INSERT INTO tv_active_episodes(target_id,entry_id,episode_id,version_id) VALUES(?,?,?,?)`)
      .run(own.targetId, show.id, active!, version.id);

    const create = async (episodeId: number, token = "one") => screenIssueSchema.parse((await s.rpc("create",
      report({ media: "tv", entryId: show.id, episodeId }), token)).result);
    expect((await create(active!, "two")).triage.finding).toBe("publication-recorded");
    const saved = await create(lost!);
    expect(saved.triage.finding).toBe("content-loss-recorded");
    expect((await create(lost!, "two")).triage.finding).toBe("no-publication-recorded");
    const inbox = JSON.stringify((await s.rpc("inbox", {}, "admin")).result);
    for (const internal of ["private-operation", "private/path.mkv", "lost-file", "user_id", "targetId", "versionId"])
      expect(inbox).not.toContain(internal);
    expect((await s.rpc("create", report({ media: "tv", entryId: show.id, episodeId: untouched }))).error?.code).toBe(-32000);
    expect(s.db.query("SELECT COUNT(*) AS count FROM tv_active_episodes").get()).toEqual({ count: 1 });
    expect(s.db.query("SELECT COUNT(*) AS count FROM media_content_losses").get()).toEqual({ count: 1 });
  } finally { await s.close(); }
});
