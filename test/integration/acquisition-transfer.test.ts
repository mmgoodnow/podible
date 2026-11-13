import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { queueMovieAcquisition } from "../../src/movies/service";
import { BooksRepo } from "../../src/repo";
import { readTransfer, transferSamples } from "../../src/runtime/acquisition-transfer";
import { transferIntervalMs } from "../../src/runtime/acquisition-transfer-contracts";
import type { AdminTransmissionReader } from "../../src/rpc/admin-torrents";
import { fixtureMovie } from "../fixtures/movie-services";

test("movie transfer samples stay scoped to an owned active download, dedupe subscribers, and stop on revocation", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), root = await realpath(await mkdtemp(join(tmpdir(), "podible-transfer-")));
  const abort = new AbortController();
  try {
    const owner = repo.upsertUser({ provider: "local", providerUserId: "transfer-owner", username: "Owner" });
    const other = repo.upsertUser({ provider: "local", providerUserId: "transfer-other", username: "Other", isAdmin: true });
    repo.createSession(owner.id, hashSessionToken("transfer-owner"), null, "app");
    repo.createSession(other.id, hashSessionToken("transfer-other"), null, "app");
    const settings = repo.ensureSettings();
    settings.movies.managedRoot = root;
    repo.updateSettings(settings);
    const request = repo.movies.request(owner.id, fixtureMovie, defaultMovieQualityPolicy);
    const batch = queueMovieAcquisition(repo, request.targetId, "initial").acquisition;
    const clientId = "transmission:11111111-1111-4111-8111-111111111111", hash = "a".repeat(40);
    const directory = join(root, "downloads", "actual");
    await mkdir(directory, { recursive: true });
    repo.movies.bindDownload(batch.id, { clientId, nativeId: hash }, { rootId: "movies", relativePath: "actual" }, "selected");
    repo.movies.bindDownload(batch.id, { clientId, nativeId: "b".repeat(40) }, { rootId: "movies", relativePath: "old" }, "old");
    repo.movies.decision(batch.id, { selection: { offerKey: "selected" } });
    repo.movies.event(request.targetId, batch.id, "fixture:download", "downloading", "Fixture");
    const scope = { media: "movie" as const, id: request.requestId };
    let calls = 0, done = 0;
    const reader: AdminTransmissionReader = { clientId, rpc: { async call(method, args) {
      calls++;
      expect(method).toBe("torrent-get");
      expect(args?.ids).toEqual([hash]);
      return { torrents: [{ hashString: hash, labels: ["Podible Movies"], downloadDir: directory, status: 4,
        leftUntilDone: 100 - done, sizeWhenDone: 100, rateDownload: done ? 2 : 0, error: 0, errorString: "not public" }] };
    } } };
    const handler = createPodibleFetchHandler(repo, Date.now(), null, { acquisitionTransmission: reader });
    const url = `http://fixture.invalid/api/trpc/acquisition.transfer?${new URLSearchParams({ input: JSON.stringify(scope) })}`;
    for (const [token, error] of [[null, "UNAUTHORIZED"], ["transfer-other", "FORBIDDEN"]] as const) {
      const response = await handler(new Request(url, { headers: token ? { Authorization: `Bearer ${token}` } : {} }));
      expect(await response.text()).toContain(error);
    }
    expect(calls).toBe(0);
    const alive = () => !!repo.getSessionByTokenHash(hashSessionToken("transfer-owner"));
    const stream = transferSamples(repo, scope, alive, abort.signal, reader);
    const second = transferSamples(repo, scope, alive, abort.signal, reader);
    const zero = { sample: { bytesDone: 0, sizeBytes: 100, bytesPerSecond: 0, etaSeconds: null } };
    expect((await stream.next()).value).toEqual(zero);
    expect((await second.next()).value).toEqual(zero);
    expect(calls).toBe(1);
    done = 62;
    const next = await Promise.all([stream.next(), Bun.sleep(transferIntervalMs + 25)]);
    expect(next[0].value).toEqual({ sample: { bytesDone: 62, sizeBytes: 100, bytesPerSecond: 2, etaSeconds: 19 } });
    expect(calls).toBe(2);
    repo.markJobCancelled(batch.job_id!, "fixture stopped");
    expect(await readTransfer(repo, scope, reader)).toBeNull();
    expect(calls).toBe(2);
    repo.transaction(() => { db.query("DELETE FROM sessions WHERE token_hash=?").run(hashSessionToken("transfer-owner")); });
    expect((await second.next()).done).toBe(true);
  } finally { abort.abort(); await rm(root, { recursive: true, force: true }); }
}, 16_000);
