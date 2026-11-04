import { expect, test } from "bun:test";
import { createPodibleFetchHandler } from "../../src/http";
import { appRouter } from "../../src/trpc";
import { hashSessionToken } from "../../src/auth";
import { createBookSpaData } from "../fixtures/book-spa-data";
import { FakeBookTorrentClient, bookTorrentMetainfo } from "../mocks/book-torrent";
import { infoHashFromTorrentBytes } from "../../src/library/torrent";
import { startMockTorznab } from "../mocks/torznab";

test("book client fake adds paused, guards ownership, and reuses the same managed files", async () => {
  const data = await createBookSpaData();
  try {
    const client = new FakeBookTorrentClient(data.repo.getSettings().libraryRoot);
    const metainfo = bookTorrentMetainfo("part.mp3");
    const hash = infoHashFromTorrentBytes(metainfo);
    const location = { rootId: "books", relativePath: hash };
    const ref = await client.ensureAdded({ metainfo, location });
    expect(await client.inspect(ref)).toMatchObject({ state: "inactive", complete: false, bytesDone: 0, location });
    const files = await client.files(ref);
    expect(files).toHaveLength(1);
    expect(files[0]!.path).toContain(`/.podible/downloads/${hash}/part.mp3`);
    expect(await Bun.file(files[0]!.path).text()).toHaveLength(11);
    expect(await client.ensureAdded({ metainfo, location })).toEqual(ref);
    expect(client.adds).toEqual([hash]);
    expect(await client.files(ref)).toEqual(files);
    await expect(client.ensureAdded({ metainfo, location: { ...location, relativePath: "another-torrent" } })).rejects.toThrow("location");
    await expect(client.resume({ ...ref, clientId: "rtorrent:foreign" })).rejects.toThrow("Foreign");
    await expect(client.inspect({ ...ref, nativeId: "0".repeat(40) })).rejects.toThrow("Missing");
    await client.resume(ref);
    expect(await client.inspect(ref)).toMatchObject({ state: "downloading", complete: false });
    expect(await client.inspect(ref)).toMatchObject({ state: "seeding", complete: true, bytesDone: 11 });
    await expect(client.removeRecord(ref)).rejects.toThrow("must not delete");
  } finally {
    await data.close();
  }
});

test("typed book RPC creates ordered manual editions through the private client without exposing search URLs", async () => {
  const data = await createBookSpaData();
  const torrents = { a: bookTorrentMetainfo("a.mp3"), b: bookTorrentMetainfo("b.mp3") };
  const torznab = startMockTorznab({
    results: ["a", "b"].map((part) => ({ title: `Piranesi Part ${part} [MP3]`, torrentId: part, size: 10 })),
    torrents,
  });
  try {
    const settings = data.repo.getSettings();
    settings.torznab = [{ name: "fixture", baseUrl: torznab.baseUrl }];
    settings.rtorrent.url = "http://127.0.0.1:1/legacy-must-not-be-used";
    data.repo.updateSettings(settings);
    const bookTorrentClient = new FakeBookTorrentClient(settings.libraryRoot);
    const client = appRouter.createCaller({
      repo: data.repo, request: new Request("http://localhost/"), startTime: Date.now(), bookTorrentClient,
      session: data.repo.getSessionByTokenHash(hashSessionToken("book-spa-reader")),
    });
    const search = await client.library.searchReleases({ bookId: data.book.id, mediaType: "audio", query: "" });
    expect(search.results).toHaveLength(2);
    expect(JSON.stringify(search)).not.toContain(torznab.baseUrl);
    expect(JSON.stringify(search)).not.toContain(".torrent");
    const indexes = ["b", "a"].map((part) => search.results.findIndex((result) => result.title.includes(`Part ${part}`)));
    const result = await client.library.createManifestationFromSearch({
      bookId: data.book.id, mediaType: "audio", searchId: search.searchId, indexes,
      manifestation: { label: "Ordered private edition", editionNote: "Reader selection" },
    });
    expect(result.manifestationId).toBeGreaterThan(0);
    expect(result.results.map((entry) => entry.release.info_hash)).toEqual([torrents.b, torrents.a].map(infoHashFromTorrentBytes));
    for (const [index, entry] of result.results.entries()) {
      expect(entry.idempotent).toBe(false);
      expect(data.repo.bookTorrents.get(entry.release.info_hash)).toMatchObject({
        book_id: data.book.id, release_id: entry.release.id, client_id: bookTorrentClient.id,
      });
      expect(JSON.parse(data.repo.getJob(entry.jobId)!.payload_json!)).toMatchObject({
        manifestationId: result.manifestationId, sequenceInManifestation: index,
      });
    }
    expect(bookTorrentClient.adds).toHaveLength(2);
    expect(torznab.searches).toHaveLength(1);
  } finally {
    torznab.stop();
    await data.close();
  }
});

test("book adapters keep media URLs opaque, editions scoped, failures safe, and admin actions protected", async () => {
  const data = await createBookSpaData();
  try {
    const ctx = {
      repo: data.repo,
      request: new Request("http://localhost/book/1"),
      startTime: Date.now(),
      session: data.repo.getSessionByTokenHash(
        hashSessionToken("book-spa-reader"),
      ),
    };
    const client = appRouter.createCaller(ctx);
    const editions = await client.book.editions({ bookId: data.book.id });
    expect(editions.audio).toHaveLength(2);
    expect(editions.ebooks).toHaveLength(2);
    expect(editions.ebooks[1]!.downloadUrl).toMatch(
      /^http:\/\/localhost\/ebook\/\d+$/,
    );
    const chapters = await client.book.chapters({
      bookId: data.book.id,
      manifestationId: data.manifestations[1]!.id,
    });
    expect(chapters.chapters[0]!.title).toBe("Alternate narration: The House");
    await expect(
      client.book.chapters({
        bookId: data.failedBook.id,
        manifestationId: data.manifestations[0]!.id,
      }),
    ).rejects.toThrow();
    const status = await client.book.activity({ bookId: data.failedBook.id });
    expect(status.jobs[0]!.state).toBe("waiting_provider");
    expect(status.activeAcquisition).toBe(false);
    expect(JSON.stringify(status)).not.toContain("NEVER-EXPOSE");
    expect(JSON.stringify(status)).not.toContain("provider.invalid");
    expect(JSON.stringify(status)).not.toContain("mam_id");
    await expect(
      client.import.inspect({ path: data.root }),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    await expect(
      client.library.rehydrate({ bookId: data.book.id }),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    const anonymous = appRouter.createCaller({ ...ctx, session: null });
    await expect(
      anonymous.book.activity({ bookId: data.book.id }),
    ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
    const report = await client.library.reportImportIssue({
      bookId: data.book.id,
      mediaType: "audio",
      releaseId: editions.audio[1]!.releaseIds[0],
      manifestationId: data.manifestations[1]!.id,
    });
    expect(report.action).toBe("wrong_file_review_queued");
    expect(data.repo.listAssetsByBook(data.book.id)).toHaveLength(4);
    const payload = JSON.parse(data.repo.getJob(report.jobId)!.payload_json!);
    expect(payload.rejectedManifestationId).toBe(data.manifestations[1]!.id);
    const queued = await client.library.requestTranscription({
      bookId: data.book.id,
      manifestationId: data.manifestations[1]!.id,
      force: true,
    });
    expect(queued.status).toBe("pending");
    const jobPayload = JSON.parse(
      data.repo.getJob(queued.jobId!)!.payload_json!,
    );
    expect(jobPayload.manifestationId).toBe(data.manifestations[1]!.id);
    const acquire = await client.library.acquire({
      bookId: data.failedBook.id,
      media: ["audio"],
    });
    expect(acquire.jobId).toBeGreaterThan(0);
    expect(
      (await client.book.activity({ bookId: data.failedBook.id }))
        .activeAcquisition,
    ).toBe(true);
  } finally {
    await data.close();
  }
});

test("SPA routes replace classic without catching auth, RPC, media, or missing assets", async () => {
  const data = await createBookSpaData();
  try {
    const handler = createPodibleFetchHandler(data.repo, Date.now());
    const request = (pathname: string) =>
      handler(
        new Request(`http://localhost${pathname}`, {
          headers: { cookie: "podible_session=book-spa-reader" },
        }),
      );
    for (const route of [
      "/",
      "/library",
      "/add?q=Piranesi",
      "/book/1?edition=1",
      "/movies/42",
      "/requests/1",
      "/movie-dual-write",
      "/series?name=The+House",
      "/author?name=Susanna+Clarke",
    ]) {
      const response = await request(route);
      expect(response.status).toBe(200);
      expect(await response.text()).toContain('<div id="root">');
    }
    const oldBook = await request("/app/book/1?edition=2");
    expect(oldBook.status).toBe(308);
    expect(oldBook.headers.get("location")).toBe("/book/1?edition=2");
    const oldAudit = await request("/movie-shadow");
    expect(oldAudit.status).toBe(308);
    expect(oldAudit.headers.get("location")).toBe("/movie-dual-write#radarr-audit");
    expect((await request("/login")).status).not.toBe(404);
    for (const route of [
      "/not-a-page",
      "/classic",
      "/classic/library",
      "/classic/add?q=Piranesi",
      "/classic/book/1",
      "/app/assets/not-real.js",
      "/web-assets/not-real.js",
      "/app/not-a-page",
      "/book/not-an-id",
      "/stream/m/999999.mp3",
      "/ebook/999999",
    ])
      expect((await request(route)).status).toBe(404);
    expect(
      (await request("/api/trpc/not-a-procedure")).headers.get("content-type"),
    ).toContain("application/json");
  } finally {
    await data.close();
  }
});

test("manifestation diagnostics preserve the admin tree and bound curation debug without exposing source credentials", async () => {
  const data = await createBookSpaData();
  try {
    const context = {
      repo: data.repo,
      request: new Request("http://localhost/book/1"),
      startTime: Date.now(),
      session: data.repo.getSessionByTokenHash(hashSessionToken("book-spa-admin")),
    };
    const client = appRouter.createCaller(context);
    const manifestation = data.manifestations[1]!;
    data.repo.upsertChapterAnalysis({
      manifestationId: manifestation.id, status: "succeeded", source: "fixture", algorithmVersion: "fixture",
      fingerprint: "fixture", resolvedBoundaryCount: 2, totalBoundaryCount: 3,
      debugJson: JSON.stringify({
        secret: "NEVER-EXPOSE-DEBUG",
        curation: {
          status: "completed_with_estimates", eventLogPath: "/config/curation/events.jsonl", traceDir: "/config/curation/traces",
          summary: {
            budget: { agentCallsStarted: 2, maxAgentCalls: 12, agentNodeTasksStarted: 1, maxAgentNodeTasks: 8 },
            unresolvedNodes: Array.from({ length: 15 }, (_, index) => ({ title: `Node ${index}`, failureKind: "weak_evidence", expectedStartTime: index, errors: ["Ambiguous transcript"] })),
          },
        },
      }),
    });
    const empty = data.repo.addManifestation({ bookId: data.book.id, kind: "audio", label: "Pending edition" });
    const result = await client.book.diagnostics({ bookId: data.book.id });
    expect(result.manifestations).toHaveLength(5);
    expect(result.manifestations.find((item) => item.id === empty.id)?.containers).toEqual([]);
    const edition = result.manifestations.find((item) => item.id === manifestation.id)!;
    expect(edition.containers[0]?.files[0]?.path).toBe(data.audioPath);
    expect(edition.containers[0]?.release).toMatchObject({ provider: "fixture", status: "imported" });
    expect(edition.analysis).toMatchObject({ curationStatus: "completed_with_estimates", resolvedBoundaries: 2, totalBoundaries: 3, unresolvedCount: 15 });
    expect(edition.analysis?.unresolvedNodes).toHaveLength(12);
    expect(edition.analysis?.budget).toContain("2/12 agent calls");
    expect(edition.analysis?.eventLogPath).toBe("/config/curation/events.jsonl");
    expect(JSON.stringify(result)).not.toContain("NEVER-EXPOSE-DEBUG");
    expect(JSON.stringify(result)).not.toContain("https://fixture.invalid");
    await expect(client.book.diagnostics({ bookId: 999999 })).rejects.toMatchObject({ code: "NOT_FOUND" });
    const reader = appRouter.createCaller({ ...context, session: data.repo.getSessionByTokenHash(hashSessionToken("book-spa-reader")) });
    await expect(reader.book.diagnostics({ bookId: data.book.id })).rejects.toMatchObject({ code: "FORBIDDEN" });
    await expect(appRouter.createCaller({ ...context, session: null }).book.diagnostics({ bookId: data.book.id })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
    data.db.query("UPDATE chapter_analysis SET debug_json=? WHERE manifestation_id=?").run("{unfinished", manifestation.id);
    expect((await client.book.diagnostics({ bookId: data.book.id })).manifestations.find((item) => item.id === manifestation.id)?.analysis?.unresolvedNodes).toEqual([]);
  } finally {
    await data.close();
  }
});

test("a sleeping acquisition exposes its next attempt instead of looking stuck or offering duplicate work", async () => {
  const data = await createBookSpaData();
  try {
    const job = data.repo.createJob({ type: "acquire", bookId: data.book.id });
    const nextRunAt = new Date(Date.now() + 60_000).toISOString();
    data.repo.attachJobExecution(job.id, "scheduled-acquisition");
    data.repo.bindJobJournalReader({
      read: (ids) =>
        new Map(
          ids.map((id) => [
            id,
            {
              status:
                id === "scheduled-acquisition"
                  ? ("queued" as const)
                  : ("failed" as const),
              error: null,
              attempt_count: 1,
              next_run_at: id === "scheduled-acquisition" ? nextRunAt : null,
              updated_at: job.updated_at,
            },
          ]),
        ),
    });
    const client = appRouter.createCaller({
      repo: data.repo,
      request: new Request("http://localhost/"),
      startTime: Date.now(),
      session: data.repo.getSessionByTokenHash(
        hashSessionToken("book-spa-reader"),
      ),
    });
    const activity = await client.book.activity({ bookId: data.book.id });
    expect(activity.activeAcquisition).toBe(true);
    expect(activity.jobs[0]?.state).toBe("scheduled");
    expect(activity.jobs[0]?.nextRunAt).toBe(nextRunAt);
    expect(activity.jobs[0]?.message).toContain(
      "No need to start another acquisition",
    );
  } finally {
    await data.close();
  }
});
