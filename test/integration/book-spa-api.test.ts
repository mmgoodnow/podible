import { expect, test } from "bun:test";
import { createPodibleFetchHandler } from "../../src/http";
import { appRouter } from "../../src/trpc";
import { hashSessionToken } from "../../src/auth";
import { createBookSpaData } from "../fixtures/book-spa-data";

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

test("default SPA and classic links coexist without catching auth, RPC, media, or missing assets", async () => {
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
      "/app/book/1",
      "/series?name=The+House",
      "/author?name=Susanna+Clarke",
    ]) {
      const response = await request(route);
      expect(response.status).toBe(200);
      expect(await response.text()).toContain('<div id="root">');
    }
    expect(await (await request("/classic/book/1")).text()).toContain(
      "Chapter preview",
    );
    expect((await request("/login")).status).not.toBe(404);
    for (const route of [
      "/not-a-page",
      "/app/assets/not-real.js",
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
