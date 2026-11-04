import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createTRPCClient, httpBatchLink, httpLink } from "@trpc/client";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { createSpaRoutes } from "../../src/http/spa-routes";
import { BooksRepo } from "../../src/repo";
import type { AppRouter } from "../../src/trpc";
import { authRouter } from "../../src/rpc/auth-router";

test("typed mutation clients handle TLS proxy origins and decode cross-origin rejections", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const admin = repo.upsertUser({ provider: "local", providerUserId: "proxy-admin", username: "admin", isAdmin: true });
  repo.createSession(admin.id, hashSessionToken("proxy-admin"), null, "browser");
  const handler = createPodibleFetchHandler(repo, Date.now());
  const client = (headers: Record<string, string>, link: typeof httpBatchLink | typeof httpLink = httpBatchLink) => createTRPCClient<AppRouter>({
    links: [link({ url: "http://podible.example/api/trpc", fetch: async (input, init) => {
      const response = await handler(new Request(input.toString(), {
        ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), cookie: "podible_session=proxy-admin", ...headers },
      }));
      return { status: response.status, ok: response.ok, headers: response.headers, json: () => response.json(), text: () => response.text() };
    } })],
  });
  try {
    const failed = () => repo.createJob({ type: "full_library_refresh", status: "failed" });
    const job = failed();
    await expect(client({ origin: "https://podible.example", "x-forwarded-proto": "https", "sec-fetch-site": "same-origin" })
      .jobs.retry.mutate({ jobId: job.id })).resolves.toMatchObject({ job: { id: job.id } });
    expect(db.query("SELECT kind FROM job_commands WHERE job_id=? ORDER BY id DESC LIMIT 1").get(job.id)).toEqual({ kind: "retry" });
    const rejectedHeaders: Record<string, string>[] = [
      { origin: "https://foreign.example", "x-forwarded-proto": "https" },
      { origin: "null", "x-forwarded-proto": "https" },
      { origin: "http://podible.example", "x-forwarded-proto": "https" },
      { origin: "https://podible.example:8443", "x-forwarded-proto": "https" },
      { origin: "https://podible.example", "x-forwarded-proto": "https", "sec-fetch-site": "cross-site" },
      { origin: "https://foreign.example", "x-forwarded-proto": "https", "x-forwarded-host": "foreign.example" },
      { origin: "https://podible.example", "x-forwarded-proto": "https, http" },
    ];
    for (const headers of rejectedHeaders) {
      for (const link of [httpBatchLink, httpLink]) {
        const blocked = failed();
        const before = db.query("SELECT COUNT(*) AS n FROM job_commands WHERE job_id=?").get(blocked.id);
        await expect(client(headers, link).jobs.retry.mutate({ jobId: blocked.id })).rejects.toMatchObject({
          message: "Cross-origin mutation rejected", data: { code: "FORBIDDEN", httpStatus: 403 },
        });
        expect(repo.getJob(blocked.id)?.status).toBe("failed");
        expect(db.query("SELECT COUNT(*) AS n FROM job_commands WHERE job_id=?").get(blocked.id)).toEqual(before);
      }
    }
    const direct = failed();
    await expect(client({ origin: "http://podible.example" }).jobs.retry.mutate({ jobId: direct.id }))
      .resolves.toMatchObject({ job: { id: direct.id } });
    expect(db.query("SELECT kind FROM job_commands WHERE job_id=? ORDER BY id DESC LIMIT 1").get(direct.id)).toEqual({ kind: "retry" });
  } finally { db.close(); }
});

test("typed API shares JSON-RPC results, validates inputs and preserves session auth", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const user = repo.upsertUser({
    provider: "local",
    providerUserId: "spa-test",
    username: "reader",
    isAdmin: false,
  });
  repo.createSession(user.id, hashSessionToken("test-token"), null, "browser");
  repo.createBook({ title: "A good story", author: "An author" });
  const handler = createPodibleFetchHandler(repo, Date.now());
  const client = (authenticated: boolean) =>
    createTRPCClient<AppRouter>({
      links: [
        httpBatchLink({
          url: "http://localhost/api/trpc",
          fetch: async (input, init) => {
            const response = await handler(
              new Request(input.toString(), {
                ...init,
                headers: {
                  ...Object.fromEntries(new Headers(init?.headers)),
                  ...(authenticated
                    ? { cookie: "podible_session=test-token" }
                    : {}),
                },
              }),
            );
            return {
              status: response.status,
              ok: response.ok,
              headers: response.headers,
              json: () => response.json(),
              text: () => response.text(),
            };
          },
        }),
      ],
    });
  try {
    await expect(client(false).auth.me.query({})).rejects.toMatchObject({
      data: { code: "UNAUTHORIZED" },
    });
    const typed = await client(true).auth.me.query({});
    const legacy = await handler(
      new Request("http://localhost/rpc", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          cookie: "podible_session=test-token",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "auth.me",
          params: {},
        }),
      }),
    );
    const payload = (await legacy.json()) as { result: unknown };
    expect(typed).toEqual(
      authRouter.routes.me.resultSchema.parse(payload.result),
    );
    const books = await client(true).library.list.query({
      limit: 20,
      q: undefined,
      cursor: undefined,
    });
    expect(books.items[0]?.title).toBe("A good story");
    const invalid = await handler(
      new Request(
        "http://localhost/api/trpc/library.get?input=%7B%22bookId%22%3A-1%7D",
        { headers: { cookie: "podible_session=test-token" } },
      ),
    );
    expect(invalid.status).toBe(400);
    const csrf = await handler(
      new Request("http://localhost/api/trpc/jobs.retry", {
        method: "POST",
        headers: {
          origin: "https://evil.example",
          "Content-Type": "application/json",
        },
        body: "{}",
      }),
    );
    expect(csrf.status).toBe(403);
    expect(
      (
        await handler(new Request("http://localhost/api/trpc/does.not.exist"))
      ).headers.get("content-type"),
    ).toContain("application/json");
  } finally {
    db.close();
  }
});

test("SPA deep links and immutable assets do not hide missing resources", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "podible-spa-"));
  try {
    await mkdir(path.join(directory, "web-assets"));
    await Bun.write(
      path.join(directory, "index.html"),
      "<!doctype html><title>Podible</title>",
    );
    await Bun.write(
      path.join(directory, "web-assets/test-123.js"),
      "export const ready = true;",
    );
    const app = createSpaRoutes(directory);
    expect((await app.request("http://localhost/movies/42")).status).toBe(
      200,
    );
    expect(
      (await app.request("http://localhost/movies/42")).headers.get(
        "cache-control",
      ),
    ).toBe("no-cache");
    expect(
      (
        await app.request("http://localhost/web-assets/test-123.js")
      ).headers.get("cache-control"),
    ).toContain("immutable");
    for (const [oldPath, canonical] of [
      ["/app", "/"],
      ["/app/", "/"],
      ["/app/movies/42?q=hello", "/movies/42?q=hello"],
      ["/app/requests/12", "/requests/12"],
      ["/movie-shadow", "/movie-dual-write#radarr-audit"],
      ["/movie-shadow/7?history=1", "/movie-dual-write?history=1#radarr-audit"],
      ["/app/movie-shadow", "/movie-dual-write#radarr-audit"],
      ["/app/movie-shadow/7/", "/movie-dual-write#radarr-audit"],
      ["/app/movie-dual-write", "/movie-dual-write"],
      ["/app/assets/test-123.js", "/web-assets/test-123.js"],
    ]) {
      const redirected = await app.request(`http://localhost${oldPath}`);
      expect(redirected.status).toBe(308);
      expect(redirected.headers.get("location")).toBe(canonical);
    }
    for (const route of [
      "/app/assets/missing.js",
      "/web-assets/missing.js",
      "/app/unknown",
      "/app/missing.css",
    ])
      expect((await app.request(`http://localhost${route}`)).status).toBe(404);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

// These assertions are compiled, never sent over the network.
function typedContract(client: ReturnType<typeof createTRPCClient<AppRouter>>) {
  // @ts-expect-error No invented procedure names.
  client.library.notARealMethod.query({});
  client.auth.me.query({}).then((result) => {
    // @ts-expect-error The server owns the result type.
    const id: string = result.user.id;
    return id;
  });
}
