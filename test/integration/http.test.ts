import { afterAll, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const isolatedDataDir = await mkdtemp(path.join(os.tmpdir(), "podible-http-test-data-"));

const { runMigrations } = await import("../../src/books/db");
const { createPodibleFetchHandler } = await import("../../src/books/http");
const { BooksRepo } = await import("../../src/books/repo");
const { hashSessionToken } = await import("../../src/books/auth");

const TINY_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+XGfQAAAAASUVORK5CYII=";

function createBrowserSessionCookie(
  repo: InstanceType<typeof BooksRepo>,
  options: { provider?: "local" | "plex"; username?: string; displayName?: string; isAdmin?: boolean } = {}
): string {
  const provider = options.provider ?? "local";
  const username = options.username ?? (options.isAdmin ? "admin" : "user");
  const user = repo.upsertUser({
    provider,
    providerUserId: `${provider}-${username}`,
    username,
    displayName: options.displayName ?? username,
    isAdmin: options.isAdmin ?? false,
  });
  const token = `${username}-session-token`;
  repo.createSession(user.id, hashSessionToken(token), new Date(Date.now() + 60_000).toISOString());
  return `podible_session=${token}`;
}

async function rpc(
  fetchHandler: (request: Request) => Promise<Response>,
  method: string,
  params: unknown,
  id = 1,
  headers: Record<string, string> = {}
) {
  const response = await fetchHandler(
    new Request("http://localhost/rpc", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        method,
        params,
      }),
    })
  );
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

afterAll(async () => {
  await rm(isolatedDataDir, { recursive: true, force: true });
});

describe("podible http", () => {
  test("serves root html user landing page", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const home = await fetchHandler(new Request("http://localhost/"));
    expect(home.status).toBe(200);
    expect(home.headers.get("content-type")).toContain("text/html");
    const body = await home.text();
    expect(body.includes("Podible")).toBe(true);
    expect(body.includes("Sign in")).toBe(true);
    expect(body.includes(">Admin<")).toBe(false);

    const adminRedirect = await fetchHandler(new Request("http://localhost/admin"));
    expect(adminRedirect.status).toBe(303);
    expect(adminRedirect.headers.get("location")).toBe("/login?redirectTo=%2Fadmin");

    const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });
    const admin = await fetchHandler(
      new Request("http://localhost/admin", {
        headers: { cookie: adminCookie },
      })
    );
    expect(admin.status).toBe(200);
    const adminBody = await admin.text();
    expect(adminBody.includes("site-nav")).toBe(true);
    expect(adminBody.includes("Admin")).toBe(true);
    expect(adminBody.includes("Manual Search + Snatch")).toBe(true);
    expect(adminBody.includes("Users")).toBe(true);
    expect(adminBody.includes("admin")).toBe(true);
    expect(adminBody.includes("manual-import-btn")).toBe(true);
    expect(adminBody.includes("settings-editor")).toBe(true);
    expect(adminBody.includes("Refresh Library")).toBe(true);
    expect(adminBody.includes("wipe-db-btn")).toBe(true);
    expect(adminBody.includes("Feed Preview")).toBe(false);
    expect(adminBody.includes("Recent Library")).toBe(false);
    expect(adminBody.includes("Open Library Search")).toBe(false);

    db.close();
  });

  test("supports rpc health/settings/library and removed rest routes 404", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (url.startsWith("https://openlibrary.org/")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL123W",
                title: "Dune",
                author_name: ["Frank Herbert"],
                first_publish_year: 1965,
                language: ["eng"],
              },
            ],
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
      const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });

      const healthRpc = await rpc(fetchHandler, "system.health", {}, 1);
      expect(healthRpc.result.ok).toBe(true);

      const createdRpc = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL123W" }, 2, {
        cookie: userCookie,
      });
      expect(createdRpc.result.book.title).toBe("Dune");
      expect(createdRpc.result.book.identifiers.openlibrary).toBe("/works/OL123W");
      expect(createdRpc.result.acquisition_job_id).toBeGreaterThan(0);

      const jobRpc = await rpc(fetchHandler, "jobs.get", { jobId: createdRpc.result.acquisition_job_id }, 21, {
        cookie: adminCookie,
      });
      expect(jobRpc.result.job.id).toBe(createdRpc.result.acquisition_job_id);
      expect(jobRpc.result.job.type).toBe("acquire");

      const listRpc = await rpc(fetchHandler, "library.list", { limit: 10 }, 3, {
        cookie: userCookie,
      });
      expect(Array.isArray(listRpc.result.items)).toBe(true);
      expect(listRpc.result.items.length).toBe(1);

      const settingsRpc = await rpc(fetchHandler, "settings.get", {}, 4, {
        cookie: adminCookie,
      });
      expect(settingsRpc.result.auth.mode).toBe("local");

      const removed = [
        new Request("http://localhost/health", { method: "GET" }),
        new Request("http://localhost/server", { method: "GET" }),
        new Request("http://localhost/settings", { method: "GET" }),
        new Request("http://localhost/settings", { method: "PUT", body: "{}" }),
        new Request("http://localhost/openlibrary/search?q=dune", { method: "GET" }),
        new Request("http://localhost/library", { method: "POST", body: "{}" }),
        new Request("http://localhost/library/refresh", { method: "POST" }),
        new Request("http://localhost/library/1", { method: "GET" }),
        new Request("http://localhost/search", { method: "POST", body: "{}" }),
        new Request("http://localhost/snatch", { method: "POST", body: "{}" }),
        new Request("http://localhost/releases?bookId=1", { method: "GET" }),
        new Request("http://localhost/downloads", { method: "GET" }),
        new Request("http://localhost/downloads/1", { method: "GET" }),
        new Request("http://localhost/downloads/1/retry", { method: "POST" }),
        new Request("http://localhost/import/reconcile", { method: "POST" }),
      ];
      for (const request of removed) {
        const response = await fetchHandler(
          new Request(request.url, {
            method: request.method,
            headers: { cookie: adminCookie },
            body: request.method === "GET" ? undefined : await request.text(),
          })
        );
        expect(response.status).toBe(404);
      }

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("serves library and book detail pages", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      files: [
        {
          path: path.join(isolatedDataDir, "dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const library = await fetchHandler(
      new Request("http://localhost/library", {
        headers: { cookie: userCookie },
      })
    );
    expect(library.status).toBe(200);
    const libraryBody = await library.text();
    expect(libraryBody.includes("Library")).toBe(true);
    expect(libraryBody.includes("Dune")).toBe(true);
    expect(libraryBody.includes("Search by title or author")).toBe(true);

    const detail = await fetchHandler(
      new Request(`http://localhost/book/${book.id}`, {
        headers: { cookie: userCookie },
      })
    );
    expect(detail.status).toBe(200);
    const detailBody = await detail.text();
    expect(detailBody.includes("Play audio")).toBe(true);
    expect(detailBody.includes("Available now")).toBe(true);
    expect(detailBody.includes("Find audio")).toBe(true);
    expect(detailBody.includes("Release history")).toBe(true);

    db.close();
  });

  test("supports library search query in SSR", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.createBook({ title: "Hyperion", author: "Dan Simmons" });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const library = await fetchHandler(
      new Request("http://localhost/library?q=Hyperion", {
        headers: { cookie: userCookie },
      })
    );
    expect(library.status).toBe(200);
    const libraryBody = await library.text();
    expect(libraryBody.includes("matching “Hyperion”")).toBe(true);
    expect(libraryBody.includes("Hyperion")).toBe(true);
    expect(libraryBody.includes("Dune")).toBe(false);

    db.close();
  });

  test("queues acquire from the book detail page", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const queued = await fetchHandler(
      new Request(`http://localhost/book/${book.id}/acquire`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded", cookie: userCookie },
        body: "media=audio",
      })
    );
    expect(queued.status).toBe(303);
    const location = queued.headers.get("location");
    expect(location).toContain(`/book/${book.id}?notice=`);

    const jobs = repo.listJobsByType("acquire");
    expect(jobs.length).toBe(1);
    expect(jobs[0]?.book_id).toBe(book.id);

    db.close();
  });

  test("serves activity page and queues a refresh job", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      files: [
        {
          path: path.join(isolatedDataDir, "activity-dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    const failed = repo.createBook({ title: "Broken Book", author: "Someone" });
    const failedRelease = repo.createRelease({
      bookId: failed.id,
      provider: "test",
      title: "Broken Book",
      mediaType: "audio",
      infoHash: "abcdef1234567890abcdef1234567890abcdef12",
      url: "https://example.com/broken.torrent",
      status: "failed",
    });
    repo.setReleaseStatus(failedRelease.id, "failed", "boom");
    repo.createJob({ type: "acquire", bookId: book.id, status: "running" });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const activity = await fetchHandler(
      new Request("http://localhost/activity", {
        headers: { cookie: userCookie },
      })
    );
    expect(activity.status).toBe(200);
    const activityBody = await activity.text();
    expect(activityBody.includes("Books in progress")).toBe(true);
    expect(activityBody.includes("Recently ready")).toBe(true);
    expect(activityBody.includes("Needs attention")).toBe(true);
    expect(activityBody.includes("Refresh library")).toBe(true);
    expect(activityBody.includes("Broken Book")).toBe(true);

    const refreshed = await fetchHandler(
      new Request("http://localhost/activity/refresh", {
        method: "POST",
        headers: { cookie: userCookie },
      })
    );
    expect(refreshed.status).toBe(303);
    expect(refreshed.headers.get("location")).toContain("/activity?notice=");
    expect(repo.listJobsByType("full_library_refresh").length).toBe(1);

    db.close();
  });

  test("queues a refresh job from the admin page", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });
    const refreshed = await fetchHandler(
      new Request("http://localhost/admin/refresh", {
        method: "POST",
        headers: { cookie: adminCookie },
      })
    );
    expect(refreshed.status).toBe(303);
    expect(refreshed.headers.get("location")).toContain("/admin?notice=");
    expect(repo.listJobsByType("full_library_refresh").length).toBe(1);

    db.close();
  });

  test("redirects logged-out page requests to login", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const library = await fetchHandler(new Request("http://localhost/library"));
    expect(library.status).toBe(303);
    expect(library.headers.get("location")).toBe("/login?redirectTo=%2Flibrary");

    const book = await fetchHandler(new Request("http://localhost/book/1?foo=bar"));
    expect(book.status).toBe(303);
    expect(book.headers.get("location")).toBe("/login?redirectTo=%2Fbook%2F1%3Ffoo%3Dbar");

    db.close();
  });

  test("serves add page search and add flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if (url.pathname === "/search.json") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      if (url.pathname === "/works/OL45804W.json") {
        return new Response(
          JSON.stringify({
            description: "On the world called Hyperion, beyond the law of the Hegemony of Man, there waits the creature called the Shrike.",
            covers: [12345],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "alice" });

      const add = await fetchHandler(
        new Request("http://localhost/add?q=Hyperion%20Dan%20Simmons", {
          headers: { cookie: userCookie },
        })
      );
      expect(add.status).toBe(200);
      const addBody = await add.text();
      expect(addBody.includes("Add a book")).toBe(true);
      expect(addBody.includes("Hyperion")).toBe(true);
      expect(addBody.includes("/works/OL45804W")).toBe(true);

      const created = await fetchHandler(
        new Request("http://localhost/add", {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded", cookie: userCookie },
          body: "openLibraryKey=%2Fworks%2FOL45804W",
        })
      );
      expect(created.status).toBe(303);
      const location = created.headers.get("location");
      expect(location).toBe("/book/1");

      const detail = await fetchHandler(
        new Request(`http://localhost${location}`, {
          headers: { cookie: userCookie },
        })
      );
      expect(detail.status).toBe(200);
      const detailBody = await detail.text();
      expect(detailBody.includes("Hyperion")).toBe(true);
      expect(detailBody.includes("Dan Simmons")).toBe(true);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports local user login and session-based browser access", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      files: [
        {
          path: path.join(isolatedDataDir, "login-dune.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    const unauthorized = await fetchHandler(new Request("http://app.test/library"));
    expect(unauthorized.status).toBe(303);
    expect(unauthorized.headers.get("location")).toBe("/login?redirectTo=%2Flibrary");

    const publicHome = await fetchHandler(new Request("http://app.test/"));
    expect(publicHome.status).toBe(200);
    const publicHomeBody = await publicHome.text();
    expect(publicHomeBody.includes("Sign in")).toBe(true);
    expect(publicHomeBody.includes("Your audiobook shelf")).toBe(false);

    const loginPage = await fetchHandler(new Request("http://app.test/login?redirectTo=%2Flibrary"));
    expect(loginPage.status).toBe(200);
    const loginBody = await loginPage.text();
    expect(loginBody.includes("Create a user")).toBe(true);

    const login = await fetchHandler(
      new Request("http://app.test/login?redirectTo=%2Flibrary", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "username=alice&displayName=Alice",
      })
    );
    expect(login.headers.get("location")).toBe("/library");
    expect(login.status).toBe(303);
    const setCookie = login.headers.get("set-cookie") ?? "";
    expect(setCookie.includes("podible_session=")).toBe(true);

    const cookieHeader = setCookie.split(";")[0] ?? "";
    const authed = await fetchHandler(
      new Request("http://app.test/library", {
        headers: { cookie: cookieHeader },
      })
    );
    expect(authed.status).toBe(200);
    const authedBody = await authed.text();
    expect(authedBody.includes("Signed in as Alice")).toBe(true);
    expect(authedBody.includes("Dune")).toBe(true);

    const logout = await fetchHandler(
      new Request("http://app.test/logout", {
        method: "POST",
        headers: { cookie: cookieHeader },
      })
    );
    expect(logout.status).toBe(303);
    expect(logout.headers.get("set-cookie")).toContain("Max-Age=0");

    db.close();
  });

  test("supports app login flow for Kindling-style bearer auth", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: {
        ...settings.auth,
        mode: "local",
        appRedirectURIs: ["kindling://auth/podible"],
      },
      torznab: [],
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    const begin = await rpc(fetchHandler, "auth.beginAppLogin", { redirectUri: "kindling://auth/podible" }, 1);
    expect(begin.result.authorizeUrl).toContain("/auth/app/");
    expect(begin.result.state).toBeTruthy();
    const authorizeUrl = begin.result.authorizeUrl as string;

    const authorize = await fetchHandler(new Request(authorizeUrl));
    expect(authorize.status).toBe(200);
    const authorizeBody = await authorize.text();
    expect(authorizeBody.includes("Create a user")).toBe(true);

    const authorizePath = new URL(authorizeUrl).pathname;
    const login = await fetchHandler(
      new Request(`http://app.test/login?redirectTo=${encodeURIComponent(`${authorizePath}/complete`)}`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "username=kindling&displayName=Kindling",
      })
    );
    expect(login.status).toBe(303);
    expect(login.headers.get("location")).toBe(`${authorizePath}/complete`);
    const browserCookie = (login.headers.get("set-cookie") ?? "").split(";")[0] ?? "";
    expect(browserCookie).toContain("podible_session=");

    const complete = await fetchHandler(
      new Request(`http://app.test${authorizePath}/complete`, {
        headers: { cookie: browserCookie },
      })
    );
    expect(complete.status).toBe(302);
    const callbackLocation = complete.headers.get("location") ?? "";
    expect(callbackLocation.startsWith("kindling://auth/podible?")).toBe(true);
    const callbackUrl = new URL(callbackLocation);
    const code = callbackUrl.searchParams.get("code");
    const state = callbackUrl.searchParams.get("state");
    expect(code).toBeTruthy();
    expect(state).toBe(begin.result.state);

    const exchange = await rpc(fetchHandler, "auth.exchange", { code }, 2);
    expect(typeof exchange.result.accessToken).toBe("string");
    expect(exchange.result.user.username).toBe("kindling");

    const me = await rpc(fetchHandler, "auth.me", {}, 3, {
      Authorization: `Bearer ${exchange.result.accessToken}`,
    });
    expect(me.result.user.username).toBe("kindling");
    expect(me.result.session.kind).toBe("app");

    db.close();
  });

  test("supports Plex browser login and creates a local session", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        const body = JSON.parse(String(init.body ?? "{}"));
        expect(body.strong).toBe(true);
        expect(typeof body.jwk?.x).toBe("string");
        expect(typeof body.jwk?.kid).toBe("string");
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        expect(url.searchParams.get("deviceJWT")?.split(".").length).toBe(3);
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "plex-jwt-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("plex-jwt-token");
        return new Response(
          JSON.stringify({
            id: "plex-user-1",
            username: "alice",
            title: "Alice",
            thumb: "https://example.com/alice.jpg",
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "plex" },
      });

      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
      repo.addAsset({
        bookId: book.id,
        kind: "single",
        mime: "audio/mpeg",
        totalSize: 123,
        durationMs: 1_000,
        files: [
          {
            path: path.join(isolatedDataDir, "plex-dune.mp3"),
            size: 123,
            start: 0,
            end: 122,
            durationMs: 1_000,
            title: "Dune",
          },
        ],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());

      const loginPage = await fetchHandler(new Request("http://app.test/login"));
      expect(loginPage.status).toBe(200);
      expect((await loginPage.text()).includes("Sign in with Plex")).toBe(true);

      const start = await fetchHandler(
        new Request("http://app.test/login/plex/start", {
          method: "POST",
        })
      );
      expect(start.status).toBe(200);
      const startPayload = (await start.json()) as { authUrl: string; pinId: number };
      expect(startPayload.pinId).toBe(123);
      expect(startPayload.authUrl).toContain("https://app.plex.tv/auth#?");
      expect(startPayload.authUrl).toContain("PINCODE123");
      expect(startPayload.authUrl).toContain(encodeURIComponent("http://app.test/login/plex/complete?pinId=123"));

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123"));
      expect(complete.status).toBe(200);
      const completeBody = await complete.text();
      expect(completeBody.includes("Sign-in complete. You can close this window.")).toBe(true);
      const setCookie = complete.headers.get("set-cookie") ?? "";
      expect(setCookie.includes("podible_session=")).toBe(true);

      const cookieHeader = setCookie.split(";")[0] ?? "";
      const authed = await fetchHandler(
        new Request("http://app.test/library", {
          headers: { cookie: cookieHeader },
        })
      );
      expect(authed.status).toBe(200);
      const authedBody = await authed.text();
      expect(authedBody.includes("Signed in as Alice")).toBe(true);
      expect(authedBody.includes("Dune")).toBe(true);
      expect(repo.listUsers("plex")[0]?.is_admin).toBe(1);
      expect(repo.getSettings().auth.plex.ownerToken).toBe("plex-jwt-token");
      expect(repo.getPlexLoginAttempt(123)).toBeNull();

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("allows admin to select which Plex server controls access", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/resources") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("owner-token");
        return new Response(
          `<?xml version="1.0"?>
          <MediaContainer>
            <Device name="Family Server" product="Plex Media Server" clientIdentifier="family-server" owned="1" provides="server"/>
            <Device name="Friends Server" product="Plex Media Server" clientIdentifier="friends-server" owned="1" provides="server"/>
          </MediaContainer>`,
          { status: 200, headers: { "Content-Type": "application/xml" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: {
            ...settings.auth.plex,
            ownerToken: "owner-token",
          },
        },
      });
      const adminUser = repo.upsertUser({
        provider: "plex",
        providerUserId: "owner",
        username: "owner",
        displayName: "Owner",
        isAdmin: true,
      });
      repo.createSession(adminUser.id, hashSessionToken("admin-session"), new Date(Date.now() + 60_000).toISOString());

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const adminCookie = "podible_session=admin-session";

      const page = await fetchHandler(
        new Request("http://app.test/admin", {
          headers: { cookie: adminCookie },
        })
      );
      expect(page.status).toBe(200);
      const pageBody = await page.text();
      expect(pageBody.includes("Plex Access Control")).toBe(true);
      expect(pageBody.includes("Family Server")).toBe(true);
      expect(pageBody.includes("Friends Server")).toBe(true);

      const selected = await fetchHandler(
        new Request("http://app.test/admin/plex/select", {
          method: "POST",
          headers: {
            cookie: adminCookie,
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: "machineId=family-server&machineName=Family%20Server",
        })
      );
      expect(selected.status).toBe(303);
      expect(repo.getSettings().auth.plex.machineId).toBe("family-server");
      expect(repo.getSettings().auth.plex.machineName).toBe("Family Server");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("gates new Plex sign-ins by access to the selected Plex server", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins" && init?.method === "POST") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/pins/123") {
        return new Response(JSON.stringify({ id: 123, code: "PINCODE123", authToken: "candidate-token" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/v2/user") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("candidate-token");
        return new Response(
          JSON.stringify({
            id: "friend-user",
            username: "friend",
            title: "Friend",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.origin === "https://plex.tv" && url.pathname === "/api/users") {
        expect(new Headers(init?.headers).get("X-Plex-Token")).toBe("owner-token");
        return new Response(
          `<?xml version="1.0"?>
          <MediaContainer>
            <User id="friend-user" title="Friend">
              <Server machineIdentifier="other-server" />
            </User>
          </MediaContainer>`,
          { status: 200, headers: { "Content-Type": "application/xml" } }
        );
      }
      throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          mode: "plex",
          plex: {
            ...settings.auth.plex,
            ownerToken: "owner-token",
            machineId: "family-server",
            machineName: "Family Server",
          },
        },
      });
      repo.upsertUser({
        provider: "plex",
        providerUserId: "owner-user",
        username: "owner",
        displayName: "Owner",
        isAdmin: true,
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const start = await fetchHandler(new Request("http://app.test/login/plex/start", { method: "POST" }));
      expect(start.status).toBe(200);

      const complete = await fetchHandler(new Request("http://app.test/login/plex/complete?pinId=123"));
      expect(complete.status).toBe(400);
      expect(complete.headers.get("set-cookie")).toBeNull();
      const body = await complete.text();
      expect(body.includes("This Plex user is not allowed on this Podible instance.")).toBe(true);
      expect(repo.listUsers("plex")).toHaveLength(1);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("does not grant admin or rpc access to non-admin browser sessions", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "plex" },
    });

    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: "plex-user-2",
      username: "bob",
      displayName: "Bob",
      isAdmin: false,
    });
    repo.createSession(user.id, hashSessionToken("session-token"), new Date(Date.now() + 60_000).toISOString());

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const cookieHeader = "podible_session=session-token";

    const library = await fetchHandler(
      new Request("http://app.test/library", {
        headers: { cookie: cookieHeader },
      })
    );
    expect(library.status).toBe(200);

    const admin = await fetchHandler(
      new Request("http://app.test/admin", {
        headers: { cookie: cookieHeader },
      })
    );
    expect(admin.status).toBe(403);

    const rpcResponse = await fetchHandler(
      new Request("http://app.test/rpc", {
        method: "POST",
        headers: {
          cookie: cookieHeader,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "settings.get",
          params: {},
        }),
      })
    );
    expect(rpcResponse.status).toBe(200);
    const rpcPayload = (await rpcResponse.json()) as any;
    expect(rpcPayload.error.code).toBe(-32003);

    db.close();
  });

  test("supports rpc openlibrary.search and add-by-key-from-search flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      const query = url.searchParams.get("q") ?? "";
      if (query === "Hyperion Dan Simmons" || query.includes("OL45804W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

      const found = await rpc(fetchHandler, "openlibrary.search", { q: "Hyperion Dan Simmons", limit: 5 }, 1, {
        cookie: userCookie,
      });
      expect(found.result.results.length).toBe(1);
      expect(found.result.results[0].openLibraryKey).toBe("/works/OL45804W");

      const created = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL45804W" }, 2, {
        cookie: userCookie,
      });
      expect(created.result.book.title).toBe("Hyperion");
      expect(created.result.book.author).toBe("Dan Simmons");
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL45804W");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc add-by-key flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if ((url.searchParams.get("q") ?? "").includes("OL82563W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL82563W",
                title: "To Kill a Mockingbird",
                author_name: ["Harper Lee"],
                first_publish_year: 1960,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
      const created = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL82563W" }, 1, {
        cookie: userCookie,
      });

      expect(created.result.book.title).toBe("To Kill a Mockingbird");
      expect(created.result.book.author).toBe("Harper Lee");
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL82563W");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports read-only GET rpc bridge and blocks mutating methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { wordCount: 188_000 });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const readRes = await fetchHandler(
      new Request(`http://localhost/rpc/library/get?bookId=${book.id}`, {
        headers: { cookie: userCookie },
      })
    );
    expect(readRes.status).toBe(200);
    const readJson = (await readRes.json()) as any;
    expect(readJson.jsonrpc).toBe("2.0");
    expect(readJson.id).toBeNull();
    expect(readJson.result.book.id).toBe(book.id);
    expect(readJson.result.book.wordCount).toBe(188000);

    const writeRes = await fetchHandler(
      new Request("http://localhost/rpc/settings/update?auth.mode=local")
    );
    expect(writeRes.status).toBe(200);
    const writeJson = (await writeRes.json()) as any;
    expect(writeJson.error.code).toBe(-32601);

    db.close();
  });

  test("supports rpc library.rehydrate for existing books", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" && url.origin !== "https://covers.openlibrary.org") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }
      if (url.pathname === "/search.json" && (url.searchParams.get("q") ?? "") === "Neuromancer William Gibson") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45754W",
                title: "Neuromancer",
                author_name: ["William Gibson"],
                first_publish_year: 1984,
                language: ["eng"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.pathname === "/works/OL45754W.json") {
        return new Response(
          JSON.stringify({
            description: "A seminal cyberpunk novel.",
            covers: [12345],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url.origin === "https://covers.openlibrary.org") {
        return new Response(Buffer.from(TINY_PNG_BASE64, "base64"), {
          status: 200,
          headers: { "Content-Type": "image/png" },
        });
      }
      return new Response(JSON.stringify({ docs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;

    try {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const settings = repo.ensureSettings();
      const libraryRoot = path.join(isolatedDataDir, "library-rehydrate");
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
        libraryRoot,
      });
      const book = repo.createBook({ title: "Neuromancer", author: "William Gibson" });
      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const adminCookie = createBrowserSessionCookie(repo, { isAdmin: true, username: "admin" });

      const hydrated = await rpc(fetchHandler, "library.rehydrate", { bookId: book.id }, 1, {
        cookie: adminCookie,
      });
      expect(hydrated.result.attempted).toBe(1);
      expect(hydrated.result.updatedBookIds).toEqual([book.id]);

      const fetched = repo.getBook(book.id);
      expect(fetched?.identifiers.openlibrary).toBe("/works/OL45754W");
      expect(fetched?.description).toBe("A seminal cyberpunk novel.");
      expect(fetched?.descriptionHtml).toContain("<p>");
      expect(fetched?.coverUrl).toBe(`/covers/${book.id}.jpg`);
      if (fetched?.coverUrl) {
        const coverRes = await fetchHandler(
          new Request(`http://localhost${fetched.coverUrl}`, {
            headers: { cookie: adminCookie },
          })
        );
        expect(coverRes.status).toBe(200);
        expect((await coverRes.arrayBuffer()).byteLength).toBeGreaterThan(16);
      }

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc library.acquire for existing books", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });

    const acquire = await rpc(fetchHandler, "library.acquire", { bookId: book.id, media: ["ebook"] }, 1, {
      cookie: userCookie,
    });
    expect(acquire.result.jobId).toBeGreaterThan(0);
    expect(acquire.result.media).toEqual(["ebook"]);

    const job = repo.getJob(acquire.result.jobId);
    expect(job?.type).toBe("acquire");
    expect(job?.book_id).toBe(book.id);
    expect(JSON.parse(job?.payload_json ?? "{}").media).toEqual(["ebook"]);

    db.close();
  });

  test("serves stored transcript json for audio assets", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const audio = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      files: [
        {
          path: path.join(isolatedDataDir, "transcript-route.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    repo.upsertAssetTranscript({
      assetId: audio.id,
      status: "succeeded",
      source: "full_transcript_epub",
      algorithmVersion: "test",
      fingerprint: "fp",
      transcriptJson: JSON.stringify({
        version: "1.2.0",
        text: "fear is the mind killer",
        words: [
          { startMs: 0, endMs: 300, text: "fear", token: "fear" },
          { startMs: 301, endMs: 500, text: "is", token: "is" },
        ],
      }),
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const response = await fetchHandler(
      new Request(`http://localhost/transcripts/${audio.id}.json`, {
        headers: { cookie: userCookie },
      })
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("application/json");
    const payload = (await response.json()) as any;
    expect(payload.text).toBe("fear is the mind killer");
    expect(payload.words[0].text).toBe("fear");

    const missing = await fetchHandler(
      new Request("http://localhost/transcripts/999.json", {
        headers: { cookie: userCookie },
      })
    );
    expect(missing.status).toBe(404);

    db.close();
  });

  test("serves brotli-compressed transcript json when requested", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const audio = repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 123,
      durationMs: 1_000,
      files: [
        {
          path: path.join(isolatedDataDir, "transcript-route-br.mp3"),
          size: 123,
          start: 0,
          end: 122,
          durationMs: 1_000,
          title: "Dune",
        },
      ],
    });
    repo.upsertAssetTranscript({
      assetId: audio.id,
      status: "succeeded",
      source: "full_transcript_epub",
      algorithmVersion: "test",
      fingerprint: "fp-br",
      transcriptJson: JSON.stringify({
        version: "1.2.0",
        text: "fear is the mind killer",
        words: [
          { startMs: 0, endMs: 300, text: "fear", token: "fear" },
          { startMs: 301, endMs: 500, text: "is", token: "is" },
        ],
      }),
    });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());
    const userCookie = createBrowserSessionCookie(repo, { username: "reader" });
    const response = await fetchHandler(
      new Request(`http://localhost/transcripts/${audio.id}.json`, {
        headers: { "Accept-Encoding": "br", cookie: userCookie },
      })
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-encoding")).toBe("br");
    expect(response.headers.get("vary")).toContain("Accept-Encoding");

    const decompressed = new Response(response.body?.pipeThrough(new DecompressionStream("brotli")));
    const payload = (await decompressed.json()) as any;
    expect(payload.text).toBe("fear is the mind killer");

    db.close();
  });
});
