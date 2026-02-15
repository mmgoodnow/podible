import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { createPodibleFetchHandler } from "../../src/kindling/http";
import { KindlingRepo } from "../../src/kindling/repo";

async function rpc(fetchHandler: (request: Request) => Promise<Response>, method: string, params: unknown, id = 1) {
  const response = await fetchHandler(
    new Request("http://localhost/rpc", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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

describe("podible http", () => {
  test("serves root html home page", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
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
    expect(body.includes("Podible Backend")).toBe(true);
    expect(body.includes("POST /rpc")).toBe(true);
    expect(body.includes("Open Library Search")).toBe(true);
    expect(body.includes("Settings JSON")).toBe(true);
    expect(body.includes("settings-editor")).toBe(true);

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
                isbn: ["9780441172719"],
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
      const repo = new KindlingRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());

      const healthRpc = await rpc(fetchHandler, "system.health", {}, 1);
      expect(healthRpc.result.ok).toBe(true);

      const createdRpc = await rpc(fetchHandler, "library.create", { title: "Dune", author: "Frank Herbert" }, 2);
      expect(createdRpc.result.book.title).toBe("Dune");
      expect(createdRpc.result.book.isbn).toBe("9780441172719");
      expect(createdRpc.result.book.identifiers.openlibrary).toBe("/works/OL123W");

      const listRpc = await rpc(fetchHandler, "library.list", { limit: 10 }, 3);
      expect(Array.isArray(listRpc.result.items)).toBe(true);
      expect(listRpc.result.items.length).toBe(1);

      const settingsRpc = await rpc(fetchHandler, "settings.get", {}, 4);
      expect(settingsRpc.result.auth.mode).toBe("local");

      const removed = [
        new Request("http://localhost/health", { method: "GET" }),
        new Request("http://localhost/server", { method: "GET" }),
        new Request("http://localhost/settings", { method: "GET" }),
        new Request("http://localhost/settings", { method: "PUT", body: "{}" }),
        new Request("http://localhost/openlibrary/search?q=dune", { method: "GET" }),
        new Request("http://localhost/library?limit=10", { method: "GET" }),
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
        const response = await fetchHandler(request);
        expect(response.status).toBe(404);
      }

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc openlibrary.search and add-by-key flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      const query = url.searchParams.get("q") ?? "";
      if (query.startsWith("key:/works/OL45804W")) {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
                isbn: ["9780553283686"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }

      if (query === "Hyperion Dan Simmons") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL45804W",
                title: "Hyperion",
                author_name: ["Dan Simmons"],
                first_publish_year: 1989,
                language: ["eng"],
                isbn: ["9780553283686"],
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
      const repo = new KindlingRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());

      const found = await rpc(fetchHandler, "openlibrary.search", { q: "Hyperion Dan Simmons", limit: 5 }, 1);
      expect(found.result.results.length).toBe(1);
      expect(found.result.results[0].openLibraryKey).toBe("/works/OL45804W");

      const created = await rpc(fetchHandler, "library.create", { openLibraryKey: "/works/OL45804W" }, 2);
      expect(created.result.book.title).toBe("Hyperion");
      expect(created.result.book.author).toBe("Dan Simmons");
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL45804W");
      expect(created.result.book.isbn).toBe("9780553283686");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports rpc add-by-isbn flow", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = new URL(String(input));
      if (url.origin !== "https://openlibrary.org" || url.pathname !== "/search.json") {
        throw new Error(`Unexpected external fetch in test: ${url.toString()}`);
      }

      if (url.searchParams.get("isbn") === "9780061120084") {
        return new Response(
          JSON.stringify({
            docs: [
              {
                key: "/works/OL82563W",
                title: "To Kill a Mockingbird",
                author_name: ["Harper Lee"],
                first_publish_year: 1960,
                language: ["eng"],
                isbn: ["9780061120084"],
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
      const repo = new KindlingRepo(db);
      const settings = repo.ensureSettings();
      repo.updateSettings({
        ...settings,
        auth: { ...settings.auth, mode: "local" },
        torznab: [],
      });

      const fetchHandler = createPodibleFetchHandler(repo, Date.now());
      const created = await rpc(fetchHandler, "library.create", { isbn: "9780061120084" }, 1);

      expect(created.result.book.title).toBe("To Kill a Mockingbird");
      expect(created.result.book.author).toBe("Harper Lee");
      expect(created.result.book.isbn).toBe("9780061120084");
      expect(created.result.book.identifiers.openlibrary).toBe("/works/OL82563W");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports read-only GET rpc bridge and blocks mutating methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    const settings = repo.ensureSettings();
    repo.updateSettings({
      ...settings,
      auth: { ...settings.auth, mode: "local" },
      torznab: [],
    });
    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    const readRes = await fetchHandler(new Request(`http://localhost/rpc/library/get?bookId=${book.id}`));
    expect(readRes.status).toBe(200);
    const readJson = (await readRes.json()) as any;
    expect(readJson.jsonrpc).toBe("2.0");
    expect(readJson.id).toBeNull();
    expect(readJson.result.book.id).toBe(book.id);

    const writeRes = await fetchHandler(
      new Request("http://localhost/rpc/settings/update?auth.mode=local&auth.key=test")
    );
    expect(writeRes.status).toBe(200);
    const writeJson = (await writeRes.json()) as any;
    expect(writeJson.error.code).toBe(-32601);

    db.close();
  });
});
