import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { createPodibleFetchHandler } from "../../src/kindling/http";
import { KindlingRepo } from "../../src/kindling/repo";

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
    expect(body.includes("/health")).toBe(true);
    expect(body.includes("Open Library Search")).toBe(true);
    expect(body.includes("Settings JSON")).toBe(true);
    expect(body.includes("settings-editor")).toBe(true);

    db.close();
  });

  test("serves health and creates library book from title/author", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (url.startsWith("https://openlibrary.org/")) {
        return new Response(JSON.stringify({
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
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
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

      const health = await fetchHandler(new Request("http://localhost/health"));
      expect(health.status).toBe(200);
      const healthJson = (await health.json()) as any;
      expect(healthJson.ok).toBe(true);

      const created = await fetchHandler(
        new Request("http://localhost/library", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ title: "Dune", author: "Frank Herbert" }),
        })
      );
      expect(created.status).toBe(201);
      const createdJson = (await created.json()) as any;
      expect(createdJson.book.title).toBe("Dune");
      expect(createdJson.book.isbn).toBe("9780441172719");
      expect(createdJson.book.identifiers.openlibrary).toBe("/works/OL123W");

      const list = await fetchHandler(new Request("http://localhost/library?limit=10"));
      expect(list.status).toBe(200);
      const listJson = (await list.json()) as any;
      expect(Array.isArray(listJson.items)).toBe(true);
      expect(listJson.items.length).toBe(1);

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports openlibrary search and add-by-key flow", async () => {
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

      const found = await fetchHandler(new Request("http://localhost/openlibrary/search?q=Hyperion%20Dan%20Simmons&limit=5"));
      expect(found.status).toBe(200);
      const foundJson = (await found.json()) as any;
      expect(foundJson.results.length).toBe(1);
      expect(foundJson.results[0].openLibraryKey).toBe("/works/OL45804W");

      const created = await fetchHandler(
        new Request("http://localhost/library", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ openLibraryKey: "/works/OL45804W" }),
        })
      );
      expect(created.status).toBe(201);
      const createdJson = (await created.json()) as any;
      expect(createdJson.book.title).toBe("Hyperion");
      expect(createdJson.book.author).toBe("Dan Simmons");
      expect(createdJson.book.identifiers.openlibrary).toBe("/works/OL45804W");
      expect(createdJson.book.isbn).toBe("9780553283686");

      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("supports add-by-isbn flow", async () => {
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

      const created = await fetchHandler(
        new Request("http://localhost/library", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ isbn: "9780061120084" }),
        })
      );

      expect(created.status).toBe(201);
      const createdJson = (await created.json()) as any;
      expect(createdJson.book.title).toBe("To Kill a Mockingbird");
      expect(createdJson.book.author).toBe("Harper Lee");
      expect(createdJson.book.isbn).toBe("9780061120084");
      expect(createdJson.book.identifiers.openlibrary).toBe("/works/OL82563W");
      db.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
