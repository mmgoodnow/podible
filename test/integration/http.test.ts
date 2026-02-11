import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { createKindlingFetchHandler } from "../../src/kindling/http";
import { KindlingRepo } from "../../src/kindling/repo";

describe("kindling http", () => {
  test("serves health and creates library book", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url = String(input);
      if (url.startsWith("https://openlibrary.org/")) {
        return new Response(JSON.stringify({ docs: [] }), {
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

      const fetchHandler = createKindlingFetchHandler(repo, Date.now());

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
});
