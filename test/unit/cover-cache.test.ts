import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { Hono } from "hono";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createCoverRoutes } from "../../src/http/asset-routes";
import { createRequestContextMiddleware, type HttpEnv } from "../../src/http/middleware";
import { BooksRepo } from "../../src/repo";

const version = "2026-01-01T00:00:00.000Z";

async function fixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "podible-cover-cache-"));
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "covers", username: "covers", isAdmin: false });
  repo.createSession(user.id, hashSessionToken("cover-cache-session"), null, "browser");
  const book = repo.createBook({ title: "Cached cover", author: "Fixture", addedByUserId: user.id });
  const coverPath = path.join(root, "cover.jpg");
  await fs.writeFile(coverPath, "first-cover");
  repo.updateBookMetadata(book.id, { coverPath });
  db.query("UPDATE books SET updated_at=? WHERE id=?").run(version, book.id);
  const app = new Hono<HttpEnv>();
  app.use("*", createRequestContextMiddleware(repo));
  app.route("/covers", createCoverRoutes(repo));
  const coverUrl = `/covers/${book.id}.jpg`;
  const currentUrl = `${coverUrl}?v=${encodeURIComponent(version)}`;
  return { root, db, repo, book, coverPath, coverUrl, currentUrl,
    request(url: string, init: RequestInit = {}) {
      const headers = new Headers(init.headers);
      if (!headers.has("cookie")) headers.set("cookie", "podible_session=cover-cache-session");
      return app.request(new Request(`http://localhost${url}`, { ...init, headers }));
    },
    async [Symbol.asyncDispose]() { db.close(); await fs.rm(root, { recursive: true, force: true }); },
  };
}

describe("authenticated cover HTTP cache", () => {
  test("current version has a bounded private cache and a stable metadata validator", async () => {
    await using f = await fixture();
    const first = await f.request(f.currentUrl);
    expect(first.status).toBe(200);
    expect(first.headers.get("cache-control")).toBe("private, max-age=86400, must-revalidate");
    expect(first.headers.get("cache-control")).not.toContain("immutable");
    expect(first.headers.get("vary")).toBe("Cookie, Authorization");
    expect(first.headers.get("content-type")).toBe("image/jpeg");
    const etag = first.headers.get("etag")!;
    expect(etag).toMatch(/^W\/"[a-f0-9]{64}"$/);
    expect(await first.text()).toBe("first-cover");
    const second = await f.request(f.currentUrl);
    expect(second.headers.get("etag")).toBe(etag);
    await second.body?.cancel();

    const bodyFile = spyOn(Bun, "file");
    try {
      const warm = await f.request(f.currentUrl, { headers: { "if-none-match": etag } });
      expect(warm.status).toBe(304);
      expect(warm.headers.get("etag")).toBe(etag);
      expect(warm.headers.get("cache-control")).toBe(first.headers.get("cache-control"));
      expect(warm.headers.get("vary")).toBe("Cookie, Authorization");
      expect(await warm.text()).toBe("");
      expect(bodyFile).not.toHaveBeenCalled();
    } finally { bodyFile.mockRestore(); }
  });

  test("unversioned legacy URLs always revalidate and honor weak, strong, list and wildcard validators", async () => {
    await using f = await fixture();
    const first = await f.request(f.coverUrl);
    expect(first.headers.get("cache-control")).toBe("private, no-cache");
    const etag = first.headers.get("etag")!;
    await first.body?.cancel();
    for (const match of [etag, etag.replace(/^W\//, ""), `"different", ${etag}`, "*"]) {
      const cached = await f.request(f.coverUrl, { headers: { "if-none-match": match } });
      expect(cached.status).toBe(304);
      expect(cached.headers.get("cache-control")).toBe("private, no-cache");
    }
    const changed = await f.request(f.coverUrl, { headers: { "if-none-match": 'W/"different"' } });
    expect(changed.status).toBe(200);
    await changed.body?.cancel();
    const head = await f.request(f.currentUrl, { method: "HEAD" });
    expect(head.status).toBe(200);
    expect(head.headers.get("etag")).toBe(etag);
    expect(await head.text()).toBe("");
  });

  test("stale or ambiguous versions redirect without caching or serving bytes under the old key", async () => {
    await using f = await fixture();
    for (const query of ["v=old", "v=", `v=${encodeURIComponent(version)}&v=old`]) {
      const response = await f.request(`${f.coverUrl}?${query}&source=legacy`, { headers: { "if-none-match": "*" } });
      expect(response.status).toBe(307);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(response.headers.get("etag")).toBeNull();
      const destination = new URL(response.headers.get("location")!, "http://localhost");
      expect(destination.pathname).toBe(f.coverUrl);
      expect(destination.searchParams.getAll("v")).toEqual([version]);
      expect(destination.searchParams.get("source")).toBe("legacy");
      expect(await response.text()).toBe("");
      const current = await f.request(`${destination.pathname}${destination.search}`);
      expect(current.status).toBe(200);
      expect(await current.text()).toBe("first-cover");
    }
  });

  test("path, mtime, size and version changes invalidate the validator without reading cover contents", async () => {
    await using f = await fixture();
    let response = await f.request(f.coverUrl);
    let etag = response.headers.get("etag")!;
    await response.body?.cancel();
    const changed = async () => {
      response = await f.request(f.coverUrl, { headers: { "if-none-match": etag } });
      expect(response.status).toBe(200);
      const next = response.headers.get("etag")!;
      expect(next).not.toBe(etag);
      etag = next;
      await response.body?.cancel();
    };
    await fs.writeFile(f.coverPath, "different-size");
    await changed();
    await fs.utimes(f.coverPath, new Date("2025-01-01"), new Date("2025-01-01"));
    await changed();
    const nextPath = path.join(f.root, "another.png");
    await fs.copyFile(f.coverPath, nextPath);
    f.repo.updateBookMetadata(f.book.id, { coverPath: nextPath });
    f.db.query("UPDATE books SET updated_at=? WHERE id=?").run(version, f.book.id);
    await changed();
    expect(response.headers.get("content-type")).toBe("image/png");
    f.db.query("UPDATE books SET updated_at=? WHERE id=?").run("2026-01-02T00:00:00.000Z", f.book.id);
    await changed();
  });

  test("same-path replacement with preserved size and mtime still changes the validator", async () => {
    await using f = await fixture();
    const timestamp = new Date("2025-01-01T00:00:00.000Z");
    await fs.utimes(f.coverPath, timestamp, timestamp);
    const original = await fs.stat(f.coverPath, { bigint: true });
    const first = await f.request(f.coverUrl);
    const etag = first.headers.get("etag")!;
    await first.body?.cancel();
    const replacement = path.join(f.root, "replacement.jpg");
    await fs.writeFile(replacement, "other-cover");
    await fs.utimes(replacement, original.atime, original.mtime);
    await fs.rename(replacement, f.coverPath);
    const replaced = await fs.stat(f.coverPath, { bigint: true });
    expect(replaced.size).toBe(original.size);
    expect(replaced.mtimeNs).toBe(original.mtimeNs);
    expect(replaced.ino).not.toBe(original.ino);
    const changed = await f.request(f.coverUrl, { headers: { "if-none-match": etag } });
    expect(changed.status).toBe(200);
    expect(changed.headers.get("etag")).not.toBe(etag);
    expect(await changed.text()).toBe("other-cover");
  });

  test("authentication precedes validators and errors never receive a reusable cache policy", async () => {
    await using f = await fixture();
    const first = await f.request(f.currentUrl);
    const etag = first.headers.get("etag")!;
    await first.body?.cancel();
    for (const cookie of ["", "podible_session=invalid"]) {
      for (const match of [etag, "*"]) {
        const anonymous = await f.request(f.currentUrl, { headers: { cookie, "if-none-match": match } });
        expect(anonymous.status).toBe(401);
        expect(anonymous.headers.get("cache-control")).toBe("no-store");
        expect(anonymous.headers.get("etag")).toBeNull();
      }
    }
    for (const url of ["/covers/999999.jpg", f.coverUrl]) {
      if (url === f.coverUrl) await fs.unlink(f.coverPath);
      const missing = await f.request(url, { headers: { "if-none-match": "*" } });
      expect(missing.status).toBe(404);
      expect(missing.headers.get("cache-control")).toBe("no-store");
      expect(missing.headers.get("etag")).toBeNull();
    }
  });
});
