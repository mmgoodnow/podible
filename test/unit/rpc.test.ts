import { mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { handleRpcMethod, handleRpcRequest } from "../../src/kindling/rpc";
import { KindlingRepo } from "../../src/kindling/repo";

async function callRpc(repo: KindlingRepo, body: string | object) {
  const request = new Request("http://localhost/rpc", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  const response = await handleRpcRequest(request, { repo, startTime: Date.now() - 1000 });
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

describe("json-rpc handler", () => {
  test("dispatches representative methods", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    repo.ensureSettings();

    const settings = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "settings.get",
      params: {},
    });
    expect(settings.result.auth).toBeTruthy();

    const listed = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.list",
      params: { limit: 10 },
    });
    expect(Array.isArray(listed.result.items)).toBe(true);

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const acquire = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 22,
      method: "library.acquire",
      params: { bookId: book.id, media: ["audio"] },
    });
    expect(acquire.result.jobId).toBeGreaterThan(0);
    expect(acquire.result.media).toEqual(["audio"]);
    const acquireJob = repo.getJob(acquire.result.jobId);
    expect(acquireJob?.type).toBe("scan");
    expect(acquireJob?.book_id).toBe(book.id);
    expect(JSON.parse(acquireJob?.payload_json ?? "{}").media).toEqual(["audio"]);

    const queued = repo.createJob({ type: "scan", payload: { fullRefresh: true } });
    const jobs = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 3,
      method: "jobs.list",
      params: { limit: 5 },
    });
    expect(Array.isArray(jobs.result.jobs)).toBe(true);
    expect(jobs.result.jobs[0].id).toBe(queued.id);

    db.close();
  });

  test("returns parse error for malformed json", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, "{");
    expect(result.error.code).toBe(-32700);
    db.close();
  });

  test("rejects batch requests", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, [
      {
        jsonrpc: "2.0",
        id: 1,
        method: "settings.get",
        params: {},
      },
    ]);
    expect(result.error.code).toBe(-32600);
    db.close();
  });

  test("returns method not found", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "nope.method",
      params: {},
    });
    expect(result.error.code).toBe(-32601);
    db.close();
  });

  test("returns invalid params", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "search.run",
      params: { query: "Dune", media: "video" },
    });
    expect(result.error.code).toBe(-32602);
    db.close();
  });

  test("requires valid params for library.acquire", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const missingBook = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.acquire",
      params: { bookId: 999 },
    });
    expect(missingBook.error.code).toBe(-32000);

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    const invalidMedia = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 2,
      method: "library.acquire",
      params: { bookId: book.id, media: "video" },
    });
    expect(invalidMedia.error.code).toBe(-32602);

    db.close();
  });

  test("requires openLibraryKey for library.create", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.create",
      params: { title: "Dune" },
    });
    expect(result.error.code).toBe(-32602);
    db.close();
  });

  test("maps domain errors to -32000", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.get",
      params: { bookId: 999 },
    });
    expect(result.error.code).toBe(-32000);
    db.close();
  });

  test("library.delete cascades DB rows and removes asset+cover files", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);

    const root = await mkdtemp(path.join(os.tmpdir(), "kindling-delete-"));
    const assetPath = path.join(root, "book.mp3");
    const coverPath = path.join(root, "cover.jpg");
    await writeFile(assetPath, Buffer.from("test-audio"));
    await writeFile(coverPath, Buffer.from([0xff, 0xd8, 0xff, 0xd9]));

    const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
    repo.updateBookMetadata(book.id, { coverPath });
    const release = repo.createRelease({
      bookId: book.id,
      provider: "test",
      title: "Dune audio",
      mediaType: "audio",
      infoHash: "abc123",
      url: "https://example.com/dune-audio.torrent",
      status: "downloaded",
    });
    repo.addAsset({
      bookId: book.id,
      kind: "single",
      mime: "audio/mpeg",
      totalSize: 10,
      sourceReleaseId: release.id,
      files: [
        {
          path: assetPath,
          size: 10,
          start: 0,
          end: 9,
          durationMs: 1000,
          title: null,
        },
      ],
    });

    const result = await callRpc(repo, {
      jsonrpc: "2.0",
      id: 1,
      method: "library.delete",
      params: { bookId: book.id },
    });
    expect(result.result.deletedBookId).toBe(book.id);
    expect(result.result.deletedAssetFileCount).toBe(1);
    expect(repo.getBook(book.id)).toBeNull();
    expect(repo.listReleasesByBook(book.id)).toEqual([]);
    expect(repo.listAssetsByBook(book.id)).toEqual([]);

    expect(await Bun.file(assetPath).exists()).toBe(false);
    expect(await Bun.file(coverPath).exists()).toBe(false);

    db.close();
  });

  test("blocks write methods in read-only rpc dispatch", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    repo.ensureSettings();

    const response = await handleRpcMethod("settings.update", {}, { repo, startTime: Date.now() - 1000 }, { readOnly: true });
    expect(response.status).toBe(200);
    const payload = (await response.json()) as any;
    expect(payload.error.code).toBe(-32601);
    db.close();
  });
});
