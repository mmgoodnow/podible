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
