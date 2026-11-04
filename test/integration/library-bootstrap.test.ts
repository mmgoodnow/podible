import { expect, spyOn, test } from "bun:test";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { createBookSpaData } from "../fixtures/book-spa-data";

test("Library bootstrap returns one authenticated actor and the exact first page without exposing credentials", async () => {
  const f = await createBookSpaData();
  try {
    f.repo.updateBookMetadata(f.book.id, { coverPath: `${f.root}/cover.jpg` });
    const handler = createPodibleFetchHandler(f.repo, Date.now(), null);
    const read = async (method: string, token: string | null, input = {}) => {
      const response = await handler(new Request(`http://localhost/api/trpc/${method}?input=${encodeURIComponent(JSON.stringify(input))}`, {
        headers: token ? { cookie: `podible_session=${token}` } : {},
      }));
      return { response, body: await response.json() as any };
    };
    const jobsBefore = f.db.query("SELECT COUNT(*) AS n FROM jobs").get();
    for (const [token, actor, admin] of [["book-spa-admin", f.admin.id, true], ["book-spa-reader", f.reader.id, false]] as const) {
      const params = { q: "Shelf story", limit: 12 };
      const result = await read("library.bootstrap", token, params);
      expect(result.response.status).toBe(200);
      expect(result.response.headers.get("cache-control")).toBe("no-store");
      expect(result.body.result.data.user.id).toBe(actor);
      expect(result.body.result.data.user.isAdmin).toBe(admin);
      expect(result.body.result.data.q).toBe(params.q);
      const page = result.body.result.data.library;
      expect(page.items).toHaveLength(12);
      expect(page.nextCursor).toBeGreaterThan(0);
      expect(page.items.every((book: { title: string }) => book.title.startsWith("Shelf story"))).toBe(true);
      expect(page).toEqual((await read("library.list", token, params)).body.result.data);
      const encoded = JSON.stringify(result.body);
      for (const privateValue of [token, hashSessionToken(token), "fixture-only-not-a-real-key", "token_hash", "ownerToken"])
        expect(encoded).not.toContain(privateValue);
      expect(Object.keys(result.body.result.data.session).sort()).toEqual(["expiresAt", "kind"]);
    }
    const all = await read("library.bootstrap", "book-spa-reader", { q: "Piranesi", limit: 48 });
    const withCover = all.body.result.data.library.items.find((book: { coverUrl: string | null }) => book.coverUrl);
    expect(withCover).toBeDefined();
    expect(new URL(withCover.coverUrl, "http://localhost").searchParams.get("v")).toBe(withCover.updatedAt);
    const rpc = await handler(new Request("http://localhost/rpc", { method: "POST", headers: {
      cookie: "podible_session=book-spa-reader", "content-type": "application/json",
    }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "library.bootstrap", params: { q: "Piranesi" } }) }));
    const reply = await rpc.json() as any;
    expect(reply.result.user.id).toBe(f.reader.id);
    expect(reply.result.library.items).toHaveLength(1);
    expect(reply.result.library.items[0].id).toBe(f.book.id);
    expect(f.db.query("SELECT COUNT(*) AS n FROM jobs").get()).toEqual(jobsBefore);
  } finally { await f.close(); }
});

test("missing, expired or revoked actors cannot bootstrap or execute Library reads; input remains bounded", async () => {
  const f = await createBookSpaData();
  try {
    const handler = createPodibleFetchHandler(f.repo, Date.now(), null);
    const books = spyOn(f.repo, "listBooks");
    const read = (token: string | null, params = {}) => handler(new Request(
      `http://localhost/api/trpc/library.bootstrap?input=${encodeURIComponent(JSON.stringify(params))}`,
      { headers: token ? { cookie: `podible_session=${token}` } : {} },
    ));
    for (const token of [null, "invalid-session"]) {
      const result = await read(token);
      expect(result.status).toBe(401);
      expect(result.headers.get("cache-control")).toBe("no-store");
      expect(JSON.stringify(await result.json())).not.toContain("Piranesi");
    }
    const session = f.repo.getSessionByTokenHash(hashSessionToken("book-spa-reader"))!;
    f.db.query("UPDATE sessions SET expires_at=? WHERE id=?").run(new Date(Date.now() - 1000).toISOString(), session.id);
    expect((await read("book-spa-reader")).status).toBe(401);
    f.repo.deleteSession(session.id);
    expect((await read("book-spa-reader")).status).toBe(401);
    expect(books).not.toHaveBeenCalled();
    for (const input of [{ limit: 0 }, { limit: 201 }, { cursor: 1 }, { includeSecrets: true }])
      expect((await read("book-spa-admin", input)).status).toBe(400);
    expect(books).not.toHaveBeenCalled();
    books.mockRestore();
  } finally { await f.close(); }
});
