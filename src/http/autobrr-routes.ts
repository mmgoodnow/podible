import { createHash, timingSafeEqual } from "node:crypto";
import { Hono } from "hono";
import { parseAutobrrRelease, type AutobrrConfig } from "../media/autobrr";
import { enqueueReleaseMatches } from "../media/enqueue-release-matches";
import type { BooksRepo } from "../repo";
import type { HttpEnv } from "./middleware";

export function createAutobrrRoutes(repo: BooksRepo, config: AutobrrConfig) {
  const app = new Hono<HttpEnv>();
  const hash = (value: string) => createHash("sha256").update(value).digest();
  const expected = hash(config.authorization);
  app.post("/", async c => {
    if (!timingSafeEqual(hash(c.req.header("Authorization") ?? ""), expected)) return c.text("Unauthorized", 401);
    const maxBytes = 32 * 1024;
    if (Number(c.req.header("Content-Length")) > maxBytes) return c.text("Payload too large", 413);
    const reader = c.req.raw.body?.getReader();
    if (!reader) return c.text("Invalid JSON", 400);
    let body: unknown;
    try {
      const chunks: Uint8Array[] = [];
      let bytes = 0;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (bytes > maxBytes) return c.text("Payload too large", 413);
        chunks.push(value);
      }
      body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    } catch {
      return c.text("Invalid JSON", 400);
    } finally {
      await reader.cancel().catch(() => {});
    }
    const event = parseAutobrrRelease(config, body);
    if (!event) return c.body(null, 204);
    const { feedId, result } = event;
    repo.transaction(() => {
      const now = new Date().toISOString();
      repo.releaseInbox.ensureFeed(feedId, result.provider, result.mediaType, now);
      const ids = repo.releaseInbox.ingest(feedId, [result], now);
      enqueueReleaseMatches(repo, feedId, result.mediaType, ids);
    });
    return c.body(null, 204);
  });
  return app;
}
