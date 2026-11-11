import { Hono } from "hono";
import { createHash, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { configDir } from "../config";
import type { BooksRepo } from "../repo";
import { transferMetricsPrometheus } from "../media/transfer-metrics-prometheus";
import { indexerTrafficPrometheus } from "../media/indexer-traffic";
import { acquisitionLatencyPrometheus } from "../media/acquisition-latency";
import { getCurrentSession, type HttpEnv } from "./middleware";

async function matchesScrapeToken(authorization: string | undefined, digestPath: string): Promise<boolean> {
  const token = /^Bearer ([a-f0-9]{64})$/i.exec(authorization ?? "")?.[1];
  if (!token) return false;
  const digest = await readFile(digestPath, "utf8").catch(() => "");
  const expected = digest.trim();
  return /^[a-f0-9]{64}$/i.test(expected) &&
    timingSafeEqual(createHash("sha256").update(token).digest(), Buffer.from(expected, "hex"));
}

export function createMetricsRoutes(repo: BooksRepo, digestPath = join(configDir, "metrics-token.sha256")) {
  const app = new Hono<HttpEnv>();
  app.get("/", async c => {
    const session = getCurrentSession(c);
    if (session?.is_admin !== 1 && !await matchesScrapeToken(c.req.header("Authorization"), digestPath)) {
      if (session) return c.text("Forbidden", 403);
      return c.text("Unauthorized", 401, { "WWW-Authenticate": 'Bearer realm="podible"' });
    }
    return c.text(transferMetricsPrometheus(repo.transferMetrics.scrapeSnapshot(), repo.transferMetrics.publishedCounts()) +
      indexerTrafficPrometheus(repo) + acquisitionLatencyPrometheus(repo), 200, {
      "Content-Type": "text/plain; version=0.0.4; charset=utf-8", "Cache-Control": "no-store",
    });
  });
  return app;
}
