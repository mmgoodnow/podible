import { Hono } from "hono";
import type { BooksRepo } from "../repo";
import { transferMetricsPrometheus } from "../media/transfer-metrics-prometheus";
import { getCurrentSession, requireAuthenticatedRequest, type HttpEnv } from "./middleware";

export function createMetricsRoutes(repo: BooksRepo) {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/", c => {
    if (getCurrentSession(c)?.is_admin !== 1) return c.text("Forbidden", 403);
    return c.text(transferMetricsPrometheus(repo.transferMetrics.status()), 200, {
      "Content-Type": "text/plain; version=0.0.4; charset=utf-8", "Cache-Control": "no-store",
    });
  });
  return app;
}
