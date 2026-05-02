import { Hono } from "hono";

import { renderPrometheusMetrics } from "../metrics";
import type { HttpEnv } from "./middleware";

export function createMetricsRoutes(startTime: number): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.get("/", () =>
    new Response(renderPrometheusMetrics(startTime), {
      headers: {
        "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
        "Cache-Control": "no-store",
      },
    })
  );

  return app;
}
