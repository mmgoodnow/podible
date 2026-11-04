import { Hono } from "hono";

import { BooksRepo } from "../repo";
import { requireAdminSession, type HttpEnv } from "./middleware";
import { formString } from "./route-helpers";
import type { BuildInfo } from "../build-info";

export function createAdminRoutes(repo: BooksRepo, _startTime: number, _buildInfo: BuildInfo | null): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.use("*", async (c, next) => {
    c.header("Cache-Control", "no-store");
    await next();
  });

  for (const [legacy, canonical] of [
    ["/", "/settings"], ["/settings", "/settings"], ["/users", "/users"], ["/plex", "/settings"],
    ["/ops", "/ops"], ["/jobs", "/ops"], ["/downloads", "/ops"], ["/content", "/ops"],
  ]) {
    app.get(legacy, requireAdminSession, (c) => c.redirect(`${canonical}${new URL(c.req.url).search}`, 303));
  }

  app.post("/plex/select", requireAdminSession, async (c) => {
    const settings = repo.getSettings();
    if (settings.auth.mode !== "plex") {
      return c.text("Forbidden", 403);
    }
    const body = await c.req.parseBody();
    const machineId = formString(body, "machineId").trim();
    if (!machineId) {
      return c.redirect("/settings?plex_error=Missing%20machine%20id", 303);
    }
    repo.updateSettings({
      ...settings,
      auth: {
        ...settings.auth,
        plex: {
          ...settings.auth.plex,
          machineId,
        },
      },
    });
    return c.redirect("/settings?plex_notice=Selected%20server.", 303);
  });

  app.post("/refresh", requireAdminSession, (c) => {
    const job = repo.createJob({ type: "full_library_refresh" });
    return c.redirect(`/ops?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`, 303);
  });

  return app;
}
