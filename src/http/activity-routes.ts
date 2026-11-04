import { Hono } from "hono";
import { BooksRepo } from "../repo";
import { requireAdminSession, requireAuthenticatedPageSession, type HttpEnv } from "./middleware";

export function createActivityRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedPageSession);
  app.get("/", (c) => c.redirect("/", 303));
  app.post("/refresh", requireAdminSession, (c) => {
    const job = repo.createJob({ type: "full_library_refresh" });
    return c.redirect(`/ops?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`, 303);
  });
  return app;
}
