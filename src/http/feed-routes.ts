import { Hono } from "hono";

import { buildJsonFeed, buildRssFeed } from "../feed";
import { BooksRepo } from "../repo";
import { requireAuthenticatedRequest, type HttpEnv } from "./middleware";

export function createFeedRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.use("/feed.xml", requireAuthenticatedRequest);
  app.use("/feed.json", requireAuthenticatedRequest);

  app.get("/feed.xml", (c) => {
    const settings = repo.getSettings();
    return buildRssFeed(c.req.raw, repo, settings.feed.title, settings.feed.author);
  });

  app.get("/feed.json", (c) => {
    const settings = repo.getSettings();
    return buildJsonFeed(c.req.raw, repo, settings.feed.title, settings.feed.author);
  });

  return app;
}
