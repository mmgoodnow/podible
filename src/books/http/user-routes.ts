import { Hono } from "hono";

import { searchOpenLibrary } from "../openlibrary";
import { BooksRepo } from "../repo";
import { triggerAutoAcquire } from "../service";
import { renderActivityPage } from "./activity-page";
import { createBookFromOpenLibrary, renderAddPage } from "./add-page";
import { renderBookPage } from "./book-page";
import { renderLandingPage } from "./landing-page";
import { renderLibraryPage } from "./library-page";
import { renderLoginPage } from "./login-page";
import { getCurrentSession, requireAuthenticatedPageSession, type HttpEnv } from "./middleware";
import { parseMediaSelection } from "./page-helpers";
import { parseId, redirect } from "./route-helpers";

export function createHomeRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.get("/", (c) => {
    const settings = repo.getSettings();
    const currentSession = getCurrentSession(c);
    return currentSession
      ? renderLandingPage(repo, settings, currentSession, null)
      : renderLoginPage(settings, { currentUser: currentSession });
  });
  return app;
}

export function createLibraryRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedPageSession);
  app.get("/", (c) =>
    renderLibraryPage(repo, repo.getSettings(), {
      query: c.req.query("q"),
      currentUser: getCurrentSession(c),
      apiKey: null,
    })
  );
  return app;
}

export function createAddRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedPageSession);
  app.get("/", async (c) => {
    const settings = repo.getSettings();
    const currentSession = getCurrentSession(c);
    const query = (c.req.query("q") ?? "").trim();
    if (!query) {
      return renderAddPage(settings, { currentUser: currentSession, apiKey: null });
    }
    try {
      const results = await searchOpenLibrary(query, 10);
      return renderAddPage(settings, {
        query,
        results,
        status: `Found ${results.length} result${results.length === 1 ? "" : "s"} for “${query}”.`,
        currentUser: currentSession,
        apiKey: null,
      });
    } catch (error) {
      return renderAddPage(settings, {
        query,
        error: `Search failed: ${(error as Error).message}`,
        currentUser: currentSession,
        apiKey: null,
      });
    }
  });

  app.post("/", async (c) => {
    const settings = repo.getSettings();
    const currentSession = getCurrentSession(c);
    const form = new URLSearchParams(await c.req.raw.text());
    const openLibraryKey = (form.get("openLibraryKey") ?? "").trim();
    if (!openLibraryKey) {
      const addResponse = renderAddPage(settings, {
        error: "openLibraryKey is required.",
        currentUser: currentSession,
        apiKey: null,
      });
      return new Response(await addResponse.text(), { status: 400, headers: addResponse.headers });
    }
    try {
      const bookId = await createBookFromOpenLibrary(repo, openLibraryKey);
      return redirect(`/book/${bookId}`);
    } catch (error) {
      const addResponse = renderAddPage(settings, {
        error: `Add failed: ${(error as Error).message}`,
        currentUser: currentSession,
        apiKey: null,
      });
      return new Response(await addResponse.text(), { status: 400, headers: addResponse.headers });
    }
  });

  return app;
}

export function createBookRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedPageSession);
  app.get("/:bookId", (c) =>
    renderBookPage(repo, repo.getSettings(), parseId(c.req.param("bookId")), {
      notice: c.req.query("notice"),
      error: c.req.query("error"),
      currentUser: getCurrentSession(c),
      apiKey: null,
    })
  );

  app.post("/:bookId/acquire", async (c) => {
    const bookId = parseId(c.req.param("bookId"));
    const form = new URLSearchParams(await c.req.raw.text());
    const media = parseMediaSelection(form.get("media"));
    const book = repo.getBookRow(bookId);
    if (!book) {
      return new Response("Not found", { status: 404 });
    }
    const jobId = await triggerAutoAcquire(repo, bookId, media);
    const notice = `Queued ${media.join(" + ")} acquire for ${book.title} (job ${jobId}).`;
    return redirect(`/book/${bookId}?notice=${encodeURIComponent(notice)}`);
  });

  return app;
}

export function createActivityRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedPageSession);
  app.get("/", (c) =>
    renderActivityPage(repo, repo.getSettings(), {
      notice: c.req.query("notice"),
      error: c.req.query("error"),
      currentUser: getCurrentSession(c),
      apiKey: null,
    })
  );

  app.post("/refresh", () => {
    const job = repo.createJob({ type: "full_library_refresh" });
    return redirect(`/activity?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`);
  });

  return app;
}
