import { Hono } from "hono";

import { searchOpenLibrary } from "../library/openlibrary";
import { recordUserJourneyAction } from "../metrics";
import { BooksRepo } from "../repo";
import { triggerAutoAcquire } from "../library/service";
import { renderActivityPage } from "./activity-page";
import { createBookFromOpenLibrary, renderAddPage } from "./add-page";
import { parseRequestedEditionId, renderBookPage } from "./book-page";
import { renderLandingPage } from "./landing-page";
import { renderLibraryPage } from "./library-page";
import { renderLoginPage } from "./login-page";
import { getCurrentSession, requireAdminSession, requireAuthenticatedPageSession, type HttpEnv } from "./middleware";
import { parseMediaSelection } from "./page-helpers";
import { formString, parseId } from "./route-helpers";

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
      recordUserJourneyAction("search_openlibrary", "ok");
      return renderAddPage(settings, {
        query,
        results,
        status: `Found ${results.length} result${results.length === 1 ? "" : "s"} for “${query}”.`,
        currentUser: currentSession,
        apiKey: null,
      });
    } catch (error) {
      recordUserJourneyAction("search_openlibrary", "error");
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
    const body = await c.req.parseBody();
    const openLibraryKey = formString(body, "openLibraryKey").trim();
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
      recordUserJourneyAction("add_book", "ok");
      return c.redirect(`/book/${bookId}`, 303);
    } catch (error) {
      recordUserJourneyAction("add_book", "error");
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
      selectedEditionId: parseRequestedEditionId(c.req.query("edition")),
    })
  );

  app.post("/:bookId/acquire", async (c) => {
    const bookId = parseId(c.req.param("bookId"));
    const body = await c.req.parseBody();
    const media = parseMediaSelection(formString(body, "media"));
    const book = repo.getBookRow(bookId);
    if (!book) {
      recordUserJourneyAction("queue_acquire", "error");
      return c.notFound();
    }
    const jobId = await triggerAutoAcquire(repo, bookId, media);
    recordUserJourneyAction("queue_acquire", "ok");
    const notice = `Queued ${media.join(" + ")} acquire for ${book.title} (job ${jobId}).`;
    return c.redirect(`/book/${bookId}?notice=${encodeURIComponent(notice)}`, 303);
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

  app.post("/refresh", requireAdminSession, (c) => {
    const job = repo.createJob({ type: "full_library_refresh" });
    recordUserJourneyAction("refresh_library", "ok");
    return c.redirect(`/activity?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`, 303);
  });

  return app;
}
