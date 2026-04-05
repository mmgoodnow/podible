import { searchOpenLibrary } from "../openlibrary";
import { BooksRepo } from "../repo";
import { triggerAutoAcquire } from "../service";
import { renderActivityPage } from "./activity-page";
import { createBookFromOpenLibrary, renderAddPage } from "./add-page";
import { renderBookPage } from "./book-page";
import { renderLandingPage } from "./landing-page";
import { renderLibraryPage } from "./library-page";
import { renderLoginPage } from "./login-page";
import { parseMediaSelection } from "./page-helpers";
import { parseId, redirect } from "./route-helpers";
import type { AppSettings, SessionWithUserRow } from "../types";

export async function handleUserRoute(input: {
  repo: BooksRepo;
  request: Request;
  settings: AppSettings;
  currentSession: SessionWithUserRow | null;
  pathname: string;
  method: string;
  isAuthenticatedRequest: boolean;
}): Promise<Response | null> {
  const { repo, request, settings, currentSession, pathname, method, isAuthenticatedRequest } = input;
  const url = new URL(request.url);

  if (pathname === "/" && method === "GET") {
    return isAuthenticatedRequest
      ? renderLandingPage(repo, settings, currentSession, null)
      : renderLoginPage(settings, { currentUser: currentSession });
  }

  if (pathname === "/library" && method === "GET") {
    return renderLibraryPage(repo, settings, {
      query: url.searchParams.get("q"),
      currentUser: currentSession,
      apiKey: null,
    });
  }

  if (pathname === "/add" && method === "GET") {
    const query = (url.searchParams.get("q") ?? "").trim();
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
  }

  if (pathname === "/add" && method === "POST") {
    const form = new URLSearchParams(await request.text());
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
  }

  if (pathname.startsWith("/book/") && method === "GET") {
    const bookId = parseId(pathname.split("/")[2] ?? "");
    return renderBookPage(repo, settings, bookId, {
      notice: url.searchParams.get("notice"),
      error: url.searchParams.get("error"),
      currentUser: currentSession,
      apiKey: null,
    });
  }

  if (pathname.startsWith("/book/") && pathname.endsWith("/acquire") && method === "POST") {
    const bookId = parseId(pathname.split("/")[2] ?? "");
    const form = new URLSearchParams(await request.text());
    const media = parseMediaSelection(form.get("media"));
    const book = repo.getBookRow(bookId);
    if (!book) {
      return new Response("Not found", { status: 404 });
    }
    const jobId = await triggerAutoAcquire(repo, bookId, media);
    const notice = `Queued ${media.join(" + ")} acquire for ${book.title} (job ${jobId}).`;
    return redirect(`/book/${bookId}?notice=${encodeURIComponent(notice)}`);
  }

  if (pathname === "/activity" && method === "GET") {
    return renderActivityPage(repo, settings, {
      notice: url.searchParams.get("notice"),
      error: url.searchParams.get("error"),
      currentUser: currentSession,
      apiKey: null,
    });
  }

  if (pathname === "/activity/refresh" && method === "POST") {
    const job = repo.createJob({ type: "full_library_refresh" });
    return redirect(`/activity?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`);
  }

  return null;
}
