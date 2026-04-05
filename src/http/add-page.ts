import { hydrateBookFromOpenLibrary } from "../hydration";
import { resolveOpenLibraryCandidate, type OpenLibraryCandidate } from "../openlibrary";
import { BooksRepo } from "../repo";
import { triggerAutoAcquire } from "../service";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, renderAppPage } from "./common";
import { coverMarkup } from "./page-helpers";

export function renderAddPage(
  settings: AppSettings,
  options: {
    query?: string;
    results?: OpenLibraryCandidate[];
    status?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
    apiKey?: string | null;
  } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const query = options.query?.trim() ?? "";
  const results = options.results ?? [];
  const status = options.status?.trim() ?? "";
  const error = options.error?.trim() ?? "";
  const resultMarkup =
    query && results.length === 0 && !error
      ? `<div class="empty">No matches for “${escapeHtml(query)}”.</div>`
      : results.length > 0
        ? `<div class="book-list">${results
            .map((result) => {
              const publishYear = result.publishedAt ? new Date(result.publishedAt).getUTCFullYear() : null;
              return `<article class="book-row">
                ${coverMarkup(result.coverId ? `https://covers.openlibrary.org/b/id/${result.coverId}-L.jpg` : null, result.title)}
                <div class="meta">
                  <h3>${escapeHtml(result.title)}</h3>
                  <p class="muted">${escapeHtml(result.author)}${publishYear ? ` • ${publishYear}` : ""}</p>
                  <form method="post" action="${escapeHtml(addApiKey("/add", apiKey))}">
                    <input type="hidden" name="openLibraryKey" value="${escapeHtml(result.openLibraryKey)}" />
                    <div class="actions">
                      <button type="submit">Add and acquire</button>
                    </div>
                  </form>
                </div>
              </article>`;
            })
            .join("")}</div>`
        : `<div class="empty">Search by title and author to add a book.</div>`;

  const body = `
    <section class="hero">
      <h1>Add a book</h1>
      <p>Search for a book, pick the right match, and Podible will add it to your library and start finding files.</p>
    </section>
    <div class="grid">
      <section class="card span-12">
        <h2>Search catalog</h2>
        <form method="get" action="${escapeHtml(addApiKey("/add", apiKey))}">
          <div class="actions">
            <input type="search" name="q" value="${escapeHtml(query)}" placeholder="Title Author (e.g. Hyperion Dan Simmons)" style="min-width: 320px; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
            <button type="submit">Search</button>
          </div>
        </form>
        ${status ? `<p class="muted" style="margin-top: 10px;">${escapeHtml(status)}</p>` : ""}
        ${error ? `<p style="margin-top: 10px; color: #8b0000;">${escapeHtml(error)}</p>` : ""}
      </section>
      <section class="card span-12">
        <h2>Results</h2>
        ${resultMarkup}
      </section>
    </div>`;
  return renderAppPage("Add", body, settings, options.currentUser ?? null, "", apiKey);
}

export async function createBookFromOpenLibrary(repo: BooksRepo, openLibraryKey: string): Promise<number> {
  const resolved = await resolveOpenLibraryCandidate({ openLibraryKey });
  if (!resolved) {
    throw new Error("Open Library match not found");
  }

  const book = repo.createBook({
    title: resolved.title,
    author: resolved.author,
  });

  repo.updateBookMetadata(book.id, {
    publishedAt: resolved.publishedAt ?? null,
    language: resolved.language ?? null,
    identifiers: resolved.identifiers,
  });

  const hydrated = repo.getBook(book.id);
  if (hydrated) {
    await hydrateBookFromOpenLibrary(repo, hydrated);
  }

  await triggerAutoAcquire(repo, book.id);
  return book.id;
}
