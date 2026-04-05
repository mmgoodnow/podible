import { selectPreferredAudioAsset, streamExtension } from "../library/media";
import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, renderAppPage } from "./common";
import { coverMarkup } from "./page-helpers";

export function renderLibraryPage(
  repo: BooksRepo,
  settings: AppSettings,
  options: { query?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const query = options.query?.trim() ?? "";
  const books = repo.listBooks(200, undefined, query || undefined).items;
  const body = `
    <section class="hero">
      <h1>Library</h1>
      <p>${books.length} book${books.length === 1 ? "" : "s"}${query ? ` matching “${escapeHtml(query)}”` : ""}.</p>
      <form method="get" action="${escapeHtml(addApiKey("/library", apiKey))}">
        <div class="actions" style="margin-top: 14px;">
          <input type="search" name="q" value="${escapeHtml(query)}" placeholder="Search by title or author" style="min-width: 280px; padding: 8px 10px; border: 1px solid var(--line); border-radius: 10px;" />
          <button type="submit">Search</button>
          ${query ? `<a href="${escapeHtml(addApiKey("/library", apiKey))}">Clear</a>` : ""}
        </div>
      </form>
    </section>
    <section class="card span-12">
      ${
        books.length > 0
          ? `<div class="book-list">${books
              .map((book) => {
                const asset = selectPreferredAudioAsset(repo.listAssetsByBook(book.id));
                const detailUrl = addApiKey(`/book/${book.id}`, apiKey);
                const streamUrl = asset ? addApiKey(`/stream/${asset.id}.${streamExtension(asset)}`, apiKey) : null;
                return `<article class="book-row">
                  ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title)}
                  <div class="meta">
                    <h3><a href="${escapeHtml(detailUrl)}">${escapeHtml(book.title)}</a></h3>
                    <p class="muted">${escapeHtml(book.author)}</p>
                    <div class="stats">
                      <span class="pill">${escapeHtml(book.status)}</span>
                      <span class="pill">audio ${escapeHtml(book.audioStatus)}</span>
                      <span class="pill">ebook ${escapeHtml(book.ebookStatus)}</span>
                    </div>
                    <div class="actions">
                      <a href="${escapeHtml(detailUrl)}">Details</a>
                      ${streamUrl ? `<a href="${escapeHtml(streamUrl)}">Play</a>` : ""}
                    </div>
                  </div>
                </article>`;
              })
              .join("")}</div>`
          : `<div class="empty">No books found.</div>`
      }
    </section>`;
  return renderAppPage("Library", body, settings, options.currentUser ?? null, "", apiKey);
}
