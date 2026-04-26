import { preferredAudioManifestationsForBooks, streamExtension } from "../library/media";
import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, renderAppPage } from "./common";
import { coverMarkup, formatBookStatusLine, formatMinutes, formatOverallStatus, truncateText } from "./page-helpers";

export function renderLandingPage(
  repo: BooksRepo,
  settings: AppSettings,
  currentUser: SessionWithUserRow | null = null,
  apiKey: string | null = null
): Response {
  const recentBooks = repo.listAllBooks().slice(0, 6);
  const featured = preferredAudioManifestationsForBooks(repo).slice(0, 6);
  const inProgress = repo.listInProgressBooks().slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 6);
  const body = `
    <section class="hero">
      <h1>Podible</h1>
      <p>Your shelf for audiobooks and eBooks.</p>
      <div class="stats">
        <span class="pill">${featured.length} ready to play</span>
        <span class="pill">${inProgress.length} in progress</span>
        <span class="pill">${needsAttention.length} need attention</span>
      </div>
      <div class="actions" style="margin-top: 14px;">
        <a href="${escapeHtml(addApiKey("/library", apiKey))}">Browse library</a>
        <a href="${escapeHtml(addApiKey("/add", apiKey))}">Add a book</a>
      </div>
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>Ready now</h2>
        ${
          featured.length > 0
            ? `<div class="book-list">${featured
                .map(({ book, containers }) => {
                  const asset = containers[0]!.asset;
                  const detailUrl = addApiKey(`/book/${book.id}`, apiKey);
                  const streamUrl = addApiKey(`/stream/${asset.id}.${streamExtension(asset)}`, apiKey);
                  return `<article class="book-row">
                    ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title)}
                    <div class="meta">
                      <h3><a href="${escapeHtml(detailUrl)}">${escapeHtml(book.title)}</a></h3>
                      <p class="muted">${escapeHtml(book.author)}</p>
                      <p class="muted">${formatMinutes(book.durationMs)} • ${escapeHtml(formatBookStatusLine(book))}</p>
                      <p class="muted">${escapeHtml(truncateText((book.description || `${book.title} by ${book.author}`).replace(/\s+/g, " "), 160))}</p>
                      <div class="actions">
                        <a href="${escapeHtml(detailUrl)}">Details</a>
                        <a href="${escapeHtml(streamUrl)}">Play</a>
                      </div>
                    </div>
                  </article>`;
                })
                .join("")}</div>`
            : `<div class="empty">No playable books yet.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Still working</h2>
        ${
          inProgress.length > 0
            ? `<div class="section-list">${inProgress
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No active work right now.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Needs attention</h2>
        ${
          needsAttention.length > 0
            ? `<div class="section-list">${needsAttention
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatBookStatusLine(book))}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">Nothing needs attention right now.</div>`
        }
      </section>
      <section class="card span-6">
        <h2>Recently added</h2>
        ${
          recentBooks.length > 0
            ? `<div class="section-list">${recentBooks
                .map(
                  (book) => `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ${escapeHtml(formatOverallStatus(book.status))}${book.durationMs ? ` • ${formatMinutes(book.durationMs)}` : ""}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No books yet.</div>`
        }
      </section>
    </div>`;
  return renderAppPage("Podible", body, settings, currentUser, "", apiKey);
}
