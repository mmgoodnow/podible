import { manifestationDurationMs, preferredAudioManifestationsForBooks } from "../library/media";
import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { formatBookStatusLine, formatMinutes } from "./page-helpers";

export function renderActivityPage(
  repo: BooksRepo,
  settings: AppSettings,
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Response {
  const inProgress = repo.listInProgressBooks();
  const recentBooks = repo.listAllBooks().filter((book) => book.status === "imported").slice(0, 8);
  const needsAttention = repo.listAllBooks().filter((book) => book.status === "error").slice(0, 8);
  const playableByBookId = new Map(preferredAudioManifestationsForBooks(repo).map((entry) => [entry.book.id, entry]));
  const apiKey = flash.apiKey ?? null;
  const canRefreshLibrary = Boolean(apiKey) || (flash.currentUser?.is_admin ?? 0) === 1;
  const body = `
    <section class="hero">
      <h1>Activity</h1>
      <p>What Podible is working on right now, what just landed, and anything that needs attention.</p>
      ${
        canRefreshLibrary
          ? `<div class="actions" style="margin-top: 14px;">
              <form method="post" action="${escapeHtml(addApiKey("/activity/refresh", apiKey))}">
                <button type="submit">Refresh library</button>
              </form>
            </div>`
          : ""
      }
      ${messageMarkup(flash.notice, flash.error)}
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>Books in progress</h2>
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
        <h2>Recently ready</h2>
        ${
          recentBooks.length > 0
            ? `<div class="section-list">${recentBooks
                .map(
                  (book) => {
                    const playable = playableByBookId.get(book.id);
                    const audioDurationMs = playable ? manifestationDurationMs(playable.manifestation, playable.containers) : null;
                    return `<div>
                    <strong><a href="${escapeHtml(addApiKey(`/book/${book.id}`, apiKey))}">${escapeHtml(book.title)}</a></strong>
                    <div class="muted">${escapeHtml(book.author)} • ready to play${audioDurationMs ? ` • ${formatMinutes(audioDurationMs)}` : ""}</div>
                  </div>`;
                  }
                )
                .join("")}</div>`
            : `<div class="empty">No recently finished books yet.</div>`
        }
      </section>
      <section class="card span-12">
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
    </div>`;
  return renderAppPage("Activity", body, settings, flash.currentUser ?? null, "", apiKey);
}
