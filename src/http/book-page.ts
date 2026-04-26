import { buildChapters, selectPreferredAudioManifestation, streamExtension } from "../library/media";
import { getBookTranscriptStatus, hasStoredTranscriptPayload, selectPreferredEpubAsset } from "../library/chapter-analysis";
import { BooksRepo } from "../repo";
import type { AppSettings, SessionWithUserRow } from "../app-types";

import type { TranscriptRequestResult } from "../library/chapter-analysis";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { coverMarkup, describeBookState, formatBookStatusLine, formatMediaStatus, formatMinutes } from "./page-helpers";

function describeTranscriptStatus(status: TranscriptRequestResult | null): string {
  if (!status) return "No audio";
  switch (status.status) {
    case "current":
      return "Available";
    case "stale":
      return "Out of date — regenerate to refresh";
    case "pending":
      return "Queued";
    case "running":
      return "Generating…";
    case "failed":
      return "Failed";
    case "missing_audio":
      return "No audio";
    case "missing_config":
      return "Unavailable (API key not configured)";
    default:
      return "Not ready yet";
  }
}

function canRequestTranscription(status: TranscriptRequestResult | null): boolean {
  if (!status) return false;
  return status.status === "stale" || status.status === "failed" || status.status === "missing_config";
}

function renderTranscriptRuntimeScript(bookId: number, transcriptHref: string): string {
  return `
    <script>
      (() => {
        const panel = document.querySelector('[data-transcript-panel][data-book-id="${bookId}"]');
        if (!panel) return;
        const label = panel.querySelector('[data-transcript-label]');
        const button = panel.querySelector('[data-transcript-request]');
        const link = panel.querySelector('[data-transcript-link]');
        const transcriptHref = ${JSON.stringify(transcriptHref)};
        let polling = false;

        function describe(status) {
          switch (status) {
            case "current": return "Available";
            case "stale": return "Out of date — regenerate to refresh";
            case "pending": return "Queued";
            case "running": return "Generating…";
            case "failed": return "Failed";
            case "missing_audio": return "No audio";
            case "missing_config": return "Unavailable (API key not configured)";
            default: return "Not ready yet";
          }
        }

        function canRequest(status) {
          return status === "stale" || status === "failed";
        }

        function apply(result) {
          if (label) label.textContent = describe(result.status);
          if (link) {
            if (result.status === "current" && transcriptHref) {
              link.setAttribute("href", transcriptHref);
              link.hidden = false;
            } else {
              link.hidden = true;
            }
          }
          if (button) {
            const showButton = canRequest(result.status);
            button.hidden = !showButton;
            button.disabled = result.status === "pending" || result.status === "running";
            button.textContent = result.status === "failed" ? "Retry transcription" : "Generate transcript";
          }
          if ((result.status === "pending" || result.status === "running") && !polling) {
            polling = true;
            setTimeout(poll, 3000);
          }
        }

        async function rpc(method) {
          const url = new URL("/rpc", window.location.origin);
          url.search = window.location.search;
          const response = await fetch(url.pathname + url.search, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params: { bookId: ${bookId} } }),
          });
          const payload = await response.json();
          if (payload.error) throw new Error(payload.error.message || "Request failed");
          return payload.result;
        }

        async function poll() {
          polling = false;
          try {
            const result = await rpc("library.transcriptStatus");
            apply(result);
          } catch (e) {
            console.error("transcriptStatus poll failed:", e);
          }
        }

        if (button) {
          button.addEventListener("click", async () => {
            button.disabled = true;
            try {
              const result = await rpc("library.requestTranscription");
              apply(result);
            } catch (e) {
              console.error("requestTranscription failed:", e);
              button.disabled = false;
            }
          });
        }
      })();
    </script>
  `;
}

export async function renderBookPage(
  repo: BooksRepo,
  settings: AppSettings,
  bookId: number,
  flash: { notice?: string | null; error?: string | null; currentUser?: SessionWithUserRow | null; apiKey?: string | null } = {}
): Promise<Response> {
  const apiKey = flash.apiKey ?? null;
  const book = repo.getBook(bookId);
  const bookRow = repo.getBookRow(bookId);
  if (!book || !bookRow) {
    return new Response("Not found", { status: 404 });
  }
  const assets = repo.listAssetsByBook(bookId);
  const manifestations = repo.listManifestationsByBook(bookId);
  const audioCandidates = manifestations.map((manifestation) => ({
    manifestation,
    containers: repo.listAssetsByManifestation(manifestation.id),
  }));
  const audioChoice = selectPreferredAudioManifestation(audioCandidates);
  // Step 2: every manifestation has exactly one container, so the chosen
  // container's asset is the same row that selectPreferredAudioAsset would
  // have returned. Step 3 starts using all containers.
  const audio = audioChoice?.containers[0] ?? null;
  const ebook = selectPreferredEpubAsset(assets);
  const audioFiles = audio ? repo.getAssetFiles(audio.id) : [];
  const chapters = audio ? await buildChapters(repo, audio, audioFiles) : null;
  const transcriptUrl = audio && hasStoredTranscriptPayload(repo, audio.id) ? addApiKey(`/transcripts/${audio.id}.json`, apiKey) : null;
  const streamUrl = audio ? addApiKey(`/stream/${audio.id}.${streamExtension(audio)}`, apiKey) : null;
  const chaptersUrl = audio ? addApiKey(`/chapters/${audio.id}.json`, apiKey) : null;
  const ebookUrl = ebook ? addApiKey(`/ebook/${ebook.id}`, apiKey) : null;
  const apiKeyConfigured = Boolean(settings.agents.apiKey.trim());
  const transcriptStatus = audio ? await getBookTranscriptStatus(repo, bookId, { apiKeyConfigured }) : null;
  const releases = repo.listReleasesByBook(bookId).slice(0, 8);
  const stateSummary = describeBookState(book);
  const body = `
    <section class="hero">
      <div class="detail-grid">
        ${coverMarkup(book.coverUrl ? addApiKey(book.coverUrl, apiKey) : null, book.title, true)}
        <div>
          <h1>${escapeHtml(book.title)}</h1>
          <p class="muted">${escapeHtml(book.author)}</p>
          <div class="stats">
            <span class="pill">${escapeHtml(stateSummary)}</span>
            <span class="pill">${escapeHtml(formatMediaStatus("Audio", book.audioStatus))}</span>
            <span class="pill">${escapeHtml(formatMediaStatus("eBook", book.ebookStatus))}</span>
          </div>
          <div class="actions" style="margin-top: 12px;">
            ${streamUrl ? `<a class="button-link button-link-primary" href="${escapeHtml(streamUrl)}">Play audio</a>` : ""}
            ${ebookUrl ? `<a class="button-link" href="${escapeHtml(ebookUrl)}">Download EPUB/PDF</a>` : ""}
          </div>
          ${messageMarkup(flash.notice, flash.error)}
          <p style="margin-top: 12px;">${escapeHtml(book.description || `${book.title} by ${book.author}`)}</p>
        </div>
      </div>
    </section>
    <div class="grid">
      <section class="card span-6">
        <h2>What you can do now</h2>
        <div class="section-list">
          <div><strong>Audio:</strong> ${audio ? "Ready to play" : "Still looking"}</div>
          <div><strong>eBook:</strong> ${ebook ? "Ready to download" : "Still looking"}</div>
          <div><strong>Duration:</strong> ${formatMinutes(book.durationMs)}</div>
        </div>
        <div class="actions" style="margin-top: 12px;">
          ${streamUrl ? `<a class="button-link button-link-primary" href="${escapeHtml(streamUrl)}">Play audio</a>` : ""}
          ${ebookUrl ? `<a class="button-link" href="${escapeHtml(ebookUrl)}">Download EPUB/PDF</a>` : ""}
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="audio" />
            <button type="submit">Find audio</button>
          </form>
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="ebook" />
            <button type="submit">Find ebook</button>
          </form>
          <form method="post" action="${escapeHtml(addApiKey(`/book/${book.id}/acquire`, apiKey))}">
            <input type="hidden" name="media" value="both" />
            <button type="submit">Find both</button>
          </form>
        </div>
      </section>
      <section class="card span-6" data-transcript-panel data-book-id="${book.id}">
        <h2>Available now</h2>
        <div class="section-list">
          <div><strong>Audio:</strong> ${audio ? "Available" : "Not ready yet"}</div>
          <div><strong>eBook:</strong> ${ebook ? "Available" : "Not ready yet"}</div>
          <div><strong>Transcript:</strong> <span data-transcript-label>${escapeHtml(describeTranscriptStatus(transcriptStatus))}</span></div>
          <div><strong>Chapters:</strong> ${chaptersUrl ? "Available" : "Not ready yet"}</div>
        </div>
        <div class="actions" style="margin-top: 12px;">
          ${chaptersUrl ? `<a class="button-link" href="${escapeHtml(chaptersUrl)}">Chapters JSON</a>` : ""}
          <a class="button-link" data-transcript-link ${transcriptUrl && transcriptStatus?.status === "current" ? `href="${escapeHtml(transcriptUrl)}"` : `href="#" hidden`}>Transcript JSON</a>
          <button type="button" data-transcript-request ${canRequestTranscription(transcriptStatus) ? "" : "hidden"} ${transcriptStatus?.status === "missing_config" ? "disabled" : ""}>${transcriptStatus?.status === "failed" ? "Retry transcription" : "Generate transcript"}</button>
        </div>
        ${transcriptStatus?.status === "missing_config" ? `<p class="muted" style="margin-top:8px;">Transcription requires an OpenAI API key in Settings.</p>` : ""}
        ${transcriptStatus?.status === "failed" && transcriptStatus.error ? `<p class="muted" style="margin-top:8px;">Last error: ${escapeHtml(transcriptStatus.error)}</p>` : ""}
      </section>
      <section class="card span-12">
        <h2>Chapter preview</h2>
        ${
          chapters?.chapters?.length
            ? `<div class="section-list">${chapters.chapters
                .slice(0, 12)
                .map((chapter) => `<div class="chapter-row"><span>${escapeHtml(chapter.title)}</span><span class="muted">${chapter.startTime.toFixed(0)}s</span></div>`)
                .join("")}${chapters.chapters.length > 12 ? `<div class="muted">+ ${chapters.chapters.length - 12} more</div>` : ""}</div>`
            : `<div class="empty">No chapter data yet.</div>`
        }
      </section>
      <section class="card span-12">
        <h2>Release history</h2>
        ${
          releases.length > 0
            ? `<div class="section-list">${releases
                .map(
                  (release) => `<div>
                    <strong>${escapeHtml(release.title)}</strong>
                    <div class="muted">${escapeHtml(release.media_type)} • ${escapeHtml(release.provider)} • ${escapeHtml(release.status)}${release.error ? ` • ${escapeHtml(release.error)}` : ""}</div>
                  </div>`
                )
                .join("")}</div>`
            : `<div class="empty">No release activity yet.</div>`
        }
      </section>
    </div>
    ${audio ? renderTranscriptRuntimeScript(book.id, addApiKey(`/transcripts/${audio.id}.json`, apiKey)) : ""}`;
  return renderAppPage(
    book.title,
    body,
    settings,
    flash.currentUser ?? null,
    `<a href="${escapeHtml(addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey))}">Raw JSON</a>`,
    apiKey
  );
}
