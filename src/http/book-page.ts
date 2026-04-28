import { buildManifestationChapters, manifestationDurationMs, selectPreferredAudioManifestation, streamExtensionForManifestation } from "../library/media";
import { getBookTranscriptStatus, hasStoredManifestationTranscriptPayload, selectPreferredEpubAsset } from "../library/chapter-analysis";
import { BooksRepo } from "../repo";
import type { AppSettings, AssetFileRow, AssetRow, ManifestationRow, ReleaseRow, SessionWithUserRow } from "../app-types";

import type { TranscriptRequestResult } from "../library/chapter-analysis";

import { addApiKey, escapeHtml, messageMarkup, renderAppPage } from "./common";
import { coverMarkup, describeBookState, formatBookStatusLine, formatMediaStatus, formatMinutes } from "./page-helpers";

type BookAudioCandidate = {
  manifestation: ManifestationRow;
  containers: AssetRow[];
};

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

export function parseRequestedEditionId(value: string | null | undefined): number | null {
  if (!value) return null;
  const id = Number(value);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

function chooseAudioCandidate(candidates: BookAudioCandidate[], requestedEditionId: number | null): BookAudioCandidate | null {
  if (requestedEditionId !== null) {
    const requested = candidates.find(
      (candidate) => candidate.manifestation.kind === "audio" && candidate.containers.length > 0 && candidate.manifestation.id === requestedEditionId
    );
    if (requested) return requested;
  }
  return selectPreferredAudioManifestation(candidates);
}

function manifestationDisplayName(candidate: BookAudioCandidate, index: number): string {
  if (candidate.manifestation.label?.trim()) return candidate.manifestation.label.trim();
  if (candidate.containers.length > 1) return `Audio edition ${index + 1} (${candidate.containers.length} parts)`;
  return `Audio edition ${index + 1}`;
}

function manifestationMeta(candidate: BookAudioCandidate): string {
  const parts = [
    candidate.manifestation.edition_note?.trim() || null,
    candidate.containers.length > 1 ? `${candidate.containers.length} parts` : "1 part",
    formatMinutes(candidate.manifestation.duration_ms),
  ].filter(Boolean);
  return parts.join(" • ");
}

function renderEditionPicker(bookId: number, candidates: BookAudioCandidate[], selectedManifestationId: number | null, apiKey: string | null): string {
  const audioCandidates = candidates.filter((candidate) => candidate.manifestation.kind === "audio" && candidate.containers.length > 0);
  if (audioCandidates.length <= 1) return "";
  return `
    <form class="edition-picker" method="get" action="${escapeHtml(addApiKey(`/book/${bookId}`, apiKey))}">
      <label for="edition-select"><strong>Audio edition</strong></label>
      <select id="edition-select" name="edition" onchange="this.form.submit()">
        ${audioCandidates
          .map((candidate, index) => {
            const selected = candidate.manifestation.id === selectedManifestationId ? " selected" : "";
            const label = manifestationDisplayName(candidate, index);
            const meta = manifestationMeta(candidate);
            return `<option value="${candidate.manifestation.id}"${selected}>${escapeHtml(meta ? `${label} — ${meta}` : label)}</option>`;
          })
          .join("")}
      </select>
      <noscript><button type="submit">Use edition</button></noscript>
    </form>`;
}

function uniqueReleaseIds(assets: AssetRow[]): number[] {
  return Array.from(new Set(assets.map((asset) => asset.source_release_id).filter((id): id is number => id !== null)));
}

function renderReportIssueButton(
  bookId: number,
  mediaType: "audio" | "ebook",
  label: string,
  releaseId: number,
  manifestationId: number | null
): string {
  const manifestationAttr = manifestationId === null ? "" : ` data-manifestation-id="${manifestationId}"`;
  return `<button type="button" data-report-import-issue data-book-id="${bookId}" data-media-type="${mediaType}" data-release-id="${releaseId}"${manifestationAttr}>${escapeHtml(label)}</button>`;
}

function renderReportIssueSection(bookId: number, audioChoice: BookAudioCandidate | null, ebook: AssetRow | null): string {
  const buttons: string[] = [];
  const audioReleaseIds = audioChoice ? uniqueReleaseIds(audioChoice.containers) : [];
  if (audioReleaseIds.length === 1) {
    buttons.push(renderReportIssueButton(bookId, "audio", "Report wrong audio", audioReleaseIds[0]!, audioChoice?.manifestation.id ?? null));
  } else if (audioReleaseIds.length > 1) {
    buttons.push(
      ...audioReleaseIds.map((releaseId, index) =>
        renderReportIssueButton(bookId, "audio", `Report wrong audio part ${index + 1}`, releaseId, audioChoice?.manifestation.id ?? null)
      )
    );
  }
  if (ebook?.source_release_id) {
    buttons.push(renderReportIssueButton(bookId, "ebook", "Report wrong ebook", ebook.source_release_id, ebook.manifestation_id));
  }
  if (buttons.length === 0) return "";
  return `
      <section class="card span-12" data-report-import-panel data-book-id="${bookId}">
        <h2>Something wrong?</h2>
        <p class="muted">Report a bad import to preserve the current edition, try an alternate import with the agent, and escalate to an agent reacquire if import cannot be recovered.</p>
        <div class="actions" style="margin-top: 12px;">${buttons.join("")}</div>
        <p class="muted" data-report-import-status style="margin-top: 8px;"></p>
      </section>`;
}

function renderAdminManifestationSection(
  repo: BooksRepo,
  manifestations: ManifestationRow[],
  releases: ReleaseRow[],
  selectedManifestationId: number | null
): string {
  const releasesById = new Map(releases.map((release) => [release.id, release]));
  const rows = manifestations
    .map((manifestation) => {
      const containers = repo.listAssetsByManifestation(manifestation.id);
      const isSelected = manifestation.id === selectedManifestationId;
      const open = isSelected ? " open" : "";
      const label = manifestation.label?.trim() || `Manifestation ${manifestation.id}`;
      const meta = [
        manifestation.kind,
        manifestation.edition_note?.trim() || null,
        formatMinutes(manifestation.duration_ms),
        `${manifestation.total_size} bytes`,
        `score ${manifestation.preferred_score}`,
      ].filter(Boolean);
      const containerMarkup =
        containers.length > 0
          ? containers
              .map((asset) => {
                const release = asset.source_release_id ? releasesById.get(asset.source_release_id) : null;
                const files = repo.getAssetFiles(asset.id);
                return `<li>
                  <div><strong>Asset ${asset.id}</strong> • ${escapeHtml(asset.kind)} • ${escapeHtml(asset.mime)} • seq ${asset.sequence_in_manifestation}${release ? ` • release ${release.id}: ${escapeHtml(release.title)}` : ""}</div>
                  ${asset.import_note ? `<div class="muted">Import note: ${escapeHtml(asset.import_note)}</div>` : ""}
                  ${
                    files.length > 0
                      ? `<ul>${files.map((file) => renderAdminAssetFile(file)).join("")}</ul>`
                      : `<div class="muted">No files.</div>`
                  }
                </li>`;
              })
              .join("")
          : `<li class="muted">No containers.</li>`;
      return `<details${open} class="manifestation-details">
        <summary><strong>${escapeHtml(label)}</strong> <span class="muted">#${manifestation.id} • ${escapeHtml(meta.join(" • "))}${isSelected ? " • selected" : ""}</span></summary>
        ${manifestation.selection_note ? `<p class="muted">Selection note: ${escapeHtml(manifestation.selection_note)}</p>` : ""}
        <ul>${containerMarkup}</ul>
      </details>`;
    })
    .join("");
  return `
      <section class="card span-12">
        <h2>Admin: Manifestations</h2>
        <p class="muted">Diagnostic view of editions, containers, source releases, and imported files.</p>
        ${rows || `<div class="empty">No manifestations.</div>`}
      </section>`;
}

function renderAdminAssetFile(file: AssetFileRow): string {
  const source = file.source_path && file.source_path !== file.path ? ` • source ${file.source_path}` : "";
  return `<li><code>${escapeHtml(file.path)}</code><span class="muted"> • ${file.size} bytes • ${formatMinutes(file.duration_ms)}${escapeHtml(source)}</span></li>`;
}

function renderTranscriptRuntimeScript(bookId: number, manifestationId: number | null, transcriptHref: string): string {
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
            body: JSON.stringify({
              jsonrpc: "2.0",
              id: 1,
              method,
              params: {
                bookId: ${bookId},
                ${manifestationId === null ? "" : `manifestationId: ${manifestationId},`}
              },
            }),
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

function renderReportIssueRuntimeScript(bookId: number): string {
  return `
    <script>
      (() => {
        const panel = document.querySelector('[data-report-import-panel][data-book-id="${bookId}"]');
        if (!panel) return;
        const status = panel.querySelector('[data-report-import-status]');

        function setStatus(message) {
          if (status) status.textContent = message;
        }

        async function rpc(method, params) {
          const url = new URL("/rpc", window.location.origin);
          url.search = window.location.search;
          const response = await fetch(url.pathname + url.search, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
          });
          const payload = await response.json();
          if (payload.error) throw new Error(payload.error.message || "Request failed");
          return payload.result;
        }

        panel.addEventListener("click", async (event) => {
          const target = event.target instanceof Element ? event.target : null;
          const button = target?.closest("[data-report-import-issue]");
          if (!(button instanceof HTMLButtonElement)) return;
          const mediaType = button.getAttribute("data-media-type");
          const releaseId = Number(button.getAttribute("data-release-id"));
          const manifestationId = Number(button.getAttribute("data-manifestation-id"));
          if (mediaType !== "audio" && mediaType !== "ebook") return;
          if (!Number.isSafeInteger(releaseId) || releaseId <= 0) return;
          const params = { bookId: ${bookId}, mediaType, releaseId };
          if (Number.isSafeInteger(manifestationId) && manifestationId > 0) {
            params.manifestationId = manifestationId;
          }
          if (!window.confirm("Report this " + mediaType + " as the wrong file? Podible will keep the current files and queue agent recovery.")) return;
          button.disabled = true;
          setStatus("Reporting wrong " + mediaType + "...");
          try {
            const result = await rpc("library.reportImportIssue", params);
            setStatus("Queued agent import review job " + result.jobId + ". If that cannot recover it, Podible will queue agent reacquire.");
          } catch (error) {
            console.error("library.reportImportIssue failed:", error);
            button.disabled = false;
            setStatus(error.message || "Unable to report issue.");
          }
        });
      })();
    </script>
  `;
}

function renderCoverRuntimeScript(bookId: number): string {
  return `
    <script>
      (() => {
        const panel = document.querySelector('[data-cover-panel][data-book-id="${bookId}"]');
        if (!panel) return;
        const button = panel.querySelector('[data-cover-load]');
        const status = panel.querySelector('[data-cover-status]');
        const results = panel.querySelector('[data-cover-results]');

        function setStatus(message) {
          if (status) status.textContent = message;
        }

        function escapeText(value) {
          return String(value ?? "").replace(/[&<>"']/g, (char) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          })[char]);
        }

        async function rpc(method, params) {
          const url = new URL("/rpc", window.location.origin);
          url.search = window.location.search;
          const response = await fetch(url.pathname + url.search, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
          });
          const payload = await response.json();
          if (payload.error) throw new Error(payload.error.message || "Request failed");
          return payload.result;
        }

        function renderCandidates(candidates) {
          if (!results) return;
          if (!candidates.length) {
            results.innerHTML = '<div class="empty">No alternate Open Library covers found.</div>';
            return;
          }
          results.innerHTML = candidates.map((cover) => {
            const details = [cover.source, cover.publishDate, cover.publisher, cover.language].filter(Boolean).join(" • ");
            return '<button type="button" class="cover-candidate" data-cover-id="' + cover.coverId + '">' +
              '<img src="' + escapeText(cover.coverUrl) + '" alt="Cover candidate ' + cover.coverId + '" loading="lazy" />' +
              '<span><strong>Use cover ' + cover.coverId + '</strong></span>' +
              (details ? '<span class="muted">' + escapeText(details) + '</span>' : '') +
              '</button>';
          }).join("");
        }

        if (button) {
          button.addEventListener("click", async () => {
            button.disabled = true;
            setStatus("Loading alternate covers...");
            try {
              const result = await rpc("openlibrary.covers", { bookId: ${bookId}, limit: 24 });
              renderCandidates(result.results || []);
              setStatus((result.results || []).length ? "Choose a cover to apply." : "No alternate covers found.");
            } catch (error) {
              console.error("openlibrary.covers failed:", error);
              setStatus(error.message || "Unable to load covers.");
            } finally {
              button.disabled = false;
            }
          });
        }

        if (results) {
          results.addEventListener("click", async (event) => {
            const target = event.target instanceof Element ? event.target : null;
            const candidate = target?.closest("[data-cover-id]");
            if (!candidate) return;
            const coverId = Number(candidate.getAttribute("data-cover-id"));
            if (!Number.isSafeInteger(coverId) || coverId <= 0) return;
            candidate.disabled = true;
            setStatus("Applying cover...");
            try {
              await rpc("openlibrary.setCover", { bookId: ${bookId}, coverId });
              setStatus("Cover updated. Refreshing...");
              window.location.reload();
            } catch (error) {
              console.error("openlibrary.setCover failed:", error);
              candidate.disabled = false;
              setStatus(error.message || "Unable to update cover.");
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
  flash: {
    notice?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
    apiKey?: string | null;
    selectedEditionId?: number | null;
  } = {}
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
  const audioChoice = chooseAudioCandidate(audioCandidates, flash.selectedEditionId ?? null);
  const audio = audioChoice?.containers[0] ?? null;
  const selectedManifestationId = audioChoice?.manifestation.id ?? null;
  const audioContainers = audioChoice
    ? audioChoice.containers.map((container) => ({ asset: container, files: repo.getAssetFiles(container.id) }))
    : [];
  const ebook = selectPreferredEpubAsset(assets);
  const chapters = audioContainers.length > 0 ? await buildManifestationChapters(repo, audioContainers) : null;
  const transcriptUrl =
    audioChoice && hasStoredManifestationTranscriptPayload(repo, audioContainers)
      ? addApiKey(`/transcripts/m/${audioChoice.manifestation.id}.json`, apiKey)
      : null;
  const streamUrl = audioChoice ? addApiKey(`/stream/m/${audioChoice.manifestation.id}.${streamExtensionForManifestation(audioContainers)}`, apiKey) : null;
  const chaptersUrl = audioChoice ? addApiKey(`/chapters/m/${audioChoice.manifestation.id}.json`, apiKey) : null;
  const ebookUrl = ebook ? addApiKey(`/ebook/${ebook.id}`, apiKey) : null;
  const audioDurationMs = audioChoice ? manifestationDurationMs(audioChoice.manifestation, audioContainers) : null;
  const apiKeyConfigured = Boolean(settings.agents.apiKey.trim());
  const transcriptStatus =
    audio && selectedManifestationId !== null
      ? await getBookTranscriptStatus(repo, bookId, { apiKeyConfigured, manifestationId: selectedManifestationId })
      : null;
  const allReleases = repo.listReleasesByBook(bookId);
  const releases = allReleases.slice(0, 8);
  const isAdmin = Boolean(apiKey) || (flash.currentUser?.is_admin ?? 0) === 1;
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
          ${renderEditionPicker(book.id, audioCandidates, selectedManifestationId, apiKey)}
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
          <div><strong>Audio duration:</strong> ${formatMinutes(audioDurationMs)}</div>
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
      ${renderReportIssueSection(book.id, audioChoice, ebook)}
      <section class="card span-12" data-cover-panel data-book-id="${book.id}">
        <h2>Artwork</h2>
        <p class="muted">Try alternate Open Library covers for this book.</p>
        <div class="actions" style="margin-top: 12px;">
          <button type="button" data-cover-load>Load alternate covers</button>
        </div>
        <p class="muted" data-cover-status style="margin-top: 8px;"></p>
        <div class="cover-candidates" data-cover-results></div>
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
      ${isAdmin ? renderAdminManifestationSection(repo, manifestations, allReleases, selectedManifestationId) : ""}
    </div>
    <style>
      .cover-candidates {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
        gap: 12px;
        margin-top: 12px;
      }
      .cover-candidates button.cover-candidate {
        display: grid;
        gap: 6px;
        align-items: start;
        justify-content: stretch;
        width: 100%;
        min-height: 0;
        padding: 10px;
        text-align: left;
        overflow: hidden;
      }
      .cover-candidate img {
        width: 100%;
        aspect-ratio: 2 / 3;
        object-fit: cover;
        border: 1px solid var(--line-soft);
        border-radius: 8px;
        background: var(--surface);
      }
      .cover-candidate span {
        min-width: 0;
        overflow-wrap: anywhere;
      }
      .manifestation-details {
        border-top: 1px solid var(--line-soft);
        padding: 10px 0;
      }
      .manifestation-details:first-of-type {
        border-top: 0;
      }
      .manifestation-details summary {
        cursor: pointer;
      }
      .manifestation-details ul {
        margin: 8px 0 0 18px;
        padding: 0;
      }
      .manifestation-details li {
        margin: 6px 0;
      }
    </style>
    ${audio ? renderTranscriptRuntimeScript(book.id, selectedManifestationId, transcriptUrl ?? "") : ""}
    ${renderReportIssueRuntimeScript(book.id)}
    ${renderCoverRuntimeScript(book.id)}`;
  return renderAppPage(
    book.title,
    body,
    settings,
    flash.currentUser ?? null,
    `<a href="${escapeHtml(addApiKey(`/rpc/library/get?bookId=${book.id}`, apiKey))}">Raw JSON</a>`,
    apiKey
  );
}
