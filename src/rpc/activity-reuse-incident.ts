import { z } from "zod";
import type { AppSettings, JobRow } from "../app-types";
import { crossSeedOffer } from "../media/cross-seed";
import { crossSeedCommandSchema } from "../media/cross-seed-state";
import type { BooksRepo } from "../repo";

const payload = z.object({ operationId: z.uuid() }).strict();

export function describeReuseIncident(repo: BooksRepo, settings: AppSettings, job: JobRow, label: (value: string) => string) {
  if (job.type !== "torrent_reuse" || job.status !== "failed") return null;
  try {
    const { operationId } = payload.parse(JSON.parse(job.payload_json ?? "{}"));
    const receipt = repo.crossSeed.receipt(operationId);
    if (!receipt || receipt.jobId !== job.id || receipt.command.operationId !== operationId) return null;
    const command = crossSeedCommandSchema.parse(receipt.command);
    const { scope } = command;
    let work: string | undefined, href: string | null = null;
    if (scope.media === "tv") {
      const target = repo.tv.target(scope.targetId);
      if (target) {
        work = repo.activityReads.tvTitle(target.entry_id) ?? undefined;
        if (repo.tv.availableEpisodeCount(target.entry_id) > 0) href = `/tv/library/${target.entry_id}`;
      }
    } else if (scope.media === "movie") {
      const target = repo.movies.target(scope.targetId);
      const movie = target ? repo.movies.movie(target.movie_id) : null;
      work = movie?.title;
      if (movie) href = `/movies/${movie.tmdbId}`;
    } else {
      const book = repo.getBook(scope.targetId);
      work = book?.title;
      if (book) href = `/book/${scope.targetId}`;
    }
    const title = work ? `${label(work)}: cross-seeding` : "Cross-seeding";
    let failure: ReturnType<BooksRepo["activityReuseFailure"]> = null;
    try { failure = repo.activityReuseFailure(job.id); } catch { /* Missing evidence cannot establish the failing step. */ }
    if (!failure) return { title, href, detail: null };
    if (failure.stage === "inventory") return { title, href, code: "reuse-local-files",
      detail: "Podible could not finish checking local files for reuse. This only affects seeding on an additional tracker, not the original download. Retry to check the files again. If it fails again, check the library storage." };

    const offer = crossSeedOffer(repo, command);
    const clean = label(offer.provider).trim();
    const provider = /^[\p{L}\p{N}][\p{L}\p{N} _().&+'!-]{0,119}$/u.test(clean) ? clean : null;
    let viaProwlarr = false;
    try { viaProwlarr = new URL(offer.url).origin === new URL(settings.prowlarr.url).origin; } catch { /* No known proxy. */ }
    const status = failure.httpStatus;
    const source = provider ?? "the configured indexer";
    const check = viaProwlarr ? `Check ${source} in Prowlarr and run its Test action before retrying.`
      : `Check ${source}'s status and test its configured indexer connection before retrying.`;
    const origin = status === 429 ? " The response does not identify whether the indexer or a proxy blocked the request." : "";
    const detail = status === null ? `Podible could not fetch torrent metadata from ${source}.`
      : `Fetching torrent metadata from ${source} returned HTTP ${status}.`;
    return { title, href, code: status === 429 ? "provider-cooldown" : "reuse-metainfo",
      detail: `${detail}${origin} ${check}` };
  } catch { return null; }
}
