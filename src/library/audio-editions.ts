import { z } from "zod";
import type { BooksRepo } from "../repo";
import { selectPreferredAudioManifestation } from "./asset-selection";

export const audioEditionSchema = z.enum(["dramatized", "narrated"]);
export type AudioEdition = z.infer<typeof audioEditionSchema>;
export const audioEditionLabels: Record<AudioEdition, string> = {
  dramatized: "Dramatized / GraphicAudio",
  narrated: "Narrated audiobook",
};

// Unlike -1 (a selectable alternate), pending parts must not appear in playback options.
export const PENDING_AUDIO_EDITION_SCORE = -2;

const latestKey = (bookId: number, edition: AudioEdition) => `book-audio-edition:${bookId}:${edition}`;
const requestKey = (jobId: number) => `book-audio-edition-request:${jobId}`;
type Part = { releaseId: number; downloadJobId: number; importJobId?: number };
type EditionRequest = {
  bookId: number;
  edition: AudioEdition;
  noMatch?: true;
  candidate?: { id: number; count: number; parts: Record<number, Part>; ready?: true };
};

export const audioEditionStatusSchema = z.object({
  edition: audioEditionSchema,
  status: z.enum(["idle", "queued", "searching", "importing", "ready", "no_match", "failed"]),
  jobId: z.number().int().positive().nullable(),
  manifestationId: z.number().int().positive().nullable(),
});
type Status = z.infer<typeof audioEditionStatusSchema>;

function getRequest(repo: BooksRepo, jobId: number): EditionRequest {
  const request = repo.getJsonState<EditionRequest>(requestKey(jobId));
  if (!request) throw new Error("Missing audiobook edition request");
  return request;
}

export function readAudioEditionStatus(repo: BooksRepo, bookId: number, edition: AudioEdition): Status {
  const jobId = repo.getJsonState<number>(latestKey(bookId, edition));
  const status = (value: Status["status"], manifestationId: number | null = null): Status =>
    ({ edition, status: value, jobId, manifestationId });
  if (jobId === null) return status("idle");
  const request = getRequest(repo, jobId);
  const candidate = request.candidate;
  if (candidate?.ready && repo.getManifestation(candidate.id) &&
      repo.listAssetsByManifestation(candidate.id).length === candidate.count) return status("ready", candidate.id);
  const parent = repo.getJobStatus(jobId);
  if (parent === "queued") return status("queued");
  if (parent === "running") return status(candidate ? "importing" : "searching");
  const downstream = Object.values(candidate?.parts ?? {}).map((part) => part.importJobId ?? part.downloadJobId);
  if ([...repo.getJobs(downstream).values()].some((job) => job.status === "queued" || job.status === "running"))
    return status("importing");
  return status(request.noMatch && parent === "succeeded" ? "no_match" : "failed");
}

export function queueAudioEditionRequest(repo: BooksRepo, bookId: number, edition: AudioEdition): Status {
  return repo.transaction(() => {
    const existing = readAudioEditionStatus(repo, bookId, edition);
    if (existing.status !== "idle" && existing.status !== "failed" && existing.status !== "no_match") return existing;
    const job = repo.createJob({ type: "acquire", bookId, payload: { bookId, media: ["audio"], audioEdition: edition } });
    repo.setJsonState(requestKey(job.id), { bookId, edition } satisfies EditionRequest);
    repo.setJsonState(latestKey(bookId, edition), job.id);
    return { edition, status: "queued", jobId: job.id, manifestationId: null };
  });
}

export function requireAudioEditionRequest(repo: BooksRepo, jobId: number, bookId: number, edition: AudioEdition): void {
  const request = getRequest(repo, jobId);
  if (request.bookId !== bookId || request.edition !== edition || repo.getJsonState(latestKey(bookId, edition)) !== jobId)
    throw new Error("Audiobook edition request no longer matches this acquisition");
}

export function markAudioEditionNoMatch(repo: BooksRepo, jobId: number): void {
  repo.setJsonState(requestKey(jobId), { ...getRequest(repo, jobId), noMatch: true });
}

export function stageAudioEdition(repo: BooksRepo, jobId: number, id: number, count: number): void {
  const request = getRequest(repo, jobId);
  const manifestation = repo.getManifestation(id);
  if (manifestation?.book_id !== request.bookId || manifestation.kind !== "audio")
    throw new Error("Audiobook edition manifestation does not belong to the request");
  if (request.candidate && (request.candidate.id !== id || request.candidate.count !== count))
    throw new Error("Audiobook edition candidate changed");
  if (!request.candidate) repo.setJsonState(requestKey(jobId), { ...request, candidate: { id, count, parts: {} } });
}

export function audioEditionPart(repo: BooksRepo, jobId: number, id: number | null, index: number | null) {
  const request = getRequest(repo, jobId);
  const candidate = request.candidate;
  if (!candidate || candidate.id !== id || index === null || index < 0 || index >= candidate.count)
    throw new Error("Audiobook part does not belong to this edition request");
  return { request, candidate, index, part: candidate.parts[index] };
}

export function recordAudioEditionPart(repo: BooksRepo, jobId: number, id: number | null, sequence: number | null, part: Part): void {
  const state = audioEditionPart(repo, jobId, id, sequence);
  if (state.part && (state.part.releaseId !== part.releaseId || state.part.downloadJobId !== part.downloadJobId))
    throw new Error("Audiobook edition part changed release");
  state.candidate.parts[state.index] = part;
  repo.setJsonState(requestKey(jobId), state.request);
}

export function finalizeAudioEdition(repo: BooksRepo, jobId: number, manifestationId: number): void {
  repo.transaction(() => {
    const request = getRequest(repo, jobId);
    const candidate = request.candidate;
    const manifestation = repo.getManifestation(manifestationId);
    if (!candidate || candidate.id !== manifestationId || manifestation?.book_id !== request.bookId || manifestation.kind !== "audio")
      throw new Error("Audiobook edition changed identity before publication");
    if (candidate.ready || repo.getJsonState(latestKey(request.bookId, request.edition)) !== jobId) return;
    const assets = repo.listAssetsByManifestation(manifestationId);
    if (assets.length < candidate.count) return;
    if (assets.length !== candidate.count || assets.some((asset, index) => {
      const releaseId = asset.source_release_id;
      const files = repo.getAssetFiles(asset.id);
      return asset.sequence_in_manifestation !== index || releaseId === null || releaseId !== candidate.parts[index]?.releaseId ||
        !asset.mime.startsWith("audio/") || files.length === 0 || files.some((file) => file.size <= 0 || file.duration_ms <= 0);
    })) throw new Error("Requested audiobook does not contain playable audio for all ordered parts");
    if (assets.some((asset) => repo.getRelease(asset.source_release_id!)?.status !== "imported")) return;
    const incumbent = selectPreferredAudioManifestation(repo.listManifestationsByBook(request.bookId)
      .filter((row) => row.id !== manifestationId && row.kind === "audio" && row.preferred_score >= 0)
      .map((row) => ({ manifestation: row, containers: repo.listAssetsByManifestation(row.id) })));
    repo.setManifestationPreference(manifestationId, incumbent ? -1 : 0);
    candidate.ready = true;
    repo.setJsonState(requestKey(jobId), request);
  });
}
