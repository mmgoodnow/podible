import { createHash } from "node:crypto";
import path from "node:path";
import { promises as fs } from "node:fs";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { AppSettings, MediaType, ManifestationRow } from "../app-types";
import type { ModelProvider } from "@openai/agents-core";
import { selectPreferredAudioManifestation, selectPreferredDownloadableEbookAsset } from "./asset-selection";
import { assessBookQuality, bookQualityAssessmentSchema, type BookQualityAssessment } from "./quality";
import { assessBookUpgrade } from "./quality-agent";

export const bookManualSelectionKey = (bookId: number, media: MediaType) => `book-quality-manual:${bookId}:${media}`;
export const bookUpgradeKey = (id: number) => `book-quality-upgrade:${id}`;
const bookRepairKey = (id: number) => `book-quality-repair:${id}`;
const repairSchema = z.object({
  bookId: z.number().int().positive(), media: z.enum(["audio", "ebook"]),
  candidateId: z.number().int().positive(), parts: z.number().int().positive(),
  reviewJobId: z.number().int().positive(), manualStamp: z.string(),
  status: z.enum(["staged", "active", "retained"]), reason: z.string().nullable(),
});

/** A server-created user report, not priorFailure or an upgrade retry, grants repair authority. */
export function validateBookRepair(repo: BooksRepo, bookId: number, media: MediaType, reviewJobId: number) {
  const job = repo.getJob(reviewJobId);
  const payload = job?.payload_json ? JSON.parse(job.payload_json) : null;
  const release = job?.release_id ? repo.getRelease(job.release_id) : null;
  if (!job || job.type !== "import" || payload?.userReportedIssue !== true || !release ||
    release.book_id !== bookId || release.media_type !== media || (job.book_id !== null && job.book_id !== bookId))
    throw new Error("Invalid explicit book repair provenance");
  return release;
}

export function stageBookRepair(repo: BooksRepo, input: {
  bookId: number; media: MediaType; candidateId: number; parts: number; reviewJobId: number;
}) {
  repo.transaction(() => {
    validateBookRepair(repo, input.bookId, input.media, input.reviewJobId);
    const key = bookRepairKey(input.candidateId), existing = repo.getJsonState(key);
    if (existing) {
      const prior = repairSchema.parse(existing);
      if (prior.bookId !== input.bookId || prior.media !== input.media || prior.parts !== input.parts ||
        prior.reviewJobId !== input.reviewJobId) throw new Error("Book repair intent changed");
      return;
    }
    const candidate = repo.getManifestation(input.candidateId);
    if (!candidate || candidate.book_id !== input.bookId || candidate.kind !== input.media)
      throw new Error("Book repair candidate changed identity");
    repo.setManifestationPreference(candidate.id, -1);
    repo.setJsonState(key, repairSchema.parse({ ...input, status: "staged", reason: null,
      manualStamp: JSON.stringify(repo.getJsonState(bookManualSelectionKey(input.bookId, input.media))) }));
  });
}
const baselineSchema = z.object({ id: z.number().int().positive(), fingerprint: z.string(), facts: z.unknown() });
const intentSchema = z.object({
  version: z.literal(1), bookId: z.number().int().positive(), media: z.enum(["audio", "ebook"]),
  candidateId: z.number().int().positive(), baseline: baselineSchema, parts: z.number().int().positive(),
  preflight: bookQualityAssessmentSchema,
  status: z.enum(["staged", "active", "retained"]), reason: z.string().nullable(),
  assessment: bookQualityAssessmentSchema.nullable(),
});
export type BookUpgradeBaseline = z.infer<typeof baselineSchema>;

function assetsFor(repo: BooksRepo, manifestation: ManifestationRow) {
  return repo.listAssetsByManifestation(manifestation.id).map(asset => ({
    ...asset, files: repo.getAssetFiles(asset.id),
    release: asset.source_release_id === null ? null : repo.getRelease(asset.source_release_id),
  }));
}

function factsFor(repo: BooksRepo, manifestation: ManifestationRow) {
  const assets = assetsFor(repo, manifestation);
  return {
    label: manifestation.label, editionNote: manifestation.edition_note,
    selectionNote: manifestation.selection_note, language: manifestation.language,
    assets: assets.map(asset => ({
      format: asset.mime, durationMs: asset.duration_ms, sizeBytes: asset.total_size,
      releaseTitle: asset.release?.title ?? null,
      files: asset.files.map(file => ({ name: path.basename(file.identity_path || file.source_path || file.path), sizeBytes: file.size, durationMs: file.duration_ms })),
    })),
  };
}

function fingerprint(repo: BooksRepo, manifestation: ManifestationRow) {
  return createHash("sha256").update(JSON.stringify({
    id: manifestation.id, preference: manifestation.preferred_score,
    facts: factsFor(repo, manifestation),
    files: assetsFor(repo, manifestation).flatMap(asset => asset.files.map(file => [file.id, file.path, file.size])),
  })).digest("hex");
}

export function currentBookQualityBaseline(repo: BooksRepo, bookId: number, media: MediaType): BookUpgradeBaseline | null {
  const manifestations = repo.listManifestationsByBook(bookId).filter(row => row.kind === media &&
    assetsFor(repo, row).some(asset => !asset.release || asset.release.status !== "failed"));
  const eligible = new Set(manifestations.map(row => row.id));
  const id = media === "audio"
    ? selectPreferredAudioManifestation(manifestations.map(manifestation => ({ manifestation, containers: repo.listAssetsByManifestation(manifestation.id) })))?.manifestation.id
    : selectPreferredDownloadableEbookAsset(repo.listAssetsByBook(bookId).filter(asset =>
      asset.manifestation_id !== null && eligible.has(asset.manifestation_id)))?.manifestation_id;
  const manifestation = manifestations.find(row => row.id === id);
  return manifestation ? { id: manifestation.id, fingerprint: fingerprint(repo, manifestation), facts: factsFor(repo, manifestation) } : null;
}

export function bookAutomaticUpgradeAllowed(repo: BooksRepo, bookId: number, media: MediaType): boolean {
  return repo.getJsonState(bookManualSelectionKey(bookId, media)) === null;
}

export function stageBookUpgrade(repo: BooksRepo, input: {
  bookId: number; media: MediaType; candidateId: number; baseline: BookUpgradeBaseline;
  parts: number; preflight: BookQualityAssessment;
}) {
  return repo.transaction(() => {
    const existing = repo.getJsonState(bookUpgradeKey(input.candidateId));
    if (existing) {
      const intent = intentSchema.parse(existing);
      if (intent.bookId !== input.bookId || intent.media !== input.media || intent.parts !== input.parts ||
        intent.baseline.fingerprint !== input.baseline.fingerprint) throw new Error("Book upgrade intent changed");
      return;
    }
    const baseline = currentBookQualityBaseline(repo, input.bookId, input.media);
    const candidate = repo.getManifestation(input.candidateId);
    if (!bookAutomaticUpgradeAllowed(repo, input.bookId, input.media) || !baseline ||
      baseline.fingerprint !== input.baseline.fingerprint || !candidate || candidate.book_id !== input.bookId ||
      candidate.kind !== input.media || candidate.id === baseline.id || !assessBookQuality(input.preflight).upgrade)
      throw new Error("Book upgrade baseline, manual choice or compatibility changed");
    repo.setManifestationPreference(candidate.id, -1);
    repo.setJsonState(bookUpgradeKey(candidate.id), intentSchema.parse({ ...input, version: 1,
      status: "staged", reason: null, assessment: null }));
  });
}

/** Import success is not promotion: wait for every ordered part and recheck actual files. */
export async function finalizeBookUpgrade(repo: BooksRepo, settings: AppSettings, manifestationId: number,
  modelProvider?: ModelProvider, assertActive = () => {}) {
  const repair = repo.getJsonState(bookRepairKey(manifestationId));
  if (repair) return finalizeBookRepair(repo, repairSchema.parse(repair), assertActive);
  const raw = repo.getJsonState(bookUpgradeKey(manifestationId));
  if (!raw) return "ordinary";
  const intent = intentSchema.parse(raw);
  if (intent.status !== "staged") return intent.status;
  const candidate = repo.getManifestation(manifestationId);
  if (!candidate || candidate.book_id !== intent.bookId || candidate.kind !== intent.media)
    throw new Error("Book upgrade candidate changed identity");
  const assets = assetsFor(repo, candidate);
  if (assets.length < intent.parts) return "waiting";
  const retain = (reason: string, assessment: BookQualityAssessment | null = null) => {
    assertActive();
    repo.setJsonState(bookUpgradeKey(manifestationId), { ...intent, status: "retained", reason, assessment });
    return "retained";
  };
  const baseline = currentBookQualityBaseline(repo, intent.bookId, intent.media);
  if (!baseline || baseline.fingerprint !== intent.baseline.fingerprint ||
    !bookAutomaticUpgradeAllowed(repo, intent.bookId, intent.media)) return retain("Existing or manual selection changed");
  if (assets.length !== intent.parts || assets.some((asset, i) => asset.sequence_in_manifestation !== i ||
    !asset.files.length || !asset.release || asset.release.book_id !== intent.bookId ||
    asset.release.media_type !== intent.media || asset.release.status === "failed")) return retain("Incomplete or changed ordered parts");
  const physicalProof = async () => {
    const proof = [];
    try {
      for (const asset of assets) for (const file of asset.files) {
        const stat = await fs.lstat(file.path);
        if (!stat.isFile() || stat.size <= 0 || stat.size !== file.size || await fs.realpath(file.path) !== file.path)
          return null;
        await fs.access(file.path, fs.constants.R_OK);
        proof.push([stat.dev, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs]);
      }
    } catch (error) {
      if (["ENOENT", "ENOTDIR", "EACCES", "ELOOP"].includes((error as NodeJS.ErrnoException).code ?? "")) return null;
      throw error;
    }
    return JSON.stringify(proof);
  };
  const proof = await physicalProof();
  if (proof === null) return retain("Imported file identity or size is not verified");
  const book = repo.getBookRow(intent.bookId);
  if (!book) return retain("Book no longer exists");
  const candidateFingerprint = fingerprint(repo, candidate);
  const assessment = await assessBookUpgrade(settings, {
    title: book.title, author: book.author, description: book.description?.slice(0, 8000) ?? null,
    media: intent.media, editionPreference: settings.agents.editionPreference, phase: "imported",
    current: baseline.facts, candidate: factsFor(repo, candidate),
  }, modelProvider);
  if (!assessment || !assessBookQuality(assessment).upgrade) return retain(assessment?.reason ?? "No quality reviewer available", assessment);
  if (await physicalProof() !== proof) return retain("Imported bytes changed while reviewing upgrade", assessment);
  return repo.transaction(() => {
    assertActive();
    const latest = currentBookQualityBaseline(repo, intent.bookId, intent.media);
    const staged = repo.getManifestation(candidate.id);
    if (!latest || latest.fingerprint !== intent.baseline.fingerprint || !staged ||
      fingerprint(repo, staged) !== candidateFingerprint || !bookAutomaticUpgradeAllowed(repo, intent.bookId, intent.media))
      return retain("Selection changed while reviewing imported upgrade", assessment);
    const score = Math.max(0, ...repo.listManifestationsByBook(intent.bookId).filter(row => row.kind === intent.media).map(row => row.preferred_score)) + 1;
    repo.setManifestationPreference(candidate.id, score);
    repo.setJsonState(bookUpgradeKey(candidate.id), { ...intent, status: "active", reason: assessment.reason, assessment });
    return "active";
  });
}

async function finalizeBookRepair(repo: BooksRepo, intent: z.infer<typeof repairSchema>, assertActive: () => void) {
  if (intent.status !== "staged") return intent.status;
  validateBookRepair(repo, intent.bookId, intent.media, intent.reviewJobId);
  const candidate = repo.getManifestation(intent.candidateId);
  if (!candidate || candidate.book_id !== intent.bookId || candidate.kind !== intent.media)
    throw new Error("Book repair candidate changed identity");
  const assets = assetsFor(repo, candidate);
  if (assets.length < intent.parts) return "waiting";
  let valid = assets.length === intent.parts && assets.every((asset, index) =>
    asset.sequence_in_manifestation === index && asset.files.length > 0 && asset.release &&
    asset.release.book_id === intent.bookId && asset.release.media_type === intent.media && asset.release.status !== "failed");
  try {
    for (const asset of assets) for (const file of asset.files) {
      const stat = await fs.lstat(file.path);
      valid &&= stat.isFile() && stat.size === file.size && stat.size > 0 && await fs.realpath(file.path) === file.path;
      await fs.access(file.path, fs.constants.R_OK);
    }
  } catch (error) {
    if (!["ENOENT", "ENOTDIR", "EACCES", "ELOOP"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
    valid = false;
  }
  return repo.transaction(() => {
    assertActive();
    valid &&= intent.manualStamp === JSON.stringify(repo.getJsonState(bookManualSelectionKey(intent.bookId, intent.media)));
    if (!valid) {
      repo.setJsonState(bookRepairKey(intent.candidateId), { ...intent, status: "retained", reason: "Files, ordered parts or manual choice changed" });
      return "retained";
    }
    const score = Math.max(0, ...repo.listManifestationsByBook(intent.bookId).filter(row => row.kind === intent.media).map(row => row.preferred_score)) + 1;
    repo.setManifestationPreference(candidate.id, score);
    repo.setJsonState(bookRepairKey(intent.candidateId), { ...intent, status: "active", reason: "Verified replacement of a user-reported invalid edition; original bytes retained" });
    return "active";
  });
}
