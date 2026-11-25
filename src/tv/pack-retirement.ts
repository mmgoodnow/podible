import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { finalizedCanonicalObject, readCanonicalDownload, resolveDownloadMembers } from "../media/canonical-download";
import { idleTorrentUseRevision } from "../media/torrent-retirement-admission";
import { retirementDigest } from "../media/torrent-retirement-evidence";
import { completeDaemonRecord, type RetirementDaemon, type RetirementDaemonSnapshot } from "../media/torrent-retirement-transmission";
import type { PackRetirementLocator, RetirementBinding } from "../media/torrent-retirement-types";
import { boundedStewardshipFetch, stewardshipBinding, verifyStewardshipDirectory } from "../media/torrent-stewardship";
import { MINIMUM_SEED_SECONDS } from "../media/torrent-stewardship-types";
import { assertMovieStorage } from "../movies/storage";
import { currentEffects } from "../runtime/execution-effects";
import { tvPackRetirementSource, type TvPackRetirementNomination } from "./pack-retirement-nominations";
import { PlexTvLibrary } from "./plex";
import { renderTvFileEntry, tvdbAiredNumber, tvRoots, type TvLibrary } from "./publication";
import { tvSeasonPromotionSchema } from "./season-promotion";
import { tvSourceSchema, type TvSource } from "./source-reuse";
import type { TvVersion } from "./types";

type Source = { binding: RetirementBinding; source: TvSource; version: TvVersion; proof: TvPackRetirementNomination["pack"] };
export type PackRetirementLibrary = Pick<TvLibrary, "verify">;
const same = (a: unknown, b: unknown) => retirementDigest(a) === retirementDigest(b);
const ids = (values: readonly number[]) => [...values].sort((a, b) => a - b);

/** Synchronous domain proof, also re-run by the writer fence at the dispatch boundary. */
export function inspectTvPackRetirement(repo: BooksRepo, locator: PackRetirementLocator, binding: RetirementBinding) {
  const tv = repo.tv, { targetId, packVersionId } = locator;
  const nomination = tv.packRetirementNomination(targetId, packVersionId);
  const settings = repo.readSettings(), target = tv.target(targetId);
  if (!nomination || !settings || !target || binding.media !== "tv" || target.revision < nomination.publishedRevision ||
      tv.publicationAcquisitionId(targetId) !== null) throw new Error("Pack nomination or current publication is unavailable");
  const { pack, superseded, season, episodeIds, requestId } = nomination;
  const candidates = superseded.filter(item => item.clientId === binding.clientId && item.infoHash === binding.hash);
  const request = tv.requestById(requestId), requested = new Set(tv.requestedEpisodeIds(requestId));
  const targetIntent = repo.retention.get({ namespace: "tv-target", id: String(targetId) });
  const seasonIntent = repo.retention.get({ namespace: "tv-season", id: `${targetId}:tvdb:aired:${season}` });
  if (candidates.length !== 1 || pack.clientId !== binding.clientId || pack.infoHash === binding.hash || episodeIds.length < 2 ||
      request?.target_id !== targetId || request.withdrawn_at || episodeIds.some(id => !requested.has(id)) ||
      targetIntent?.desiredPresence === "evicted" || seasonIntent?.desiredPresence === "evicted")
    throw new Error("Pack demand, retention intent or displaced source changed");
  const candidate = candidates[0]!;
  const entry = tv.entry(target.entry_id);
  const roster = entry?.episodes.filter(episode => episode.numbering.some(n => n.scheme === "tvdb:aired" && n.season === season));
  if (!entry?.tvdbId || entry.episodes.length > 5000 || !roster || !same(ids(roster.map(row => row.id)), ids(episodeIds)) ||
      roster.some(episode => episode.identity.stability !== "provider" || !/^tvdb:[1-9]\d*$/.test(episode.key) ||
        episode.numbering.filter(n => n.scheme === "tvdb:aired").length !== 1 || !tvdbAiredNumber(episode).episode) ||
      new Set(roster.map(row => tvdbAiredNumber(row).episode)).size !== roster.length)
    throw new Error("Exact current TV season roster is unavailable");
  const active = tv.activeEpisodes(targetId), selected = active.filter(row => row.version_id === packVersionId);
  if (!same(ids(selected.map(row => row.episode_id)), ids(episodeIds)) || active.some(row => row.version_id === candidate.versionId))
    throw new Error("The pack is not the exact full current season selection");

  function source(proof: TvPackRetirementNomination["pack"]): Source {
    const version = tv.version(proof.versionId), acquisition = tv.acquisition(proof.acquisitionId);
    const row = repo.reuseTorrent({ clientId: proof.clientId, nativeId: proof.infoHash });
    const current = row && stewardshipBinding(repo, "screen", { ...row }, proof.clientId);
    if (!version || version.targetId !== targetId || version.entryId !== entry!.id || acquisition?.target_id !== targetId ||
        !acquisition.finished_at || !row || !current || current.media !== "tv" ||
        !tv.downloadBindings([acquisition.id]).some(item => item.torrentId === proof.torrentId) ||
        !same(tvPackRetirementSource(version, row), proof) ||
        repo.crossSeed.registrationOwner({ clientId: proof.clientId, nativeId: proof.infoHash }))
      throw new Error("Exact finished TV acquisition source is no longer owned");
    const saved = tvSourceSchema.parse((version.facts as { source: unknown }).source);
    if (saved.manifest.files.length > 256) throw new Error("TV source inventory exceeded the retirement budget");
    return { binding: current, source: saved, version, proof };
  }
  const replacement = source(pack), displaced = source(candidate);
  const acquisition = tv.acquisition(pack.acquisitionId)!;
  const job = acquisition.job_id === null ? null : repo.getJob(acquisition.job_id);
  const decision = z.object({ seasonPromotion: tvSeasonPromotionSchema }).parse(JSON.parse(acquisition.decision_json ?? "null")).seasonPromotion;
  const anchorUse = idleTorrentUseRevision(repo, { clientId: pack.clientId, infoHash: pack.infoHash });
  if (acquisition.reason !== "upgrade" || acquisition.request_id !== requestId || decision.season !== season ||
      decision.revision + 1 !== nomination.publishedRevision || decision.fingerprint !== nomination.promotionFingerprint ||
      !same(ids(decision.episodeIds), ids(episodeIds)) || (acquisition.job_id !== null && (!job || ["queued", "running"].includes(job.status))) ||
      anchorUse === null || displaced.binding.bindingDigest !== binding.bindingDigest)
    throw new Error("Pack provenance, admission or binding changed");
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath);
  const plex = { url: settings.movies.plexUrl, token: settings.movies.plexToken, sectionId: settings.tv.plexSectionId };
  const files = replacement.version.files.map(file => {
    const episodes = file.episodeIds.map(id => roster.find(episode => episode.id === id)!);
    const numbers = episodes.map(tvdbAiredNumber).sort((a, b) => a.episode - b.episode);
    const view = renderTvFileEntry(entry, roots, targetId, packVersionId, file.relativePath, numbers);
    return { ...view, serverPath: path.posix.join(roots.serverViewRoot, view.relativePath), episodes };
  });
  return { replacement, displaced, roots, plex, files, entry,
    history: { candidateVersionId: candidate.versionId, packVersionId },
    digest: retirementDigest([nomination, target.revision, request, targetIntent, seasonIntent, plex, roots, anchorUse,
      replacement.binding.bindingDigest, replacement.version.files, displaced.version.files, files]) };
}

function exactDaemonSource(source: Source, observed: RetirementDaemonSnapshot) {
  const row = completeDaemonRecord(source.binding, observed), expected = source.source.manifest;
  // Sealing disables all wanted files to prevent repairs from rewriting adopted TV objects.
  if (!row.labels.includes("Podible Seed Only") || row.totalSize !== expected.size ||
      row.files?.length !== expected.files.length || row.fileStats?.length !== expected.files.length ||
      expected.files.some((file, index) => {
        const current = row.files?.[index];
        return !current || current.name !== file.path || current.length !== file.size || current.bytesCompleted !== file.size ||
          row.fileStats?.[index]?.wanted !== false;
      })) throw new Error("Daemon source does not match every nominated member");
  return row;
}

type Pack = ReturnType<typeof inspectTvPackRetirement>;
export function verifyPackCandidate(pack: Pack, snapshot: RetirementDaemonSnapshot, seedFloor = 0) {
  const row = exactDaemonSource(pack.displaced, snapshot);
  if (row.secondsSeeding < Math.max(MINIMUM_SEED_SECONDS, seedFloor)) throw new Error("Actual seed time is below the required nonregressing minimum");
  return row;
}

async function verifyLayout(repo: BooksRepo, source: Source, contentRoot: string) {
  const { binding, proof, version } = source, expected = source.source.manifest.files;
  await verifyStewardshipDirectory(repo, binding);
  const plan = await readCanonicalDownload(contentRoot, binding.directory);
  if (!plan?.ready || !plan.sealed || plan.clientId !== binding.clientId || plan.infoHash !== binding.hash ||
      plan.files.length !== expected.length || plan.files.some((file, index) => file.path !== expected[index]!.path || file.size !== expected[index]!.size))
    throw new Error("Sealed canonical source layout is unavailable");
  const resolve = resolveDownloadMembers(contentRoot, binding.directory, plan);
  const members = new Map(proof.members.map(member => [member.fileIndex, member.contentFileId]));
  for (const [index, file] of expected.entries()) {
    const resolved = await resolve(path.join(binding.directory, file.path), file.size);
    const stat = await fs.lstat(resolved.path);
    const current = { mtimeMs: stat.mtimeMs, ctimeMs: stat.ctimeMs, device: stat.dev, inode: stat.ino };
    if (!stat.isFile() || stat.size !== file.size || BigInt(stat.dev) !== resolved.stat.dev || BigInt(stat.ino) !== resolved.stat.ino ||
        !same(current, file.identity)) throw new Error("Verified source member identity changed");
    const id = members.get(index);
    if (id) {
      const content = version.files.find(item => item.id === id)!;
      const finalized = await finalizedCanonicalObject(contentRoot, resolved.path, binding.directory);
      if (finalized?.id !== id || finalized.relativePath !== content.relativePath || finalized.sizeBytes !== content.sizeBytes)
        throw new Error("Selected or displaced canonical object identity changed");
    }
  }
  if (!same(plan, await readCanonicalDownload(contentRoot, binding.directory))) throw new Error("Canonical source layout changed");
}

/** Only read-only layout, daemon and Plex verification. Never stages publication, scans Plex, or modifies a file. */
export async function verifyTvPackRetirement(repo: BooksRepo, pack: Pack, candidate: RetirementDaemonSnapshot,
  daemon: RetirementDaemon, active: () => void, library?: PackRetirementLibrary) {
  const deadline = AbortSignal.any([AbortSignal.timeout(8_000), ...(currentEffects() ? [currentEffects()!.signal] : [])]);
  const plex = library ?? new PlexTvLibrary(pack.plex, Object.assign((input: Parameters<typeof fetch>[0], init?: RequestInit) =>
    boundedStewardshipFetch(input, { ...init, signal: AbortSignal.any([deadline, ...(init?.signal ? [init.signal] : [])]) }),
    { preconnect: fetch.preconnect }));
  active();
  if (!await plex.verify(pack.entry, pack.files)) throw new Error("Plex no longer verifies every exact selected episode and path");
  active();
  await assertMovieStorage(repo, pack.roots.contentRoot);
  await verifyLayout(repo, pack.displaced, pack.roots.contentRoot);
  await verifyLayout(repo, pack.replacement, pack.roots.contentRoot);
  for (const file of pack.files) {
    const view = path.join(pack.roots.viewRoot, file.relativePath);
    if (await fs.realpath(path.dirname(view)) !== path.dirname(view) || !(await fs.lstat(view)).isSymbolicLink() ||
        await fs.realpath(view) !== file.contentPath) throw new Error("Exact selected TV view is unavailable");
  }
  return verifyPackAnchor(pack, candidate, daemon, active);
}

export async function verifyPackAnchor(pack: Pack, candidate: RetirementDaemonSnapshot, daemon: RetirementDaemon, active: () => void) {
  active();
  const anchor = await daemon.read(pack.replacement.binding, true);
  active();
  const row = exactDaemonSource(pack.replacement, anchor);
  if (anchor.identityDigest !== candidate.identityDigest || anchor.sessionDigest !== candidate.sessionDigest || row.id === candidate.record?.id)
    throw new Error("Pack and displaced registrations are not distinct records in one private daemon session");
  return retirementDigest([anchor.identityDigest, anchor.sessionDigest, row.id, pack.replacement.proof]);
}
