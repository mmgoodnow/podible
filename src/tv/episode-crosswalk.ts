import { createHash } from "node:crypto";
import { z } from "zod";
import { TEXT_AGENT_MODEL } from "../ai-models";
import type { TvCatalogEntry, TvCatalogEpisode } from "./types";
import type { SeerrTvSeason } from "./upstream";

export type RequestedProviderEpisode = SeerrTvSeason["episodes"][number];
export type EpisodeCorrespondence = NonNullable<TvCatalogEpisode["correspondences"]>[number];
export const episodeMappingDecisionSchema = z.object({
  sourceId: z.number().int().positive(), targetKey: z.string().nullable(), reason: z.string().min(1).max(2000),
});
export type EpisodeMappingDecision = z.infer<typeof episodeMappingDecisionSchema>;
export interface EpisodeMappingResearch {
  (input: { entry: TvCatalogEntry; sources: RequestedProviderEpisode[]; candidates: TvCatalogEpisode[] }): Promise<EpisodeMappingDecision[]>;
}
export class EpisodeMappingError extends Error {}

export const episodeTitle = (title: string) => title.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
const date = (value: string | null) => value?.slice(0, 10) || null;
// Only exact metadata snapshots reuse a decision. Renumbering/title edits trigger research again.
export function episodeMappingFingerprint(tmdbId: number, source: RequestedProviderEpisode, target: TvCatalogEpisode) {
  return createHash("sha256").update(JSON.stringify({ tmdbId, source, target: {
    key: target.key, title: target.title, airDate: target.airDate, runtimeMinutes: target.runtimeMinutes,
    numbering: target.numbering.filter(n => n.scheme === "tvdb:aired"),
  } })).digest("hex");
}

/** Structural validation stays separate from semantic review; numbers and counts cannot prove identity. */
export function validateEpisodeMappingDecisions(raw: unknown, sources: RequestedProviderEpisode[], candidates: TvCatalogEpisode[]) {
  const decisions = z.array(episodeMappingDecisionSchema).parse(raw);
  const sourceIds = new Set(sources.map(ep => ep.id)), targetKeys = new Set(candidates.map(ep => ep.key));
  const seen = new Set<number>(), assigned = new Set<string>();
  for (const decision of decisions) {
    if (!sourceIds.has(decision.sourceId) || seen.has(decision.sourceId)) throw new EpisodeMappingError("Unknown or duplicate requested episode in mapping");
    seen.add(decision.sourceId);
    if (decision.targetKey !== null) {
      if (!targetKeys.has(decision.targetKey) || assigned.has(decision.targetKey)) throw new EpisodeMappingError("Unknown or duplicate canonical episode in mapping");
      const source = sources.find(ep => ep.id === decision.sourceId)!;
      if (source.tvdbId !== null && decision.targetKey !== `tvdb:${source.tvdbId}`)
        throw new EpisodeMappingError("Mapping contradicts an explicit provider episode identity");
      assigned.add(decision.targetKey);
    }
  }
  if (seen.size !== sourceIds.size) throw new EpisodeMappingError("Mapping omitted requested episodes");
  return decisions;
}

export async function mapRequestedEpisodes(input: {
  entry: TvCatalogEntry; tmdbId: number; seasons: number[]; metadata: SeerrTvSeason[];
  previous?: TvCatalogEpisode[]; research: EpisodeMappingResearch;
}): Promise<{ entry: TvCatalogEntry; selectedKeys: string[] }> {
  const { entry, tmdbId, metadata } = input;
  if (entry.tmdbId !== tmdbId || !entry.tvdbId ||
      (entry.key.startsWith("tvdb:") && entry.key !== `tvdb:${entry.tvdbId}`) ||
      entry.refs?.some(ref => (ref.provider === "tmdb" && ref.id !== String(tmdbId)) || (ref.provider === "tvdb" && ref.id !== String(entry.tvdbId))))
    throw new EpisodeMappingError("Episode mapping work identity disagrees with the request");
  const requested = new Set(input.seasons);
  if (metadata.length !== requested.size || new Set(metadata.map(s => s.seasonNumber)).size !== metadata.length ||
      metadata.some(s => s.tmdbId !== tmdbId || !requested.has(s.seasonNumber) || !s.episodes.length))
    throw new EpisodeMappingError("Requested provider seasons are missing or conflicting");
  const sources = metadata.flatMap(s => s.episodes);
  if (new Set(sources.map(ep => ep.id)).size !== sources.length ||
      new Set(sources.map(ep => `${ep.seasonNumber}:${ep.episodeNumber}`)).size !== sources.length ||
      metadata.some(s => s.episodes.some(ep => ep.seasonNumber !== s.seasonNumber)))
    throw new EpisodeMappingError("Provider episodes have duplicate identities or conflicting season membership");
  const targets = entry.episodes;
  if (new Set(targets.map(ep => ep.key)).size !== targets.length || targets.some(ep => !/^tvdb:[1-9]\d*$/.test(ep.key) || ep.identity?.stability === "ephemeral"))
    throw new EpisodeMappingError("Canonical episodes require distinct stable TVDB identities");
  const mapping = new Map<number, { target: TvCatalogEpisode; evidence: EpisodeCorrespondence }>();
  const used = new Set<string>();
  const contextFingerprint = createHash("sha256").update(JSON.stringify([...targets].sort((a, b) => a.key.localeCompare(b.key))
    .map(ep => ({ key: ep.key, title: ep.title, airDate: ep.airDate, runtimeMinutes: ep.runtimeMinutes,
      numbering: ep.numbering.filter(n => n.scheme === "tvdb:aired"), refs: ep.refs })))).digest("hex");
  const bind = (source: RequestedProviderEpisode, target: TvCatalogEpisode, method: EpisodeCorrespondence["method"], reason: string, model: string | null) => {
    if (used.has(target.key)) throw new EpisodeMappingError("Multiple requested episodes claim one canonical episode");
    if (source.tvdbId !== null && target.key !== `tvdb:${source.tvdbId}`) throw new EpisodeMappingError("Mapping contradicts explicit provider identity");
    used.add(target.key);
    mapping.set(source.id, { target, evidence: { provider: "tmdb", workId: tmdbId, episodeId: source.id,
      season: source.seasonNumber, episode: source.episodeNumber, fingerprint: episodeMappingFingerprint(tmdbId, source, target), method, reason, model,
      ...(method === "agent" ? { contextFingerprint } : {}) } });
  };
  for (const source of sources) {
    const explicit = source.tvdbId === null ? [] : targets.filter(ep => ep.key === `tvdb:${source.tvdbId}`);
    if (source.tvdbId !== null) {
      if (explicit.length !== 1) throw new EpisodeMappingError("Explicit provider episode is absent from canonical work");
      bind(source, explicit[0]!, "provider_id", "Provider supplied the same TVDB episode ID.", null); continue;
    }
    const title = episodeTitle(source.title);
    const matches = targets.filter(ep => episodeTitle(ep.title) === title);
    const sourceDuplicates = sources.filter(ep => episodeTitle(ep.title) === title);
    const uniqueTitleDate = (target: TvCatalogEpisode) => !!title && matches.length === 1 && sourceDuplicates.length === 1 &&
      matches[0]!.key === target.key && date(source.airDate) !== null && date(target.airDate) === date(source.airDate);
    const saved = (input.previous ?? []).flatMap(ep => (ep.correspondences ?? []).filter(proof =>
      proof.workId === tmdbId && proof.episodeId === source.id).map(proof => ({ key: ep.key, proof })))
      .filter(({ key, proof }) => { const target = targets.find(ep => ep.key === key); return target &&
        (proof.method !== "agent" || proof.contextFingerprint === contextFingerprint) &&
        (proof.method !== "title_date" || uniqueTitleDate(target)) && proof.fingerprint === episodeMappingFingerprint(tmdbId, source, target); });
    if (saved.length > 1) throw new EpisodeMappingError("Conflicting persisted episode correspondences");
    if (saved.length === 1) {
      const { key, proof } = saved[0]!; bind(source, targets.find(ep => ep.key === key)!, proof.method, proof.reason, proof.model); continue;
    }
    if (matches.length === 1 && uniqueTitleDate(matches[0]!))
      bind(source, matches[0]!, "title_date", "Unique normalized episode title and matching provider air date within the verified work.", null);
  }
  const unresolved = sources.filter(ep => !mapping.has(ep.id));
  // Small independent tasks, not a complete long-series dump into one context window.
  for (let offset = 0; offset < unresolved.length; offset += 12) {
    const batch = unresolved.slice(offset, offset + 12), candidates = targets.filter(ep => !used.has(ep.key));
    const decisions = validateEpisodeMappingDecisions(await input.research({ entry, sources: batch, candidates }), batch, candidates);
    if (decisions.some(d => d.targetKey === null)) throw new EpisodeMappingError("Episode research could not resolve all approved episodes; request scope retained");
    for (const decision of decisions) bind(batch.find(ep => ep.id === decision.sourceId)!, targets.find(ep => ep.key === decision.targetKey)!, "agent", decision.reason, TEXT_AGENT_MODEL);
  }
  const byKey = new Map([...mapping.values()].map(value => [value.target.key, value.evidence]));
  return { selectedKeys: sources.map(source => mapping.get(source.id)!.target.key), entry: { ...entry,
    episodes: targets.map(ep => {
      const proof = byKey.get(ep.key);
      if (!proof) return ep;
      const source = `episode-crosswalk:tmdb:${tmdbId}:${proof.method}`;
      return { ...ep, refs: [...(ep.refs ?? []).filter(ref => ref.provider !== "tmdb"), { provider: "tmdb", id: String(proof.episodeId), instance: null, source }],
        numbering: [...ep.numbering.filter(n => n.scheme !== "tmdb:aired"), { scheme: "tmdb:aired", season: proof.season, episode: proof.episode, absolute: null, source }],
        correspondences: [...(ep.correspondences ?? []).filter(p => p.provider !== "tmdb" || p.workId !== tmdbId), proof] };
    }) } };
}
