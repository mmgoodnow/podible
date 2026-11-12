import type { TvEpisode, TvEntry } from "./types";
import type { TvFileCandidate } from "./agents";
import { modelEvidencePage, modelEvidenceText } from "../library/decisions";
import { videoProbeEvidence } from "../media/video-decisions";

const text = (value: string) => modelEvidenceText(value, 240);

export function tvWorkEvidence(entry: TvEntry) {
  return { id: entry.id, key: text(entry.key), title: text(entry.title), year: entry.year, tvdbId: entry.tvdbId, tmdbId: entry.tmdbId,
    aliases: entry.aliases.slice(0,30).map(text), relations: entry.relations.slice(0,20).map(relation =>
      ({ key: text(relation.key), title: text(relation.title), relation: text(relation.relation) })),
    moreAliases: Math.max(0,entry.aliases.length-30), moreRelations: Math.max(0,entry.relations.length-20) };
}
export function tvEpisodeEvidence(episode: TvEpisode) {
  return { id: episode.id, key: text(episode.key), title: text(episode.title), runtimeMinutes: episode.runtimeMinutes,
    numbering: episode.numbering.slice(0, 30).map(({ scheme, season, episode, absolute, source }) =>
      ({ scheme: modelEvidenceText(scheme, 80), season, episode, absolute, source: modelEvidenceText(source, 80) })),
    identity: { stability: episode.identity.stability }, moreNumbering: Math.max(0, episode.numbering.length - 30) };
}
export function tvFileEvidence(file: TvFileCandidate) {
  return { index: file.index, name: text(file.name), sizeBytes: file.sizeBytes };
}
/** Probe container metadata can contain huge arbitrary tags. Only relevant stream facts enter model context. */
export function tvProbeEvidence(file: TvFileCandidate, offset = 0, limit = 12) {
  return { ...tvFileEvidence(file), ...videoProbeEvidence(file, offset, limit) };
}
export function evidencePage<T>(values: T[], offset: number, limit: number, present: (value: T) => unknown) {
  return modelEvidencePage(values, offset, limit, present);
}
