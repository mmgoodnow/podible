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

export function inlineTvImportEvidence(files: TvFileCandidate[], episodes: TvEpisode[]) {
  if (files.length > 16 || episodes.length > 16) return;
  const evidence = { files: files.map(file => tvProbeEvidence(file)), episodes: episodes.map(tvEpisodeEvidence) };
  if (Buffer.byteLength(JSON.stringify(evidence)) <= 16_000) return evidence;
}

type InlineTvImportEvidence = NonNullable<ReturnType<typeof inlineTvImportEvidence>>;

export const compactTvImportLegend = `Inline evidence tables (columns in order):
episodes: [id, key, title, runtimeMinutes, numbering, identityStability, moreNumbering].
Each numbering row: [numberingSourceIndex, season, episode, absolute]. numberingSources rows: [scheme, source].
files: [index, name, sizeBytes, durationSeconds, streamCount, nextOffset, streamProfileIndex].
Each streamProfiles entry is an ordered list of [codec_type, codec_name, width, height, color_transfer, attached_pic, language].
Indexes into numberingSources and streamProfiles are zero-based. Null duration or stream fields mean absent; other nulls keep their usual meaning. File and episode IDs are unchanged.`;

/** Pack the already-sanitized inline evidence, not the raw probe or catalog. */
export function compactTvImportEvidence(evidence: InlineTvImportEvidence) {
  const numberingSources: (readonly [string, string])[] = [];
  type StreamProfile = ReturnType<typeof packStreams>;
  const streamProfiles: StreamProfile[] = [];
  const sourceIndexes = new Map<string, number>();
  const streamIndexes = new Map<string, number>();
  const intern = <T>(row: T, rows: T[], indexes: Map<string, number>) => {
    const key = JSON.stringify(row);
    const previous = indexes.get(key);
    if (previous !== undefined) return previous;
    const index = rows.length;
    rows.push(row);
    indexes.set(key, index);
    return index;
  };
  function packStreams(file: InlineTvImportEvidence["files"][number]) {
    return file.streams.map(stream => [stream.codec_type ?? null, stream.codec_name ?? null, stream.width ?? null,
      stream.height ?? null, stream.color_transfer ?? null, stream.disposition.attached_pic ?? null, stream.tags.language ?? null] as const);
  }
  return {
    episodes: evidence.episodes.map(ep => [ep.id, ep.key, ep.title, ep.runtimeMinutes,
      ep.numbering.map(n => [intern([n.scheme, n.source] as const, numberingSources, sourceIndexes), n.season, n.episode, n.absolute] as const),
      ep.identity.stability, ep.moreNumbering] as const),
    files: evidence.files.map(file => [file.index, file.name, file.sizeBytes, file.format.duration ?? null, file.streamCount,
      file.nextOffset, intern(packStreams(file), streamProfiles, streamIndexes)] as const),
    numberingSources, streamProfiles,
  };
}

export function evidencePage<T>(values: T[], offset: number, limit: number, present: (value: T) => unknown) {
  return modelEvidencePage(values, offset, limit, present);
}
