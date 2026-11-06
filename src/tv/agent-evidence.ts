import type { TvEpisode, TvEntry } from "./types";
import type { TvFileCandidate } from "./agents";

export function tvWorkEvidence(entry: TvEntry) {
  return { id: entry.id, key: entry.key, title: entry.title, year: entry.year, tvdbId: entry.tvdbId, tmdbId: entry.tmdbId,
    aliases: entry.aliases.slice(0,30), relations: entry.relations.slice(0,20),
    moreAliases: Math.max(0,entry.aliases.length-30), moreRelations: Math.max(0,entry.relations.length-20) };
}
export function tvEpisodeEvidence(episode: TvEpisode) {
  return { id: episode.id, key: episode.key, title: episode.title, runtimeMinutes: episode.runtimeMinutes,
    numbering: episode.numbering, identity: episode.identity };
}
export function tvFileEvidence(file: TvFileCandidate) {
  return { index: file.index, name: file.name, sizeBytes: file.sizeBytes };
}
/** Probe container metadata can contain huge arbitrary tags. Only relevant stream facts enter model context. */
export function tvProbeEvidence(file: TvFileCandidate, offset = 0, limit = 12) {
  const streams = Array.isArray(file.probe.streams) ? file.probe.streams : [];
  return { ...tvFileEvidence(file), format: { duration: file.probe.format?.duration },
    streamCount: streams.length, nextOffset: offset+limit < streams.length ? offset+limit : null,
    streams: streams.slice(offset,offset+limit).map(stream => ({
      codec_type: stream.codec_type, codec_name: stream.codec_name, width: stream.width, height: stream.height,
      color_transfer: stream.color_transfer, disposition: stream.disposition,
      tags: { language: stream.tags?.language },
    })) };
}
export function evidencePage<T>(values: T[], offset: number, limit: number, present: (value: T) => unknown) {
  const items: unknown[] = [];
  let length = 0;
  for (const value of values.slice(offset,offset+limit)) {
    const item = present(value), size = JSON.stringify(item).length;
    if (size > 12_000) throw new Error("Evidence record exceeds tool response budget; narrow the evidence projection");
    if (length+size > 12_000) break;
    items.push(item); length += size;
  }
  return { total: values.length, offset, nextOffset: offset+items.length < values.length ? offset+items.length : null, items };
}
