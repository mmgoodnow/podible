import path from "node:path";
import { DECISION_MAX_CANDIDATES, modelEvidenceText } from "../library/decisions";
import type { MovieOffer } from "../movies/offers";
import type { MovieFileCandidate } from "../movies/operations";
import { factsFromMovieProbe } from "../movies/probe";
import { evaluateMovieQuality, type MovieFacts, type MovieQualityPolicy } from "../movies/quality";
import type { TvEntry, TvEpisode } from "../tv/types";

const words = (text: string) => text.normalize("NFC").toLowerCase().replace(/[ ._()[\]-]+/g, " ").trim();
type Identity = { titles: string[]; marker: string; year?: number };
type Claims = Pick<MovieFacts, "identityVerified" | "cut" | "source" | "proper">;

export function videoPolicyEvidence(policy: MovieQualityPolicy) {
  const label = (value: string) => modelEvidenceText(value, 80);
  return { tiers: policy.tiers.slice(0, 40).map(({ source, resolution }) => ({ source, resolution })),
    cut: policy.cut === null ? null : label(policy.cut), audioLanguages: policy.audioLanguages.slice(0, 30).map(label),
    allowedCodecs: policy.allowedCodecs.slice(0, 30).map(label), preferredCodecs: policy.preferredCodecs.slice(0, 30).map(label),
    allowHdr: policy.allowHdr, minimumPreferenceGain: policy.minimumPreferenceGain, preferProper: true };
}

export function videoProbeEvidence(file: MovieFileCandidate, offset = 0, limit = 12) {
  const safe = (value: string | undefined) => value && /^[a-z0-9_-]{1,32}$/i.test(value) ? value : undefined;
  const streams = file.probe.streams;
  const duration = file.probe.format?.duration;
  return { format: { duration: duration && /^\d{1,9}(?:\.\d{1,9})?$/.test(duration) ? duration : undefined },
    streamCount: streams.length, nextOffset: offset + limit < streams.length ? offset + limit : null,
    streams: streams.slice(offset, offset + limit).map(stream => ({ codec_type: safe(stream.codec_type), codec_name: safe(stream.codec_name),
      width: stream.width, height: stream.height, color_transfer: safe(stream.color_transfer),
      disposition: { attached_pic: stream.disposition?.attached_pic }, tags: { language: safe(stream.tags?.language) } })) };
}

export function videoFactsEvidence(facts: MovieFacts) {
  return { identityVerified: facts.identityVerified, cut: facts.cut === null ? null : modelEvidenceText(facts.cut, 80),
    source: facts.source, width: facts.width, height: facts.height,
    videoCodec: facts.videoCodec === null ? null : modelEvidenceText(facts.videoCodec, 32), dynamicRange: facts.dynamicRange,
    audioLanguages: facts.audioLanguages.slice(0, 30).map(language => modelEvidenceText(language, 24)), proper: facts.proper };
}

export function movieDecisionIdentity(movie: { title: string; originalTitle: string; year: number | null }): Identity | null {
  return movie.year && movie.year >= 1800 && movie.year <= 2200 ? { titles: [movie.title, movie.originalTitle], marker: String(movie.year) } : null;
}

export function tvDecisionIdentity(entry: TvEntry, episodes: TvEpisode[]): Identity | null {
  const [episode] = episodes;
  if (episodes.length !== 1 || !episode || !entry.tvdbId || !entry.year || entry.year < 1800 || entry.year > 2200 ||
      entry.anime || entry.relations.length || episode.identity.stability !== "provider") return null;
  const aired = episode.numbering.filter(number => number.scheme === "tvdb:aired");
  const coordinate = aired[0];
  if (aired.length !== 1 || !coordinate?.season || !coordinate.episode || entry.episodes.some(other => other.id !== episode.id &&
      other.numbering.some(number => number.scheme === "tvdb:aired" && number.season === coordinate.season && number.episode === coordinate.episode))) return null;
  return { titles: [entry.title], year: entry.year, marker: `S${String(coordinate.season).padStart(2, "0")}E${String(coordinate.episode).padStart(2, "0")}` };
}

/** An intentionally narrow grammar. Unknown labels, editions and multi-episode names require tool review. */
function advertised(identity: Identity, title: string): MovieFacts | null {
  if (modelEvidenceText(title) !== title) return null;
  const normalized = words(title);
  for (const name of identity.titles) {
    if (!words(name) || modelEvidenceText(name) !== name) continue;
    const prefix = `${words(name)} ${identity.year === undefined ? "" : `${identity.year} `}${identity.marker.toLowerCase()} `;
    if (!normalized.startsWith(prefix)) continue;
    const match = /^(480|720|1080|2160)p (web dl|web rip|web|webrip|webdl|bluray|blu ray|hdtv|dvd) (h ?264|x264|h ?265|x265|hevc|av1)(?: (proper|repack))?$/.exec(normalized.slice(prefix.length));
    if (!match) continue;
    const [, resolution, source, codec, proper] = match;
    const height = Number(resolution);
    return { identityVerified: true, cut: null, source: source!.startsWith("web") ? "web" : source!.startsWith("blu") ? "bluray" : source === "dvd" ? "dvd" : "hdtv",
      width: { 480: 720, 720: 1280, 1080: 1920, 2160: 3840 }[height as 480 | 720 | 1080 | 2160], height,
      videoCodec: codec === "av1" ? "av1" : codec!.endsWith("264") ? "h264" : "hevc", dynamicRange: null, audioLanguages: [], proper: !!proper };
  }
  return null;
}

export function screenedVideoOffers(identity: Identity, offers: MovieOffer[], policy: MovieQualityPolicy, maximumBytes = Infinity) {
  if (!offers.length || offers.length > DECISION_MAX_CANDIDATES || new Set(offers.map(offer => offer.key)).size !== offers.length) return null;
  const screened: { offer: MovieOffer; facts: MovieFacts; tier: number; preference: number }[] = [];
  for (const offer of offers) {
    if (!Number.isSafeInteger(offer.sizeBytes) || offer.sizeBytes! <= 0 || offer.sizeBytes! > maximumBytes ||
        !Number.isFinite(offer.seeders) || offer.seeders! <= 0) return null;
    const facts = advertised(identity, offer.title);
    if (!facts) return null;
    const quality = evaluateMovieQuality(policy, facts);
    if (quality.eligibility !== "eligible" || quality.tier === null) return null;
    screened.push({ offer, facts, tier: quality.tier, preference: quality.preferenceScore });
  }
  const compare = (a: typeof screened[number], b: typeof screened[number]) => a.tier - b.tier || a.preference - b.preference;
  const best = screened.reduce((a, b) => compare(a, b) >= 0 ? a : b);
  // Do not bypass the evidence needed for viable same-tier and same-preference ties.
  if (screened.some(candidate => candidate !== best && compare(candidate, best) === 0)) return null;
  const preferredIndex = screened.indexOf(best);
  return { ...best, preferredIndex,
    candidates: screened.map(({ offer }, index) => ({ title: offer.title, ...(screened.length > 1 ? { preferred: index === preferredIndex } : {}) })) };
}

export function screenedVideoFile(identity: Identity, releaseTitle: string, files: MovieFileCandidate[], policy: MovieQualityPolicy,
  runtimeMinutes: number | null | undefined): { file: MovieFileCandidate; title: string; claims: Claims } | null {
  const [file] = files;
  if (files.length !== 1 || !file || !Number.isSafeInteger(file.sizeBytes) || file.sizeBytes <= 0 || !runtimeMinutes || !Number.isFinite(runtimeMinutes) || runtimeMinutes <= 0 ||
      modelEvidenceText(file.name) !== file.name) return null;
  const title = path.posix.basename(file.name.replaceAll("\\", "/"));
  if (!/\.(mkv|mp4|m4v|avi|mov|ts)$/i.test(title)) return null;
  const release = advertised(identity, releaseTitle), named = advertised(identity, title.slice(0, title.lastIndexOf(".")));
  if (!release || !named || release.source !== named.source || release.videoCodec !== named.videoCodec || release.height !== named.height ||
      release.proper !== named.proper) return null;
  const duration = Number(file.probe.format?.duration), ratio = duration / (runtimeMinutes * 60);
  if (!Number.isFinite(ratio) || ratio < 0.94 || ratio > 1.1 || !file.probe.streams.some(stream => stream.codec_type === "audio")) return null;
  const { identityVerified, cut, source, proper } = named;
  const claims = { identityVerified, cut, source, proper };
  const facts = factsFromMovieProbe(file.probe, claims), assessed = evaluateMovieQuality(policy, facts);
  const advertisedTier = policy.tiers.findIndex(tier => tier.source === named.source && tier.resolution === named.height);
  return assessed.eligibility === "eligible" && assessed.tier === advertisedTier && facts.videoCodec === named.videoCodec
    ? { file, title, claims } : null;
}
