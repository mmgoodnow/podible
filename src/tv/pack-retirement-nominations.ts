import { z } from "zod";
import { reuseDigest } from "../media/reuse-plan";
import type { TorrentRecord } from "../media/torrent-records";
import { playableTvMember } from "./season-promotion";
import { tvSourceSchema } from "./source-reuse";
import type { TvContentFile, TvVersion } from "./types";

const id = z.number().int().positive().safe();
const fileIndex = z.number().int().nonnegative().safe();
const episodeIds = z.array(id).min(1).max(64);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const clientId = z.string().regex(/^transmission:[a-f0-9-]{36}$/);
const location = z.strictObject({ rootId: z.literal("tv"), relativePath: z.string().min(1) });
const proofSchema = z.strictObject({
  versionId: id, acquisitionId: id, torrentId: id, clientId,
  infoHash: z.string().regex(/^[a-f0-9]{40}$/), location, manifestDigest: digest,
  members: z.array(z.strictObject({ fileIndex, contentFileId: z.string().min(1), episodeIds })).min(1).max(64),
});
type SourceProof = z.infer<typeof proofSchema>;
type NominationVersion = Pick<TvVersion, "id" | "acquisitionId" | "facts"> & {
  files: readonly Pick<TvContentFile, "id" | "sizeBytes" | "episodeIds">[];
};
const episodeKey = (ids: readonly number[]) => JSON.stringify([...ids].sort((a, b) => a - b));

// This receipt captures publication-time provenance, not current removal eligibility.
export const tvPackRetirementNominationSchema = z.strictObject({
  targetId: id, requestId: id, publishedRevision: id, season: id, promotionFingerprint: digest, episodeIds,
  pack: proofSchema, superseded: z.array(proofSchema).min(1).max(64),
}).refine(({ episodeIds, pack, superseded }) => {
  const wanted = new Set(episodeIds);
  const torrents = new Set([pack.torrentId]);
  return wanted.size === episodeIds.length && episodeKey(pack.members.flatMap(member => member.episodeIds)) === episodeKey(episodeIds) &&
    superseded.every(source => {
      const member = source.members[0];
      if (torrents.has(source.torrentId) || source.clientId !== pack.clientId || source.members.length !== 1 ||
          member?.episodeIds.length !== 1 || !wanted.has(member.episodeIds[0]!)) return false;
      torrents.add(source.torrentId);
      return true;
    });
});

export const tvPackRetirementNominationKey = (targetId: number, packVersionId: number) =>
  `tv-pack-retirement:nomination:v1:${targetId}:${packVersionId}`;

export function readTvNominationJson(value: string | null): unknown {
  try { return value === null ? null : JSON.parse(value); }
  catch { return null; }
}

const sourceFacts = z.object({
  identityVerified: z.literal(true), source: tvSourceSchema,
  plan: z.object({ files: z.array(z.object({ fileIndex, episodeIds })).min(1).max(64) }),
});
/** Match the recorded import to its exact acquisition binding, never to a sibling snatch. */
export function tvPackRetirementSource(version: NominationVersion, binding: TorrentRecord): SourceProof | null {
  const facts = sourceFacts.safeParse(version.facts);
  if (!facts.success) return null;
  const { source, plan } = facts.data;
  if (!clientId.safeParse(source.ref.clientId).success || binding.client_id !== source.ref.clientId || binding.native_id !== source.ref.nativeId ||
      binding.backend !== "transmission" || binding.ownership_tag !== "Podible TV" || binding.offer_key !== source.offerKey) return null;
  const boundLocation = location.safeParse(readTvNominationJson(binding.location_json));
  if (!boundLocation.success || boundLocation.data.relativePath !== source.location.relativePath) return null;

  if (source.manifest.files.some(file => !file.identity)) return null;
  const playable = new Set(source.manifest.files.flatMap((file, i) => playableTvMember(file.path) ? [i] : []));
  if (playable.size !== plan.files.length || playable.size !== version.files.length) return null;
  const members: SourceProof["members"] = [];
  for (const [sequence, mapping] of plan.files.entries()) {
    const content = version.files[sequence]!;
    const member = source.manifest.files[mapping.fileIndex];
    if (!playable.delete(mapping.fileIndex) || !member || member.size <= 0 || member.size !== content.sizeBytes ||
        episodeKey(mapping.episodeIds) !== episodeKey(content.episodeIds)) return null;
    members.push({ fileIndex: mapping.fileIndex, contentFileId: content.id, episodeIds: content.episodeIds });
  }
  return {
    versionId: version.id, acquisitionId: version.acquisitionId, torrentId: binding.id,
    clientId: binding.client_id, infoHash: binding.native_id, location: boundLocation.data,
    manifestDigest: reuseDigest(source.manifest), members: members.sort((a, b) => a.fileIndex - b.fileIndex),
  };
}
