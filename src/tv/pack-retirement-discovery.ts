import { z } from "zod";
import type { BooksRepo } from "../repo";
import { packRetirementLocatorSchema } from "../media/torrent-retirement-types";
import { tvPackRetirementNominationKey, tvPackRetirementNominationPrefix } from "./pack-retirement-nominations";

const cursorKey = "tv-pack-retirement:maintenance-cursor:v1";
const cursorSchema = z.strictObject({ key: z.string().max(128), index: z.number().int().min(0).max(63) });

/** One pair per slot, including ineligible pairs. Rotate independently of the observation page. */
export function nextPackRetirementCandidate(repo: BooksRepo, clientId: string) {
  return repo.transaction(() => {
    const stored = cursorSchema.safeParse(repo.getJsonState(cursorKey));
    const cursor = stored.success ? stored.data : { key: "", index: 0 };
    const key = repo.readStewardshipInventory({ source: "screen", offset: 0, limit: 1,
      packRetirementAtOrAfter: cursor.key }).packNominationKeys?.[0];
    if (!key) { repo.setJsonState(cursorKey, null); return null; }
    const [targetId, packVersionId] = key.slice(tvPackRetirementNominationPrefix.length).split(":").map(Number);
    const parsed = packRetirementLocatorSchema.safeParse({ targetId, packVersionId });
    const nomination = parsed.success && key === tvPackRetirementNominationKey(targetId!, packVersionId!)
      ? repo.tv.packRetirementNomination(targetId!, packVersionId!) : null;
    const index = cursor.key === key ? cursor.index : 0;
    const candidate = nomination?.superseded[index];
    // '!' sorts before any continuation digit, so a prefix key cannot skip its successor.
    repo.setJsonState(cursorKey, index + 1 < (nomination?.superseded.length ?? 0)
      ? { key, index: index + 1 } : { key: key + "!", index: 0 });
    return candidate?.clientId === clientId && parsed.success
      ? { source: "screen" as const, clientId, infoHash: candidate.infoHash, packNomination: parsed.data } : null;
  });
}
