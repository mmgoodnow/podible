import { z } from "zod";
import type { BooksRepo } from "../repo";

export const TV_SEASON_PACK_PROMOTION_PILOT = "tv-season-pack-promotion-target-ids";
export const TV_SEASON_PACK_PROMOTION_MAX_TARGETS = 100;
const targetsSchema = z.array(z.number().int().positive().safe()).max(TV_SEASON_PACK_PROMOTION_MAX_TARGETS);

/** Missing or malformed private state disables the entire pilot. */
export function tvSeasonPackPromotionTargets(repo: Pick<BooksRepo, "getJsonState">): ReadonlySet<number> {
  const parsed = targetsSchema.safeParse(repo.getJsonState(TV_SEASON_PACK_PROMOTION_PILOT));
  return new Set(parsed.success ? parsed.data : []);
}
