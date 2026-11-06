import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { MovieOffer } from "../movies/offers";
import { crossSeedScopeSchema, type CrossSeedScope } from "./cross-seed-state";

const offerSchema = z.object({
  key: z.string().min(1), title: z.string().min(1), provider: z.string(),
  sizeBytes: z.number().nonnegative().nullable(), seeders: z.number().nullable(),
  url: z.url().refine(value => ["http:", "https:"].includes(new URL(value).protocol)),
  infoHash: z.string().nullable(), attributes: z.record(z.string(), z.unknown()),
  inboxId: z.number().int().positive().optional(),
});
const copySchema = z.object({
  scope: crossSeedScopeSchema, acquisitionId: z.number().int().positive(),
  selectedOfferKey: z.string(), offer: offerSchema,
});
const groupSchema = z.object({
  selected: offerSchema,
  copies: z.array(z.object({ id: z.string().uuid(), ...copySchema.shape })).max(2000),
});
const copyKey = (id: string) => `cross-seed:selected-copy:${z.string().uuid().parse(id)}`;
const groupKey = (scope: CrossSeedScope, acquisitionId: number, selectedKey: string) =>
  `release-selection:${scope.media}:${acquisitionId}:${createHash("sha256").update(selectedKey).digest("hex")}`;
const sourceKey = (scope: CrossSeedScope, selectedKey: string) =>
  `release-selection:source:${scope.media}:${scope.targetId}:${createHash("sha256").update(selectedKey).digest("hex")}`;

export function selectedCopyId(value: string): string {
  const hash = createHash("sha256").update(value).digest("hex");
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-4${hash.slice(13, 16)}-8${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}

/** Server-owned search evidence, captured before downloading, independent of RSS retention. */
export function rememberSelectedCopies(repo: BooksRepo, scope: CrossSeedScope, acquisitionId: number,
  selected: MovieOffer, alternatives: MovieOffer[] = []) {
  crossSeedScopeSchema.parse(scope);
  z.number().int().positive().parse(acquisitionId);
  const key = groupKey(scope, acquisitionId, selected.key);
  return repo.transaction(() => {
    const previous = repo.getJsonState<unknown>(key);
    if (previous) return groupSchema.parse(previous);
    const unique = [...new Map(alternatives.filter(offer => offer.key !== selected.key).map(offer => [offer.key, offer])).values()];
    const group = groupSchema.parse({ selected, copies: unique.map(offer => ({
      id: selectedCopyId(JSON.stringify([scope, acquisitionId, selected.key, offer.key])),
      scope, acquisitionId, selectedOfferKey: selected.key, offer,
    })) });
    for (const copy of group.copies) repo.setJsonState(copyKey(copy.id), copy);
    repo.setJsonState(key, group);
    const source = sourceKey(scope, selected.key);
    const ids = z.array(z.string().uuid()).parse(repo.getJsonState(source) ?? []);
    repo.setJsonState(source, [...new Set([...ids, ...group.copies.map(copy => copy.id)])]);
    return group;
  });
}

export function selectedCopyOffer(repo: BooksRepo, scope: CrossSeedScope, id: string): MovieOffer {
  const copy = copySchema.parse(repo.getJsonState(copyKey(id)));
  if (copy.scope.media !== scope.media || copy.scope.targetId !== scope.targetId)
    throw new Error("Selected copy belongs to another target");
  return copy.offer;
}

export function selectedCopies(repo: BooksRepo, scope: CrossSeedScope, acquisitionId: number, selectedKey: string) {
  const raw = repo.getJsonState<unknown>(groupKey(scope, acquisitionId, selectedKey));
  // A retained pack may be imported by several acquisitions; search evidence belongs to its source too.
  const ids = z.array(z.string().uuid()).parse(repo.getJsonState(sourceKey(scope, selectedKey)) ?? []);
  const copies = [...(raw ? groupSchema.parse(raw).copies : []), ...ids.map(id => ({
    id, ...copySchema.parse(repo.getJsonState(copyKey(id))),
  }))];
  if (copies.some(copy => copy.scope.media !== scope.media || copy.scope.targetId !== scope.targetId || copy.selectedOfferKey !== selectedKey))
    throw new Error("Selected copies do not belong to this source and target");
  return [...new Map(copies.map(copy => [copy.offer.key, copy])).values()];
}
