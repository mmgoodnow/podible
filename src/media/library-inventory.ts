import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { BooksRepo } from "../repo";

const count = z.number().int().nonnegative().safe();
const bytes = z.object({
  logicalBytes: count.nullable(), onDiskBytes: count.nullable(),
  knownLogicalBytes: count, knownOnDiskBytes: count, physicalObjects: count,
  missingFiles: count, partialFiles: count, unreadableFiles: count,
});
const category = bytes.extend({ count, unavailableCount: count, unknownCount: count });
export const libraryInventorySchema = z.object({
  observedAt: z.iso.datetime(), complete: z.boolean(), totals: bytes,
  categories: z.object({ books: category, ebooks: category, audiobooks: category, movies: category,
    tvShows: category, seasons: category, episodes: category }),
});
export type LibraryInventory = z.infer<typeof libraryInventorySchema>;
type Category = keyof LibraryInventory["categories"];
const categories: Category[] = ["books", "ebooks", "audiobooks", "movies", "tvShows", "seasons", "episodes"];
type File = { root: string; file: string; size: number };
type Unit = { category: Category; key: string; alternatives: string[][]; references: string[] };
type Observation = { state: "complete" | "missing" | "partial" | "unknown"; identity?: string; size?: number; allocated?: number };

/** Published references only. No tree walks, torrent calls, ownership adoption, or writes. */
export async function readLibraryInventory(repo: BooksRepo): Promise<LibraryInventory> {
  const settings = repo.getSettings();
  const files = new Map<string, File>(), units = new Map<string, Unit>();
  const addFile = (root: string, file: string, size: number) => {
    const key = JSON.stringify([root, file, size]);
    files.set(key, { root, file, size }); return key;
  };
  const addUnit = (category: Category, key: string, alternatives: string[][], references = alternatives.flat()) => {
    const id = `${category}:${key}`, unit = units.get(id) ?? { category, key, alternatives: [], references: [] };
    unit.alternatives.push(...alternatives); unit.references.push(...references); units.set(id, unit);
  };
  const bookRoot = settings.libraryRoot ? path.join(path.resolve(settings.libraryRoot), ".podible", "content") : "";
  for (const book of repo.listAllBooks()) {
    for (const manifestation of repo.listManifestationsByBook(book.id)) {
      if (manifestation.preferred_score < 0) continue; // Staged, not approved for playback.
      const containers = repo.listAssetsByManifestation(manifestation.id);
      if (!containers.length) continue;
      const members = containers.flatMap(asset => repo.getAssetFiles(asset.id));
      const refs = members.map(file => addFile(bookRoot, file.path, file.size));
      // An empty member container must not make a multipart manifestation look complete.
      const alternatives = containers.every(asset => members.some(file => file.asset_id === asset.id)) ? [refs] : [[]];
      addUnit(manifestation.kind === "audio" ? "audiobooks" : "ebooks", String(book.id), alternatives, refs);
      addUnit("books", String(book.id), alternatives, refs);
    }
  }
  const movieRoot = settings.movies.managedRoot ? path.join(path.resolve(settings.movies.managedRoot), "objects") : "";
  for (const target of repo.movies.targets()) {
    if (!target.active_version_id) continue;
    const version = repo.movies.version(target.active_version_id);
    addUnit("movies", String(target.movie_id), [version?.files.map(file => addFile(movieRoot,
      path.resolve(movieRoot, file.relativePath), file.sizeBytes)) ?? []]);
  }
  const tvRoot = settings.tv.managedRoot ? path.join(path.resolve(settings.tv.managedRoot), "objects") : "";
  const versions = new Map<number, ReturnType<BooksRepo["tv"]["version"]>>();
  for (const target of repo.tv.targets()) {
    const active = repo.tv.activeEpisodes(target.id);
    for (const episode of active) {
      if (!versions.has(episode.version_id)) versions.set(episode.version_id, repo.tv.version(episode.version_id));
      const version = versions.get(episode.version_id);
      const refs = version?.files.filter(file => file.episodeIds.includes(episode.episode_id))
        .map(file => addFile(tvRoot, path.resolve(tvRoot, file.relativePath), file.sizeBytes)) ?? [];
      const identity = repo.tv.episode(episode.episode_id, target.entry_id);
      addUnit("episodes", String(episode.episode_id), [refs]);
      addUnit("tvShows", String(target.entry_id), [refs]);
      const seasons = new Set(identity?.numbering.filter(n => n.scheme === "tvdb:aired" && n.season !== null).map(n => n.season!) ?? []);
      for (const season of seasons) addUnit("seasons", `${target.entry_id}:${season}`, [refs]);
    }
  }

  const roots = new Map<string, Promise<string>>();
  const rootPath = (root: string) => {
    let value = roots.get(root);
    if (!value) { value = root ? fs.realpath(root) : Promise.reject(new Error("Root unconfigured")); roots.set(root, value); }
    return value;
  };
  const inside = (root: string, file: string) => file.startsWith(root + path.sep);
  const observations = new Map<string, Observation>();
  const pending = [...files.entries()];
  let next = 0;
  // Bound filesystem concurrency independently of catalog size; each registered path is inspected once.
  await Promise.all(Array.from({ length: Math.min(8, pending.length) }, async () => {
    while (next < pending.length) {
      const [key, file] = pending[next++]!;
      try {
        const root = await rootPath(file.root), original = path.resolve(file.file);
        const requestedRoot = path.resolve(file.root);
        const relative = inside(requestedRoot, original) ? path.relative(requestedRoot, original)
          : inside(root, original) ? path.relative(root, original) : null;
        if (relative === null || !Number.isSafeInteger(file.size) || file.size < 0) throw new Error("Invalid canonical reference");
        const actual = path.resolve(root, relative);
        if (!inside(root, actual) || await fs.realpath(path.dirname(actual)) !== path.dirname(actual)) throw new Error("Canonical parent traverses a symlink");
        const stat = await fs.lstat(actual, { bigint: true });
        if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("Canonical object is not a regular file");
        const size = Number(stat.size), allocated = Number(stat.blocks * 512n);
        if (!Number.isSafeInteger(size) || !Number.isSafeInteger(allocated) || size < 0 || allocated < 0) throw new Error("Unsafe byte count");
        observations.set(key, { state: size === file.size ? "complete" : "partial", identity: `${stat.dev}:${stat.ino}`, size, allocated });
      } catch (error) {
        observations.set(key, { state: (error as NodeJS.ErrnoException).code === "ENOENT" ? "missing" : "unknown" });
      }
    }
  }));
  const aggregate = (keys: Set<string>): z.infer<typeof bytes> => {
    const objects = new Map<string, Observation>();
    let missingFiles = 0, partialFiles = 0, unreadableFiles = 0;
    for (const key of keys) {
      const observed = observations.get(key)!;
      if (observed.state === "missing") missingFiles++;
      if (observed.state === "partial") partialFiles++;
      if (observed.state === "unknown") unreadableFiles++;
      if (observed.identity) objects.set(observed.identity, observed);
    }
    const knownLogicalBytes = [...objects.values()].reduce((sum, file) => sum + file.size!, 0);
    const knownOnDiskBytes = [...objects.values()].reduce((sum, file) => sum + file.allocated!, 0);
    const complete = missingFiles + partialFiles + unreadableFiles === 0;
    return { logicalBytes: complete ? knownLogicalBytes : null, onDiskBytes: complete ? knownOnDiskBytes : null,
      knownLogicalBytes, knownOnDiskBytes, physicalObjects: objects.size, missingFiles, partialFiles, unreadableFiles };
  };
  const result = {} as LibraryInventory["categories"];
  for (const name of categories) {
    const members = [...units.values()].filter(unit => unit.category === name);
    let count = 0, unavailableCount = 0, unknownCount = 0;
    for (const unit of members) {
      if (unit.alternatives.some(refs => refs.length && refs.every(key => observations.get(key)!.state === "complete"))) count++;
      else if (unit.alternatives.some(refs => !refs.length || refs.some(key => observations.get(key)!.state === "unknown"))) unknownCount++;
      else unavailableCount++;
    }
    const measured = aggregate(new Set(members.flatMap(unit => unit.references)));
    result[name] = { count, unavailableCount, unknownCount, ...measured,
      ...(unknownCount ? { logicalBytes: null, onDiskBytes: null } : {}) };
  }
  const totals = aggregate(new Set(files.keys()));
  if (Object.values(result).some(value => value.unknownCount)) { totals.logicalBytes = null; totals.onDiskBytes = null; }
  return libraryInventorySchema.parse({ observedAt: new Date().toISOString(), complete:
    totals.missingFiles + totals.partialFiles + totals.unreadableFiles === 0 &&
    Object.values(result).every(value => value.unknownCount === 0), totals, categories: result });
}
