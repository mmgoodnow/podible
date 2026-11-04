import { randomUUID } from "node:crypto";
import { promises as fs, type BigIntStats } from "node:fs";
import path from "node:path";
import { z } from "zod";

const contentMarker = ".podible-content.json";
const manifestName = ".podible-view.json";
const pendingName = ".podible-view-pending.json";
const manifestSchema = z
  .object({
    contentId: z.string().uuid(),
    files: z.record(z.string(), z.array(z.string())),
  })
  .strict();
const adoptionSchema = z
  .object({
    source: z.string(),
    dev: z.string().regex(/^\d+$/),
    ino: z.string().regex(/^\d+$/),
    size: z.string().regex(/^\d+$/),
  })
  .strict();
const pendingSchema = manifestSchema.extend({
  adoptions: z.record(z.string(), adoptionSchema).optional(),
});
type Adoption = z.infer<typeof adoptionSchema>;
type FileIdentity = Pick<Adoption, "dev" | "ino" | "size">;
export type LibraryViewEntry = { relativePath: string; contentPath: string };

function identity(file: BigIntStats): FileIdentity {
  return {
    dev: file.dev.toString(),
    ino: file.ino.toString(),
    size: file.size.toString(),
  };
}

function matchesIdentity(file: FileIdentity, expected: FileIdentity) {
  return (
    file.dev === expected.dev &&
    file.ino === expected.ino &&
    file.size === expected.size
  );
}

function contained(root: string, relative: string): string {
  if (
    !relative ||
    path.isAbsolute(relative) ||
    relative.includes("\\") ||
    relative.includes("\0") ||
    relative
      .split("/")
      .some(
        (part) =>
          !part || part === "." || part === ".." || part.startsWith(".podible"),
      )
  ) {
    throw new Error(`Unsafe view path: ${relative}`);
  }
  const result = path.resolve(root, relative);
  if (!result.startsWith(`${path.resolve(root)}${path.sep}`))
    throw new Error("Path escaped managed root");
  return result;
}

async function stat(file: string) {
  try {
    return await fs.lstat(file, { bigint: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function directory(
  root: string,
  relative = "",
  assertActive?: () => void,
  create = true,
) {
  const parts = path.resolve(root).split(path.sep).filter(Boolean);
  if (relative) parts.push(...relative.split("/"));
  let current = path.parse(path.resolve(root)).root;
  for (const part of parts) {
    current = path.join(current, part);
    const existing = await stat(current);
    if (existing && (!existing.isDirectory() || existing.isSymbolicLink()))
      throw new Error(`Not a real directory: ${current}`);
    if (!existing && create) {
      assertActive?.();
      await fs.mkdir(current);
    }
  }
}

async function readJson(file: string): Promise<unknown | null> {
  const existing = await stat(file);
  if (!existing) return null;
  if (!existing.isFile() || existing.isSymbolicLink())
    throw new Error(`Invalid ownership record: ${file}`);
  return JSON.parse(await fs.readFile(file, "utf8"));
}

async function writeJson(
  file: string,
  value: unknown,
  assertActive?: () => void,
) {
  const temporary = `${file}-${randomUUID()}`;
  assertActive?.();
  const handle = await fs.open(temporary, "wx", 0o600);
  try {
    assertActive?.();
    await handle.writeFile(JSON.stringify(value));
    assertActive?.();
    await handle.sync();
  } finally {
    await handle.close();
  }
  assertActive?.();
  await fs.rename(temporary, file);
  const parent = await fs.open(path.dirname(file), "r");
  try {
    assertActive?.();
    await parent.sync();
  } finally {
    await parent.close();
  }
}

/** Explicit initialization only; never silently adopt a preexisting media tree. */
export async function initializeManagedContent(root: string) {
  await directory(root);
  const current = await readJson(path.join(root, contentMarker));
  if (current !== null)
    return z.object({ id: z.string().uuid() }).parse(current).id;
  if ((await fs.readdir(root)).length)
    throw new Error("Content root is not empty and has no ownership marker");
  const id = randomUUID();
  await fs.writeFile(path.join(root, contentMarker), JSON.stringify({ id }), {
    flag: "wx",
  });
  return id;
}

/** Callers serialize reconciliation for a view root. No target bytes are modified. */
export async function reconcileLibraryView(input: {
  contentRoot: string;
  viewRoot: string;
  entries: LibraryViewEntry[];
  /** Exact legacy hardlinks whose canonical content and DB cutover the caller verified. */
  adoptRegularFiles?: LibraryViewEntry[];
  assertActive?: () => void;
  onLinkApplied?: (relativePath: string) => void | Promise<void>;
}) {
  input.assertActive?.();
  const contentRoot = path.resolve(input.contentRoot);
  const viewRoot = path.resolve(input.viewRoot);
  const content = z
    .object({ id: z.string().uuid() })
    .parse(await readJson(path.join(contentRoot, contentMarker)));
  if (
    contentRoot === viewRoot ||
    (contentRoot.startsWith(path.join(viewRoot, path.sep)) &&
      path.relative(viewRoot, contentRoot).split(path.sep)[0] !== ".podible") ||
    viewRoot.startsWith(path.join(contentRoot, path.sep))
  )
    throw new Error("Content and view roots must be separate");
  // Verify the mounted root, not merely that an empty mountpoint directory exists.
  if ((await fs.realpath(contentRoot)) !== contentRoot)
    throw new Error("Content root traverses a symlink");
  await directory(viewRoot, "", input.assertActive);
  const previous = manifestSchema.parse(
    (await readJson(path.join(viewRoot, manifestName))) ?? {
      contentId: content.id,
      files: {},
    },
  );
  const pendingRaw = await readJson(path.join(viewRoot, pendingName));
  const pending = pendingRaw === null ? null : pendingSchema.parse(pendingRaw);
  if (
    previous.contentId !== content.id ||
    (pending && pending.contentId !== content.id)
  )
    throw new Error("Managed content mount identity changed");
  const canonicalSource = async (contentPath: string) => {
    const sourceRelative = path.relative(contentRoot, path.resolve(contentPath));
    const source = contained(contentRoot, sourceRelative);
    const existing = await stat(source);
    if ((await fs.realpath(path.dirname(source))) !== path.dirname(source) || !existing?.isFile())
      throw new Error("Source must be a canonical regular file");
    return { source: sourceRelative, ...identity(existing) };
  };
  const files: Record<string, string> = Object.create(null);
  for (const entry of input.entries) {
    contained(viewRoot, entry.relativePath);
    if (Object.hasOwn(files, entry.relativePath))
      throw new Error(`Duplicate view path: ${entry.relativePath}`);
    files[entry.relativePath] = (await canonicalSource(entry.contentPath)).source;
  }
  const adoptions: Record<string, Adoption> = Object.assign(
    Object.create(null),
    pending?.adoptions,
  );
  const validateAdoption = async (adoption: Adoption) => {
    const source = await canonicalSource(contained(contentRoot, adoption.source));
    if (!matchesIdentity(source, adoption))
      throw new Error("Adopted canonical file identity changed");
  };
  for (const adoption of Object.values(adoptions))
    await validateAdoption(adoption);
  const requestedAdoptions = new Set<string>();
  for (const entry of input.adoptRegularFiles ?? []) {
    contained(viewRoot, entry.relativePath);
    if (requestedAdoptions.has(entry.relativePath))
      throw new Error(`Duplicate adoption path: ${entry.relativePath}`);
    requestedAdoptions.add(entry.relativePath);
    const adoption = await canonicalSource(entry.contentPath);
    const recorded = adoptions[entry.relativePath];
    if (
      recorded &&
      (recorded.source !== adoption.source || !matchesIdentity(recorded, adoption))
    )
      throw new Error(`Adoption identity changed: ${entry.relativePath}`);
    adoptions[entry.relativePath] = adoption;
  }
  const knownPaths = new Set([
    ...Object.keys(previous.files),
    ...Object.keys(pending?.files ?? {}),
    ...Object.keys(files),
    ...Object.keys(adoptions),
  ]);
  const seen = new Set<string>();
  for (const relative of knownPaths) {
    contained(viewRoot, relative);
    const key = relative.normalize("NFC").toLowerCase();
    if (seen.has(key)) throw new Error(`Duplicate view path: ${relative}`);
    seen.add(key);
  }
  for (const key of seen) {
    const parts = key.split("/");
    for (let count = 1; count < parts.length; count++) {
      if (seen.has(parts.slice(0, count).join("/")))
        throw new Error(`Conflicting view paths: ${key}`);
    }
  }
  const observed = new Map<string, string | FileIdentity | null>();
  // Preflight every path before changing any link, including retired entries.
  for (const relative of knownPaths) {
    const destination = contained(viewRoot, relative);
    const parent = path.posix.dirname(relative);
    if (parent !== ".")
      await directory(viewRoot, parent, input.assertActive, false);
    const existing = await stat(destination);
    let target: string | FileIdentity | null = null;
    if (existing) {
      if (existing.isSymbolicLink()) {
        target = path.resolve(
          path.dirname(destination),
          await fs.readlink(destination),
        );
        // Adoption grants regular-file retirement, never symlink ownership.
        const authorized = [
          ...(previous.files[relative] ?? []),
          ...(pending?.files[relative] ?? []),
        ].map((value) => contained(contentRoot, value));
        if (!authorized.includes(target))
          throw new Error(`Foreign or changed symlink: ${relative}`);
      } else {
        const adoption = adoptions[relative];
        if (!existing.isFile() || !adoption)
          throw new Error(`Refusing to replace a regular file: ${relative}`);
        target = identity(existing);
        if (!matchesIdentity(target, adoption))
          throw new Error(
            `Adopted regular file does not match canonical source: ${relative}`,
          );
      }
    }
    observed.set(relative, target);
  }
  const owned = Object.fromEntries(
    [...knownPaths].map((relative) => [
      relative,
      [
        ...new Set([
          ...(previous.files[relative] ?? []),
          ...(pending?.files[relative] ?? []),
          ...(files[relative] === undefined ? [] : [files[relative]]),
        ]),
      ],
    ]),
  );
  await writeJson(
    path.join(viewRoot, pendingName),
    {
      contentId: content.id,
      files: owned,
      ...(Object.keys(adoptions).length ? { adoptions } : {}),
    },
    input.assertActive,
  );
  const assertUnchanged = async (relative: string) => {
    const destination = contained(viewRoot, relative);
    const parent = path.posix.dirname(relative);
    if (parent !== ".")
      await directory(viewRoot, parent, input.assertActive, false);
    const current = await stat(destination);
    const expected = observed.get(relative);
    if (!current && expected === null) return;
    if (expected && typeof expected !== "string") {
      if (current?.isFile() && matchesIdentity(identity(current), expected)) {
        await validateAdoption(adoptions[relative]);
        return;
      }
      throw new Error(`View changed during reconciliation: ${relative}`);
    }
    if (
      !current?.isSymbolicLink() ||
      path.resolve(
        path.dirname(destination),
        await fs.readlink(destination),
      ) !== expected
    ) {
      throw new Error(`View changed during reconciliation: ${relative}`);
    }
  };
  let linked = 0;
  let removed = 0;
  for (const [relative, sourceRelative] of Object.entries(files)) {
    input.assertActive?.();
    const destination = contained(viewRoot, relative);
    const target = contained(contentRoot, sourceRelative);
    if (observed.get(relative) === target) continue;
    const temporary = path.join(
      path.dirname(destination),
      `.podible-link-${randomUUID()}`,
    );
    const parent = path.posix.dirname(relative);
    if (parent !== ".") await directory(viewRoot, parent, input.assertActive);
    input.assertActive?.();
    await fs.symlink(
      path.relative(path.dirname(destination), target),
      temporary,
    );
    await assertUnchanged(relative);
    input.assertActive?.();
    await fs.rename(temporary, destination);
    linked++;
    await input.onLinkApplied?.(relative);
  }
  for (const relative of knownPaths) {
    input.assertActive?.();
    if (files[relative] !== undefined || observed.get(relative) === null)
      continue;
    await assertUnchanged(relative);
    input.assertActive?.();
    await fs.unlink(contained(viewRoot, relative));
    removed++;
  }
  await writeJson(
    path.join(viewRoot, manifestName),
    {
      contentId: content.id,
      files: Object.fromEntries(
        Object.entries(files).map(([key, value]) => [key, [value]]),
      ),
    },
    input.assertActive,
  );
  input.assertActive?.();
  await fs.unlink(path.join(viewRoot, pendingName));
  return { linked, removed, unchanged: Object.keys(files).length - linked };
}
