import { mkdir } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

const root = path.resolve(import.meta.dir, "../..");
const artifacts = process.env.LAB_INTERNAL
  ? "/artifacts"
  : path.join(root, "tmp/movies-lab");
await mkdir(artifacts, { recursive: true });
async function command(args: string[]) {
  const process = Bun.spawn(args, {
    cwd: root,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  if (code) throw new Error(`${args[0]} failed (${code}): ${stderr}`);
  return stdout.trim();
}
if (!process.env.LAB_INTERNAL) {
  await command([
    "ffmpeg",
    "-y",
    "-f",
    "lavfi",
    "-i",
    "testsrc=size=640x360:rate=24",
    "-f",
    "lavfi",
    "-i",
    "sine=frequency=440",
    "-t",
    "12",
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    path.join(artifacts, "fixture.mp4"),
  ]);
  await command([
    "docker",
    "compose",
    "-f",
    "test/lab/compose.yml",
    "up",
    "-d",
    "plex",
    "rtorrent",
    "prowlarr",
  ]);
  console.log(
    await command([
      "docker",
      "run",
      "--rm",
      "--network",
      "container:podible-movies-lab-plex-1",
      "-e",
      "LAB_INTERNAL=1",
      "-v",
      "podible-movies-lab-content:/data",
      "-v",
      `${path.join(root, "src/media/library-view.ts")}:/app/src/media/library-view.ts:ro`,
      "-v",
      `${path.join(root, "test/lab")}:/app/test/lab:ro`,
      "-v",
      `${path.join(artifacts, "fixture.mp4")}:/fixture.mp4:ro`,
      "-v",
      `${artifacts}:/artifacts`,
      "podible-movies-poc:local",
      "bun",
      "test/lab/plex-acceptance.ts",
    ]),
  );
  process.exit(0);
}
const base = "http://127.0.0.1:32400";
async function api(route: string, method = "GET") {
  const response = await fetch(base + route, {
    method,
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok)
    throw new Error(
      `Plex ${method} ${route.split("?")[0]}: ${response.status}`,
    );
  return response;
}
async function until<T>(
  read: () => Promise<T | null>,
  label: string,
): Promise<T> {
  const deadline = Date.now() + 60_000;
  do {
    try {
      const value = await read();
      if (value !== null) return value;
    } catch {}
    await Bun.sleep(1000);
  } while (Date.now() < deadline);
  throw new Error(`Timed out: ${label}`);
}
await until(async () => {
  await api("/identity");
  return true;
}, "Plex startup");
async function seed(mode: string) {
  return command([process.execPath, "run", "test/lab/seed-view.ts", mode]);
}
await seed("initial");
const sectionsSchema = z.object({
  MediaContainer: z.object({
    Directory: z
      .array(
        z.object({
          key: z.string(),
          title: z.string(),
          refreshing: z.boolean().optional(),
        }),
      )
      .optional(),
  }),
});
const sections = sectionsSchema.parse(
  await (await api("/library/sections")).json(),
);
let section = sections.MediaContainer.Directory?.find(
  (entry) => entry.title === "Podible Lab",
);
if (!section) {
  const params = new URLSearchParams({
    name: "Podible Lab",
    type: "movie",
    agent: "com.plexapp.agents.none",
    scanner: "Plex Video Files Scanner",
    language: "xn",
    location: "/data/views/plex/movies",
  });
  await api(`/library/sections?${params}`, "POST");
  section = sectionsSchema
    .parse(await (await api("/library/sections")).json())
    .MediaContainer.Directory?.find((entry) => entry.title === "Podible Lab");
}
if (!section) throw new Error("Plex did not create the isolated library");
const itemSchema = z.object({
  ratingKey: z.string(),
  viewCount: z.number().optional(),
  Media: z.array(
    z.object({
      Part: z.array(z.object({ file: z.string(), key: z.string() })),
    }),
  ),
});
const contentsSchema = z.object({
  MediaContainer: z.object({ Metadata: z.array(itemSchema).optional() }),
});
async function findItem(expected: string) {
  const contents = contentsSchema.parse(
    await (await api(`/library/sections/${section!.key}/all`)).json(),
  );
  return (
    contents.MediaContainer.Metadata?.find((item) =>
      item.Media.some((media) =>
        media.Part.some((part) => part.file === expected),
      ),
    ) ?? null
  );
}
async function idle() {
  return until(async () => {
    const current = sectionsSchema
      .parse(await (await api("/library/sections")).json())
      .MediaContainer.Directory?.find((entry) => entry.key === section!.key);
    return current?.refreshing === false ? true : null;
  }, "Plex scan completion");
}
await idle();
await api(`/library/sections/${section.key}/refresh`);
const firstPath =
  "/data/views/plex/movies/Lab Movie (2020)/Lab Movie (2020).mp4";
const first = await until(() => findItem(firstPath), "symlink indexing");
await idle();
const part = first.Media.flatMap((media) => media.Part).find(
  (part) => part.file === firstPath,
)!;
const media = await fetch(base + part.key, {
  headers: { Range: "bytes=0-31" },
  signal: AbortSignal.timeout(5000),
});
if (media.status !== 206 || (await media.arrayBuffer()).byteLength !== 32)
  throw new Error("Plex cannot serve byte ranges through the symlink");
await api(
  `/:/scrobble?key=${first.ratingKey}&identifier=com.plexapp.plugins.library`,
);
await seed("rename");
await api(`/library/sections/${section.key}/refresh`);
let lastScanRequest = Date.now();
const renamed = await until(async () => {
  const found = await findItem(
    "/data/views/plex/movies/Lab Movie (2020) [local-42]/Lab Movie (2020).mp4",
  );
  if (!found && Date.now() - lastScanRequest > 5_000) {
    await idle();
    await api(`/library/sections/${section!.key}/refresh`);
    lastScanRequest = Date.now();
  }
  return found;
}, "renamed view indexing");
const report = {
  checkedAt: new Date().toISOString(),
  symlinkIndexed: true,
  byteRangePlayback: true,
  renameIndexed: true,
  sameRatingKey: renamed.ratingKey === first.ratingKey,
  watchedStatePreserved: (renamed.viewCount ?? 0) > 0,
  metadataMode: "personal-video fixture; real movie matching not proven",
};
await Bun.write(
  path.join(artifacts, "plex-acceptance.json"),
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report, null, 2));
if (!report.sameRatingKey || !report.watchedStatePreserved)
  throw new Error(
    "Plex view rename did not preserve item identity/watch state; publication protocol needs refinement",
  );
