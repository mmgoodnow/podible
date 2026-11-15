import { chmod, mkdtemp, mkdir, rm, symlink, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { watchPlexCompletions } from "../../src/runtime/plex-completion";
import { exactLatencyFixture, fixtureTitle, startLatencyMetadata } from "./plex-latency-metadata";

const root = path.resolve(import.meta.dir, "../..");
const reps = Number(process.argv.find(arg => arg.startsWith("--runs="))?.split("=")[1] ?? 1);
const media = process.argv.find(arg => arg.startsWith("--media="))?.split("=")[1] ?? "movie";
const scan = process.argv.includes("--directory") ? "directory" : "section";
const exact = process.argv.includes("--exact-identities");
const analysisMode = process.argv.find(arg => arg.startsWith("--analysis="))?.slice("--analysis=".length);
const observationMs = Number(process.argv.find(arg => arg.startsWith("--observe-ms="))?.split("=")[1] ?? (exact ? 90_000 : 12_000));
const podibleImage = process.argv.find(arg => arg.startsWith("--podible-image="))?.slice("--podible-image=".length) ?? "podible-movies-poc:local";
if (!Number.isInteger(reps) || reps < 1 || reps > 20) throw new Error("--runs must be between 1 and 20");
if (media !== "movie" && media !== "tv") throw new Error("--media must be movie or tv");
if (exact && reps !== 1) throw new Error("Run each exact-identity trial in a fresh lab");
if (!Number.isInteger(observationMs) || observationMs < 1_000 || observationMs > 120_000) throw new Error("Invalid observation window");
if (analysisMode && analysisMode !== "asap" && analysisMode !== "scheduled") throw new Error("Invalid analysis mode");

async function run(args: string[]) {
  const child = Bun.spawn(args, { cwd: root, stdout: "inherit", stderr: "inherit" });
  const status = await child.exited;
  if (status !== 0) throw new Error(`${args[0]} exited with status ${status}`);
}

if (!process.argv.includes("--inside-lab")) {
  const files = await mkdtemp(path.join(tmpdir(), "podible-plex-latency-"));
  const name = path.basename(files).toLowerCase();
  let started = false;
  try {
    await run(["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
      "-i", "color=c=0x445f68:s=320x180:r=24", "-t", "3", "-c:v", "libx264",
      "-pix_fmt", "yuv420p", path.join(files, "fixture.mp4")]);
    await chmod(files, 0o755);
    const config = Bun.spawnSync(["docker", "compose", "-f", "test/lab/compose.yml", "config", "--format", "json"], { cwd: root });
    if (config.exitCode !== 0) throw new Error("Could not load the Plex lab image configuration");
    const image = (JSON.parse(config.stdout.toString()) as { services: { plex: { image: string } } }).services.plex.image;
    await run(["docker", "run", "-d", "--rm", "--pull", "never", "--name", name, "--network", "none",
      "--cpus", "2", "--memory", "2g", "-v", `${files}:/data:ro`, image]);
    started = true;
    await run(["docker", "run", "--rm", "--pull", "never", "--network", `container:${name}`,
      "--cpus", "1", "--memory", "512m", "-e", "PODIBLE_PLEX_LATENCY_LAB=1", "-v", `${files}:/data`,
      "-v", `${root}/test/lab:/app/test/lab:ro`, "-v", `${root}/src:/app/src:ro`,
      "--entrypoint", "bun", podibleImage,
      "run", "test/lab/plex-latency.ts", "--inside-lab", `--runs=${reps}`, `--media=${media}`,
      `--observe-ms=${observationMs}`,
      ...(exact ? ["--exact-identities"] : []),
      ...(analysisMode ? [`--analysis=${analysisMode}`] : []),
      ...(scan === "directory" ? ["--directory"] : [])]);
  } finally {
    try { if (started) await run(["docker", "stop", name]); }
    finally { await rm(files, { recursive: true, force: true }); }
  }
  process.exit(0);
}

// Only the private, disposable Plex lab is allowed here.
if (process.env.PODIBLE_PLEX_LATENCY_LAB !== "1") throw new Error("Use the Docker lab wrapper");
const base = "http://127.0.0.1:32400";
async function request(route: string, method = "GET") {
  const response = await fetch(base + route, { method, redirect: "error", signal: AbortSignal.timeout(30_000),
    headers: { Accept: "application/json" } }).catch(error => {
    throw new Error(`Plex lab ${method} ${route.split("?")[0]} did not respond`, { cause: error });
  });
  if (!response.ok) throw new Error(`Plex lab ${method} ${route.split("?")[0]} returned ${response.status}: ${(await response.text()).slice(0, 500)}`);
  return response;
}
type Section = { key: string; title: string; refreshing?: boolean };
async function waitUntil<T>(read: () => Promise<T | null>, label: string, ms = 30_000): Promise<T> {
  const deadline = performance.now() + ms;
  do {
    const value = await read();
    if (value !== null) return value;
    await Bun.sleep(200);
  } while (performance.now() < deadline);
  throw new Error(`Timed out waiting for ${label}`);
}
async function listSections() {
  const listing = await (await request("/library/sections")).json() as { MediaContainer: { Directory?: Section[] } };
  return listing.MediaContainer.Directory ?? [];
}
const existing = await waitUntil(async () => {
  try { return await listSections(); }
  catch { return null; }
}, "isolated Plex startup", 60_000);
if (existing.length !== 0) throw new Error("Refusing to benchmark a nonempty Plex instance");
if (analysisMode) {
  const keys = ["GenerateBIFBehavior", "GenerateIntroMarkerBehavior", "GenerateCreditsMarkerBehavior", "GenerateAdMarkerBehavior", "GenerateChapterThumbBehavior"];
  const prefs = new URLSearchParams();
  for (const key of keys) prefs.set(key, analysisMode);
  await (await request(`/:/prefs?${prefs}`, "PUT")).body?.cancel();
  const { MediaContainer } = await (await request("/:/prefs")).json() as { MediaContainer: { Setting: { id: string; value: string }[] } };
  for (const key of keys) {
    const applied = MediaContainer.Setting.find(pref => pref.id === key)?.value;
    if (applied !== analysisMode) throw new Error(`Plex did not apply the isolated ${key} preference (read ${applied ?? "unavailable"})`);
  }
}

const lab = await mkdtemp("/data/latency-");
let verifyOnWake: ((source: string, prefix: boolean) => Promise<void>) | undefined;
let opened = false;
const watcher = watchPlexCompletions(async (source, prefix) => {
  if (prefix) opened = true;
  await verifyOnWake?.(source, prefix === true);
}, () => { opened = false; });

type Item = { ratingKey: string; parentIndex?: number; index?: number; Media?: { Part?: { file: string }[] }[] };
async function listItems(route: string) {
  const result = await (await request(route)).json() as { MediaContainer: { Metadata?: Item[] } };
  return result.MediaContainer.Metadata ?? [];
}
async function listedPaths(section: Section, tv: boolean) {
  const metadata = await listItems(`/library/sections/${section.key}/all${tv ? "?type=2" : ""}`);
  const items = tv ? (await Promise.all(metadata.map(show =>
    listItems(`/library/metadata/${encodeURIComponent(show.ratingKey)}/allLeaves`)))).flat() : metadata;
  return new Map(items.flatMap(item =>
    (item.Media ?? []).flatMap(media => (media.Part ?? []).map(part => [part.file, item] as const))));
}
type Probe = { media: "movie" | "tv"; scan: "section" | "directory"; files: number;
  indexedPaths: number; correctCoordinates: number; exactVerificationMs: number | null;
  refreshMs: number; indexedMs: number | null; exactVerificationOnEventMs: number | null; refreshingWhenIndexed: boolean | null;
  firstMatchMs: number | null; firstMetadataMs: number | null };
const probes: Probe[] = [];
let metadata: Awaited<ReturnType<typeof startLatencyMetadata>> | undefined;

try {
  await chmod(lab, 0o755); // Plex scans as a different UID than the lab runner.
  // The lab allows loopback access without a Plex credential. The watcher only
  // needs a non-empty token to connect; the stub is not a real credential.
  watcher.configure({ url: base, token: "local-lab" });
  await waitUntil(async () => opened ? true : null, "the Plex activity stream", 10_000);
  const tv = media === "tv";
  const count = tv ? 12 : 1;
  for (let i = 0; i < reps; i++) {
      const view = path.join(lab, `${media}-${i + 1}`);
      await mkdir(view);
      const title = `Podible timing ${path.basename(lab)} ${path.basename(view)}`;
      const params = new URLSearchParams({ name: title, type: tv ? "show" : "movie", language: tv || exact ? "en-US" : "xn",
        agent: tv ? "tv.plex.agents.series" : exact ? "tv.plex.agents.movie" : "com.plexapp.agents.none",
        scanner: tv ? "Plex TV Series" : exact ? "Plex Movie" : "Plex Video Files Scanner", location: view });
      if (exact) {
        metadata = await startLatencyMetadata(tv, count, request);
        params.set("agent", metadata.identifier);
        params.set("metadataAgentProviderGroupId", metadata.groupId);
      }
      await waitUntil(async () => {
        try { await (await request(`/library/sections?${params}`, "POST")).body?.cancel(); return true; }
        catch (error) {
          if (error instanceof Error && error.message.includes("the server is still starting up")) return null;
          throw error;
        }
      }, "Plex scanner registration", 60_000);
      const section = await waitUntil(async () => (await listSections()).find(item => item.title === title) ?? null, "fixture library");
      const label = exact ? `${fixtureTitle} (2024)` : `Timing Fixture ${i + 1} ${scan}`;
      const directory = path.join(view, label);
      const season = tv ? path.join(directory, "Season 01") : directory;
      await mkdir(season, { recursive: true });
      const paths: string[] = [];
      for (let n = 1; n <= count; n++) {
        const file = path.join(season, `${label}${tv ? ` - S01E${String(n).padStart(2, "0")}` : ""}.mp4`);
        const payload = path.join(lab, `object-${i}-${n}.mp4`);
        await copyFile("/data/fixture.mp4", payload);
        await symlink(path.relative(season, payload), file);
        paths.push(file);
      }
      const started = performance.now();
      const verify = exact ? exactLatencyFixture(tv, base, section.key, paths) : undefined;
      let exactVerificationOnEventMs: number | null = null;
      if (verify) verifyOnWake = async (source, prefix) => {
        if (!prefix && source !== `plex:${section.key}`) return;
        if (exactVerificationOnEventMs === null && await verify("correct"))
          exactVerificationOnEventMs = performance.now() - started;
      };
      const query = scan === "directory" ? `?${new URLSearchParams({ path: directory })}` : "";
      await (await request(`/library/sections/${section.key}/refresh${query}`)).body?.cancel();
      const refreshMs = performance.now() - started;
      let indexedMs: number | null = null, refreshingWhenIndexed: boolean | null = null;
      let exactVerificationMs: number | null = null;
      let indexedPaths = 0, correctCoordinates = 0;
      while (performance.now() - started < observationMs) {
        if (indexedMs === null) {
          const seen = await listedPaths(section, tv);
          indexedPaths = paths.filter(file => seen.has(file)).length;
          correctCoordinates = paths.filter((file, n) => seen.has(file) && (!tv || (seen.get(file)?.parentIndex === 1 && seen.get(file)?.index === n + 1))).length;
          if (correctCoordinates === paths.length) {
            indexedMs = performance.now() - started;
            refreshingWhenIndexed = (await listSections()).find(item => item.key === section.key)?.refreshing ?? null;
          }
        }
        if (verify && indexedMs !== null && exactVerificationMs === null && await verify("correct")) {
          exactVerificationMs = performance.now() - started;
          if (await verify("wrong-identity")) throw new Error("The verifier accepted a wrong identity at the right path");
          if (await verify("wrong-path")) throw new Error("The verifier accepted a correct identity at the wrong path");
        }
        const ready = verify ? exactVerificationMs : indexedMs;
        // Metadata can be readable well before Plex emits the terminal timeline event.
        if (ready !== null && (!verify || exactVerificationOnEventMs !== null)) break;
        await Bun.sleep(200);
      }
      verifyOnWake = undefined;
      const sinceScan = (at: number | null | undefined) => at != null && at >= started ? at - started : null;
      const probe: Probe = { media, scan, files: paths.length, indexedPaths, correctCoordinates, refreshMs, indexedMs, exactVerificationOnEventMs,
        refreshingWhenIndexed, exactVerificationMs, firstMatchMs: sinceScan(metadata?.firstMatchAt), firstMetadataMs: sinceScan(metadata?.firstMetadataAt) };
      probes.push(probe);
      console.log(JSON.stringify(probe));
  }
  console.log(JSON.stringify({ kind: "plex-lab-publication", analysisMode: analysisMode ?? "default", observationIntervalMs: 200,
    metadataRequests: metadata?.requests, probes }, null, 2));
  if (exact && probes.some(probe => probe.exactVerificationMs === null)) throw new Error("Plex did not verify the exact fixture identities");
  if (exact && probes.some(probe => probe.exactVerificationOnEventMs === null)) throw new Error("No Plex event verified the exact fixture identities");
  if (probes.some(probe => probe.scan === "section" && probe.indexedMs === null))
    throw new Error("A full-section scan did not index all synthetic fixtures");
} finally {
  await watcher.stop();
  await metadata?.stop();
  await rm(lab, { recursive: true, force: true });
}
