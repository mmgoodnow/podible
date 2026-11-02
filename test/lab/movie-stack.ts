import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { chromium } from "playwright";
import { OpenAIProvider, setTracingDisabled, withTrace } from "@openai/agents";
import type { ModelRequest } from "@openai/agents-core";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
setTracingDisabled(true);
const live = process.argv.includes("--live-agent");
if (live && !process.env.OPENAI_API_KEY)
  throw new Error("No authorized local OpenAI key configured");
const provider = live
  ? new OpenAIProvider({
      apiKey: process.env.OPENAI_API_KEY,
      useResponses: true,
    })
  : null;

const root = path.resolve(import.meta.dir, "../..");
const runnerName = `podible-movie-stack-${randomUUID()}`;
const artifacts = path.join(root, "tmp/movies-lab");
await fs.mkdir(artifacts, { recursive: true });
async function bounded<T>(action: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([action, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}
async function command(args: string[]) {
  const child = Bun.spawn(args, { cwd: root, stdout: "pipe", stderr: "pipe" });
  const [code, out, err] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code) throw new Error(`${args[0]} failed: ${err.slice(-3000)}`);
  return out;
}
for (const [name, dimensions] of [
  ["720", "1280x720"],
  ["1080", "1920x1080"],
])
  await command([
    "ffmpeg",
    "-y",
    "-f",
    "lavfi",
    "-i",
    `color=c=0x466b50:size=${dimensions}:rate=24`,
    "-f",
    "lavfi",
    "-i",
    "sine=frequency=440",
    "-t",
    "12",
    "-c:v",
    "libx264",
    "-vf",
    "setparams=color_primaries=bt709:color_trc=bt709:colorspace=bt709",
    "-preset",
    "ultrafast",
    "-pix_fmt",
    "yuv420p",
    "-color_trc",
    "bt709",
    "-colorspace",
    "bt709",
    "-color_primaries",
    "bt709",
    "-c:a",
    "aac",
    "-metadata:s:a:0",
    "language=eng",
    path.join(artifacts, `movie-${name}.mp4`),
  ]);
const colorProbe = JSON.parse(
  await command([
    "ffprobe",
    "-v",
    "error",
    "-show_entries",
    "stream=color_transfer",
    "-of",
    "json",
    path.join(artifacts, "movie-720.mp4"),
  ]),
);
if (
  !colorProbe.streams.some(
    (stream: { color_transfer?: string }) => stream.color_transfer === "bt709",
  )
)
  throw new Error("Generated movie fixture is missing explicit SDR metadata");
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
const child = Bun.spawn(
  [
    "docker",
    "run",
    "--rm",
    "-i",
    "--name",
    runnerName,
    ...(live ? ["-e", "POC_LIVE_AGENT=1"] : []),
    "--network",
    "container:podible-movies-lab-plex-1",
    "-v",
    "podible-movies-lab-content:/data",
    "-v",
    "podible-movies-lab_prowlarr-config:/prowlarr:ro",
    "-v",
    `${root}/src:/app/src:ro`,
    "-v",
    `${root}/test/lab:/app/test/lab:ro`,
    "-v",
    `${root}/web/dist:/app/web/dist:ro`,
    "-v",
    `${artifacts}:/fixtures:ro`,
    "podible-movies-poc:local",
    "bun",
    "test/lab/movie-stack-inner.ts",
  ],
  { stdin: "pipe", stdout: "pipe", stderr: "inherit" },
);
type Reply = {
  id: number;
  status: number;
  headers: Record<string, string>;
  body: string;
  error?: string;
};
const pending = new Map<number, (reply: Reply) => void>();
let stopped = false;
void child.exited.then(() => {
  stopped = true;
  for (const [id, resolve] of pending)
    resolve({ id, status: 502, headers: {}, body: "", error: "Lab runner exited" });
  pending.clear();
});
let readyResolve!: () => void;
const ready = new Promise<void>((resolve) => {
  readyResolve = resolve;
});
void (async () => {
  let buffer = "";
  for await (const chunk of child.stdout) {
    buffer += new TextDecoder().decode(chunk);
    for (;;) {
      const at = buffer.indexOf("\n");
      if (at < 0) break;
      const line = buffer.slice(0, at);
      buffer = buffer.slice(at + 1);
      const value = JSON.parse(line);
      if (value.ready) readyResolve();
      else if (value.kind === "model") {
        void (async () => {
          try {
            if (!provider || value.model !== TEXT_AGENT_MODEL)
              throw new Error("Unexpected lab model request");
            const model = await provider.getModel(TEXT_AGENT_MODEL);
            const response = await withTrace("Movie lab provider", () =>
              model.getResponse({
                ...(value.request as ModelRequest),
                tracing: false,
                signal: AbortSignal.timeout(55_000),
              }),
            );
            child.stdin.write(
              JSON.stringify({ kind: "model-result", id: value.id, response }) +
                "\n",
            );
          } catch (error) {
            const message = (error as Error).message.replaceAll(
              process.env.OPENAI_API_KEY ?? "no-key",
              "[REDACTED]",
            );
            child.stdin.write(
              JSON.stringify({
                kind: "model-result",
                id: value.id,
                error: message,
              }) + "\n",
            );
          }
        })();
      } else {
        pending.get(value.id)?.(value);
        pending.delete(value.id);
      }
    }
  }
})().catch((error) => console.error("Lab bridge failed", error));
let sequence = 0;
const proxy = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  idleTimeout: 120,
  async fetch(request) {
    if (stopped) return new Response("Lab runner exited", { status: 502 });
    const url = new URL(request.url);
    if (
      request.method === "POST" &&
      request.headers.has("origin") &&
      request.headers.get("origin") !== url.origin
    )
      return new Response("Forbidden", { status: 403 });
    const id = ++sequence;
    const result = new Promise<Reply>((resolve) => pending.set(id, resolve));
    child.stdin.write(
      JSON.stringify({
        id,
        path: url.pathname + url.search,
        method: request.method,
        headers: Object.fromEntries(request.headers),
        body: Buffer.from(await request.arrayBuffer()).toString("base64"),
      }) + "\n",
    );
    const reply = await bounded(result, 120_000, "Lab HTTP bridge timed out");
    if (reply.error) return new Response(reply.error, { status: 502 });
    const headers = new Headers(reply.headers);
    headers.delete("content-encoding");
    headers.delete("content-length");
    headers.delete("transfer-encoding");
    return new Response(Buffer.from(reply.body, "base64"), {
      status: reply.status,
      headers,
    });
  },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  await bounded(
    Promise.race([
      ready,
      child.exited.then((code) => {
        throw new Error(`Lab runner exited before readiness (${code})`);
      }),
    ]),
    90_000,
    "Lab startup timed out",
  );
  console.log(
    "Movie stack ready: real Prowlarr, rTorrent, Plex and native movie worker.",
  );
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
  });
  await context.addCookies([
    {
      name: "podible_session",
      value: "movie-stack-fixture",
      url: proxy.url.toString(),
      httpOnly: true,
    },
  ]);
  const page = await context.newPage();
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(new URL("/app/movies", proxy.url).href);
  await page.getByRole("searchbox", { name: "Search movies" }).fill("fixture");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("heading", { name: "A Fixture Film", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Request movie", exact: true })
    .click();
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor({ timeout: 360_000 });
  await page
    .getByText(
      "Download stopped making progress; retained its data and looking for a different release",
      { exact: true },
    )
    .waitFor();
  console.log(
    "Movie stack initial request ready: stalled release replaced, bytes transferred, ffprobe passed, symlink indexed and Plex byte-range read verified.",
  );
  await context.request.post(new URL("/__fixture/upgrade", proxy.url).href);
  await page
    .getByRole("button", { name: "Check for upgrade", exact: true })
    .click();
  await page.getByText(/Quality tier: 4/).waitFor({ timeout: 360_000 });
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  if (await page.evaluate("document.documentElement.scrollWidth > innerWidth"))
    throw new Error("Real-stack request overflows on mobile");
  await page.screenshot({
    path: path.join(artifacts, "movie-stack-mobile.png"),
    fullPage: false,
    animations: "disabled",
  });
  const report = await (
    await context.request.get(new URL("/__fixture/report", proxy.url).href)
  ).json();
  if (
    report.upstreamSearches < 1 ||
    report.peerRequests[1] < 1 ||
    report.peerRequests[2] < 1 ||
    report.torrents < 3 ||
    errors.length
  )
    throw new Error(
      "Real-stack evidence incomplete: " + JSON.stringify({ report, errors }),
    );
  await fs.writeFile(
    path.join(artifacts, "movie-stack-acceptance.json"),
    JSON.stringify(
      {
        checkedAt: new Date().toISOString(),
        browserToPlex: true,
        prowlarrSearch: true,
        actualTorrentTransfers: true,
        stalledReplacement: true,
        qualityUpgrade: true,
        ...report,
      },
      null,
      2,
    ),
  );
  console.log(
    "Full local movie POC acceptance passed. Report: tmp/movies-lab/movie-stack-acceptance.json",
  );
} finally {
  await browser?.close();
  proxy.stop(true);
  await command(["docker", "stop", runnerName]).catch(() =>
    child.kill(),
  );
  await child.exited;
  await provider?.close();
}
