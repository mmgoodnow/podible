import { PlaybackError, plexHlsPlaylistType, plexHlsRoot, plexHlsSegment, type PlexHlsScope, type PlexPlayback } from "./plex-playback";

const maxPlaylistBytes = 1024 * 1024;
const maxSegments = 10_000;
const maxSegmentBytes = 32 * 1024 * 1024;
const segmentType = "video/mp2t";
const packetBytes = 188;

export type PlaybackHls = { scope: PlexHlsScope; master: string; media: string; segments: ReadonlySet<string> };

const unsupported = (): never => { throw new PlaybackError(415); };
const mime = (response: Response) => response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase();

async function readPlaylist(response: Response) {
  if (response.status !== 200 || !response.body || mime(response) !== plexHlsPlaylistType ||
      ![null, "identity"].includes(response.headers.get("content-encoding")) ||
      Number(response.headers.get("content-length")) > maxPlaylistBytes) {
    await response.body?.cancel();
    throw new PlaybackError([401, 403].includes(response.status) ? 403 : response.status === 400 ? 415 : 502);
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxPlaylistBytes) { await reader.cancel(); unsupported(); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
  if (/[\x00-\x09\x0b\x0c\x0e-\x1f\x7f]/.test(text) || /\r(?!\n)/.test(text)) unsupported();
  const lines = text.split(/\r?\n/);
  if (lines.shift() !== "#EXTM3U") unsupported();
  return lines.filter((line) => line && (!line.startsWith("#") || line.startsWith("#EXT")));
}

function rewriteMaster(lines: string[], sessionId: string, base: string) {
  const version = lines[0]?.match(/^#EXT-X-VERSION:([1-7])$/)?.[1];
  if (version) lines = lines.slice(1);
  const stream = /^#EXT-X-STREAM-INF:(.*)$/.exec(lines[0] ?? "")?.[1];
  const expected = `session/${sessionId}/base/index.m3u8`;
  if (!stream || lines.length !== 2 || (lines[1] !== expected && lines[1] !== `${plexHlsRoot}/${expected}`)) return unsupported();
  const attrs = [...stream.matchAll(/(?:^|,)([A-Z-]+)=("[^"]*"|[^,]+)(?=,|$)/g)];
  if (attrs.map((attr) => attr[0]).join("") !== stream) unsupported();
  const values = new Map<string, string>();
  for (const [, key, value] of attrs) {
    const pattern = key === "RESOLUTION" ? /^\d{1,5}x\d{1,5}$/ : key === "FRAME-RATE" ? /^\d{1,3}(?:\.\d{1,6})?$/ :
      key === "CODECS" ? /^"[a-zA-Z0-9.,-]{1,120}"$/ : ["PROGRAM-ID", "BANDWIDTH", "AVERAGE-BANDWIDTH"].includes(key!) ? /^\d{1,9}$/ : null;
    if (!pattern?.test(value!) || values.has(key!)) unsupported();
    values.set(key!, value!);
  }
  const bandwidth = Number(values.get("BANDWIDTH"));
  if (!(bandwidth > 0)) unsupported();
  // Rebuild instead of forwarding upstream metadata, even for supported attributes.
  return `#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth}\n${base}/media.m3u8\n`;
}

function rewriteMedia(lines: string[], sessionId: string, base: string) {
  const out = ["#EXTM3U"];
  const segments = new Set<string>();
  let pending = false;
  let ended = false;
  const globals = new Set<string>();
  const prefix = `${plexHlsRoot}/session/${sessionId}/base/`;
  for (const line of lines) {
    if (ended) unsupported();
    if (pending) {
      const name = line.startsWith(prefix) ? line.slice(prefix.length) : line;
      if (!plexHlsSegment.test(name) || segments.has(name) || segments.size >= maxSegments) unsupported();
      segments.add(name);
      out.push(`${base}/${name}`);
      pending = false;
      continue;
    }
    const duration = /^#EXTINF:(\d{1,2}(?:\.\d{1,9})?),.*$/.exec(line)?.[1];
    if (duration !== undefined && Number(duration) > 0) { out.push(`#EXTINF:${duration},`); pending = true; continue; }
    if (line === "#EXT-X-ENDLIST") { out.push(line); ended = true; continue; }
    if (line === "#EXT-X-DISCONTINUITY") { out.push(line); continue; }
    const global = /^(#EXT-X-(?:VERSION|TARGETDURATION|MEDIA-SEQUENCE|PLAYLIST-TYPE|ALLOW-CACHE|START)):(.*)$/.exec(line);
    if (!global || segments.size || globals.has(global[1]!)) return unsupported();
    const tag = global[1]!, value = global[2]!;
    const valid = tag === "#EXT-X-VERSION" ? /^[1-7]$/.test(value) : tag === "#EXT-X-TARGETDURATION" ? /^[1-9]\d?$/.test(value) :
      tag === "#EXT-X-MEDIA-SEQUENCE" ? /^\d{1,9}$/.test(value) : tag === "#EXT-X-PLAYLIST-TYPE" ? value === "VOD" || value === "EVENT" :
      tag === "#EXT-X-START" ? /^TIME-OFFSET=-?\d{1,8}(?:\.\d{1,9})?(?:,PRECISE=(?:YES|NO))?$/.test(value) :
      value === "YES" || value === "NO";
    if (!valid) unsupported();
    globals.add(tag);
    if (tag !== "#EXT-X-ALLOW-CACHE") out.push(line);
  }
  if (!ended || pending || !segments.size || !globals.has("#EXT-X-TARGETDURATION")) unsupported();
  return { media: `${out.join("\n")}\n`, segments };
}

export async function prepareHls(plex: PlexPlayback, part: Parameters<PlexPlayback["startHls"]>[0], scope: PlexHlsScope, base: string): Promise<PlaybackHls> {
  const master = rewriteMaster(await readPlaylist(await plex.startHls(part, scope)), scope.id, base);
  const media = rewriteMedia(await readPlaylist(await plex.hlsResource(scope, "index.m3u8")), scope.id, base);
  const first = await hlsSegment(await plex.hlsResource(scope, media.segments.values().next().value!), false);
  const probe = first.body!.getReader();
  try { if ((await probe.read()).done) throw new PlaybackError(502); }
  finally { await probe.cancel(); }
  return { scope, master, ...media };
}

export function hlsPlaylist(body: string, head: boolean) {
  return new Response(head ? null : body, { headers: { "Content-Type": plexHlsPlaylistType,
    "Content-Length": String(Buffer.byteLength(body)) } });
}

export async function hlsSegment(response: Response, head: boolean) {
  const length = response.headers.get("content-length");
  if (response.status !== 200 || !response.body || mime(response) !== segmentType ||
      ![null, "identity"].includes(response.headers.get("content-encoding")) ||
      (length !== null && (!/^\d{1,9}$/.test(length) || Number(length) <= 0 || Number(length) > maxSegmentBytes || Number(length) % packetBytes !== 0))) {
    await response.body?.cancel();
    throw new PlaybackError([401, 403].includes(response.status) ? 403 : response.status === 404 ? 404 : 502);
  }
  const headers = new Headers({ "Content-Type": segmentType });
  if (length !== null) headers.set("Content-Length", length);
  if (head) { await response.body.cancel(); return new Response(null, { headers }); }
  let received = 0;
  const body = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      const syncOffset = (packetBytes - received % packetBytes) % packetBytes;
      received += chunk.byteLength;
      if (received > maxSegmentBytes || (length !== null && received > Number(length))) return controller.error(new PlaybackError(502));
      for (let index = syncOffset; index < chunk.byteLength; index += packetBytes) {
        if (chunk[index] !== 0x47) return controller.error(new PlaybackError(502));
      }
      if (chunk.byteLength) controller.enqueue(chunk);
    },
    flush(controller) {
      if (!received || received % packetBytes !== 0 || (length !== null && received !== Number(length))) controller.error(new PlaybackError(502));
    },
  }));
  return new Response(body, { headers });
}
