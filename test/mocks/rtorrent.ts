import { randomUUID } from "node:crypto";

import { normalizeInfoHash } from "../../src/torrent";

type DownloadConfig = {
  name: string;
  basePath: string;
  sizeBytes: number;
  completeAfterPolls: number;
};

type DownloadState = DownloadConfig & {
  infoHash: string;
  bytesDone: number;
  completePolls: number;
};

type MockRtorrentOptions = {
  byHash: Record<string, DownloadConfig>;
  preloaded?: string[];
};

type MockRtorrent = {
  url: string;
  stop: () => void;
};

function xmlResponseString(value: string): string {
  return `<?xml version="1.0"?><methodResponse><params><param><value><string>${value}</string></value></param></params></methodResponse>`;
}

function xmlResponseInt(value: number): string {
  return `<?xml version="1.0"?><methodResponse><params><param><value><int>${Math.trunc(value)}</int></value></param></params></methodResponse>`;
}

function parseMethodName(xml: string): string {
  const match = /<methodName>([^<]+)<\/methodName>/.exec(xml);
  return match?.[1] ?? "";
}

function parseStringParams(xml: string): string[] {
  return [...xml.matchAll(/<string>([\s\S]*?)<\/string>/g)].map((m) => m[1] ?? "");
}

function parseBase64Param(xml: string): Uint8Array | null {
  const match = /<base64>([\s\S]*?)<\/base64>/.exec(xml);
  if (!match?.[1]) return null;
  return new Uint8Array(Buffer.from(match[1], "base64"));
}

export function startMockRtorrent(options: MockRtorrentOptions): MockRtorrent {
  const id = randomUUID();
  const states = new Map<string, DownloadState>();

  const byHash = Object.fromEntries(
    Object.entries(options.byHash).map(([hash, config]) => [normalizeInfoHash(hash), config])
  );

  for (const rawHash of options.preloaded ?? []) {
    const hash = normalizeInfoHash(rawHash);
    const config = byHash[hash];
    if (!config) continue;
    states.set(hash, {
      ...config,
      infoHash: hash,
      bytesDone: config.sizeBytes,
      completePolls: config.completeAfterPolls,
    });
  }
  for (const [hash, config] of Object.entries(byHash)) {
    if (states.has(hash)) continue;
    states.set(hash, {
      ...config,
      infoHash: hash,
      bytesDone: 0,
      completePolls: 0,
    });
  }

  const server = Bun.serve({
    port: 0,
    async fetch(request) {
      const body = await request.text();
      const method = parseMethodName(body);

      if (method === "load.raw_start") {
        const bytes = parseBase64Param(body);
        if (!bytes) return new Response(xmlResponseInt(0));
        return new Response(xmlResponseInt(0), { headers: { "Content-Type": "text/xml" } });
      }

      const [arg0] = parseStringParams(body);
      const hash = normalizeInfoHash(arg0 || "");
      const state = states.get(hash);
      if (!state) {
        return new Response(xmlResponseString(""), { headers: { "Content-Type": "text/xml" } });
      }

      if (method === "d.complete") {
        state.completePolls += 1;
        const complete = state.completePolls >= state.completeAfterPolls;
        if (complete) {
          state.bytesDone = state.sizeBytes;
        } else {
          state.bytesDone = Math.max(1, Math.floor(state.sizeBytes / 2));
        }
        return new Response(xmlResponseInt(complete ? 1 : 0), { headers: { "Content-Type": "text/xml" } });
      }

      if (method === "d.name") {
        return new Response(xmlResponseString(state.name), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.hash") {
        return new Response(xmlResponseString(state.infoHash.toUpperCase()), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.is_active") {
        return new Response(xmlResponseInt(1), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.base_path") {
        return new Response(xmlResponseString(state.basePath), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.bytes_done") {
        return new Response(xmlResponseInt(state.bytesDone), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.size_bytes") {
        return new Response(xmlResponseInt(state.sizeBytes), { headers: { "Content-Type": "text/xml" } });
      }
      if (method === "d.left_bytes") {
        return new Response(xmlResponseInt(Math.max(0, state.sizeBytes - state.bytesDone)), {
          headers: { "Content-Type": "text/xml" },
        });
      }
      if (method === "d.down.rate") {
        return new Response(xmlResponseInt(state.bytesDone >= state.sizeBytes ? 0 : 10), {
          headers: { "Content-Type": "text/xml" },
        });
      }
      if (method === "d.message") {
        return new Response(xmlResponseString(""), { headers: { "Content-Type": "text/xml" } });
      }

      return new Response(xmlResponseString(`unknown method ${method} ${id}`), {
        headers: { "Content-Type": "text/xml" },
      });
    },
  });

  return {
    url: `http://127.0.0.1:${server.port}`,
    stop: () => server.stop(),
  };
}
