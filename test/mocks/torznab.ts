import { randomUUID } from "node:crypto";

type MockResult = {
  title: string;
  torrentId: string;
  size: number;
  infoHash?: string;
};

type MockTorznabOptions = {
  results: MockResult[];
  torrents: Record<string, Uint8Array>;
};

type MockTorznab = {
  baseUrl: string;
  stop: () => void;
};

function rssItem(baseUrl: string, result: MockResult): string {
  const torrentUrl = `${baseUrl}/torrent/${encodeURIComponent(result.torrentId)}.torrent`;
  const attrs = [
    `<torznab:attr name="size" value="${result.size}" />`,
    ...(result.infoHash ? [`<torznab:attr name="infohash" value="${result.infoHash}" />`] : []),
  ].join("\n");
  return `<item>
<title>${result.title}</title>
<link>${torrentUrl}</link>
<enclosure url="${torrentUrl}" length="${result.size}" />
${attrs}
</item>`;
}

export function startMockTorznab(options: MockTorznabOptions): MockTorznab {
  const id = randomUUID();
  const server = Bun.serve({
    port: 0,
    fetch(request): Response {
      const url = new URL(request.url);
      if (url.pathname === "/api") {
        const body: string = `<?xml version="1.0"?>
<rss xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    ${options.results.map((result) => rssItem(url.origin, result)).join("\n")}
  </channel>
</rss>`;
        return new Response(body, {
          headers: { "Content-Type": "application/rss+xml" },
        });
      }

      if (url.pathname.startsWith("/torrent/")) {
        const idPart = url.pathname.split("/")[2] ?? "";
        const torrentId = idPart.replace(/\.torrent$/i, "");
        const bytes = options.torrents[torrentId];
        if (!bytes) return new Response("Not found", { status: 404 });
        return new Response(bytes, {
          headers: { "Content-Type": "application/x-bittorrent" },
        });
      }

      return new Response(`Mock Torznab ${id}: not found`, { status: 404 });
    },
  });

  return {
    baseUrl: `http://127.0.0.1:${server.port}`,
    stop: () => server.stop(),
  };
}
