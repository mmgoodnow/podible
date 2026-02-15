import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";

import { runMigrations } from "../../src/kindling/db";
import { createPodibleFetchHandler } from "../../src/kindling/http";
import { KindlingRepo } from "../../src/kindling/repo";
import { defaultSettings } from "../../src/kindling/settings";
import { runWorker } from "../../src/kindling/worker";
import { startMockRtorrent } from "../mocks/rtorrent";
import { startMockTorznab } from "../mocks/torznab";

function makeTorrentBytes(name: string): Uint8Array {
  const nameLen = Buffer.byteLength(name);
  const content = `d8:announce13:http://tracker/4:infod4:name${nameLen}:${name}12:piece lengthi16384e6:lengthi10e6:pieces20:12345678901234567890ee`;
  return new Uint8Array(Buffer.from(content, "ascii"));
}

async function eventually<T>(fn: () => T | Promise<T>, predicate: (value: T) => boolean, label: string): Promise<T> {
  const started = Date.now();
  for (;;) {
    const value = await fn();
    if (predicate(value)) return value;
    if (Date.now() - started > 8_000) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await sleep(100);
  }
}

async function rpc(fetchHandler: (request: Request) => Promise<Response>, method: string, params: unknown, id = 1) {
  const response = await fetchHandler(
    new Request("http://localhost/rpc", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        method,
        params,
      }),
    })
  );
  expect(response.status).toBe(200);
  return (await response.json()) as any;
}

describe("kindling e2e", () => {
  test("audio and ebook flows including reconcile", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "kindling-e2e-"));
    const downloadAudio = path.join(tempRoot, "downloads-audio");
    const downloadEbook = path.join(tempRoot, "downloads-ebook");
    const downloadRecon = path.join(tempRoot, "downloads-reconcile");
    const libraryRoot = path.join(tempRoot, "library");

    await mkdir(downloadAudio, { recursive: true });
    await mkdir(downloadEbook, { recursive: true });
    await mkdir(downloadRecon, { recursive: true });
    await mkdir(libraryRoot, { recursive: true });

    const audioFile = path.join(downloadAudio, "book.mp3");
    const ebookFile = path.join(downloadEbook, "book.epub");
    const reconcileFile = path.join(downloadRecon, "other.epub");
    await writeFile(audioFile, Buffer.from("ID3-audio-test-data"));
    await writeFile(ebookFile, Buffer.from("ebook-epub-test-data"));
    await writeFile(reconcileFile, Buffer.from("ebook-reconcile-test-data"));

    const audioTorrent = makeTorrentBytes("audio-book");
    const ebookTorrent = makeTorrentBytes("ebook-book");
    const reconcileTorrent = makeTorrentBytes("ebook-reconcile");

    const audioHash = "0123456789abcdef0123456789abcdef01234567";
    const ebookHash = "89abcdef0123456789abcdef0123456789abcdef";
    const reconcileHash = "1111111111111111111111111111111111111111";

    const torznab = startMockTorznab({
      results: [
        { title: "Dune Audio", torrentId: "audio", size: 1234, infoHash: audioHash },
        { title: "Dune Ebook", torrentId: "ebook", size: 456, infoHash: ebookHash },
      ],
      torrents: {
        audio: audioTorrent,
        ebook: ebookTorrent,
      },
    });

    const rtorrent = startMockRtorrent({
      byHash: {
        [audioHash]: {
          name: "Dune Audio",
          basePath: downloadAudio,
          sizeBytes: 1234,
          completeAfterPolls: 1,
        },
        [ebookHash]: {
          name: "Dune Ebook",
          basePath: downloadEbook,
          sizeBytes: 456,
          completeAfterPolls: 1,
        },
        [reconcileHash]: {
          name: "Dune Recon",
          basePath: downloadRecon,
          sizeBytes: 222,
          completeAfterPolls: 1,
        },
      },
      preloaded: [reconcileHash],
    });

    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new KindlingRepo(db);
    repo.updateSettings(
      defaultSettings({
        auth: { mode: "local", key: "test" },
        torznab: [
          {
            name: "mock",
            baseUrl: torznab.baseUrl,
            categories: { audio: "audio", ebook: "book" },
          },
        ],
        rtorrent: {
          transport: "http-xmlrpc",
          url: rtorrent.url,
          username: "",
          password: "",
        },
        libraryRoot,
        polling: { rtorrentMs: 50, scanMs: 200 },
      })
    );

    const fetchHandler = createPodibleFetchHandler(repo, Date.now());

    let stopWorker = false;
    const worker = runWorker({
      repo,
      getSettings: () => repo.getSettings(),
      shouldStop: () => stopWorker,
    });

    try {
      const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });

      const searchJson = await rpc(fetchHandler, "search.run", { query: "Dune Frank Herbert", media: "audio" }, 1);
      const audioResult = searchJson.result.results.find((row: any) => row.title === "Dune Audio");
      const ebookResult = searchJson.result.results.find((row: any) => row.title === "Dune Ebook");
      expect(audioResult).toBeTruthy();
      expect(ebookResult).toBeTruthy();

      const snatchAudio = await rpc(fetchHandler, "snatch.create", {
        bookId: book.id,
        provider: audioResult.provider,
        title: audioResult.title,
        mediaType: "audio",
        url: audioResult.url,
        infoHash: audioResult.infoHash,
        sizeBytes: audioResult.sizeBytes,
      }, 2);
      expect(snatchAudio.result.idempotent).toBe(false);

      const snatchAgainJson = await rpc(fetchHandler, "snatch.create", {
        bookId: book.id,
        provider: audioResult.provider,
        title: audioResult.title,
        mediaType: "audio",
        url: audioResult.url,
        infoHash: audioResult.infoHash,
        sizeBytes: audioResult.sizeBytes,
      }, 3);
      expect(snatchAgainJson.result.idempotent).toBe(true);

      const snatchEbook = await rpc(fetchHandler, "snatch.create", {
        bookId: book.id,
        provider: ebookResult.provider,
        title: ebookResult.title,
        mediaType: "ebook",
        url: ebookResult.url,
        infoHash: ebookResult.infoHash,
        sizeBytes: ebookResult.sizeBytes,
      }, 4);
      expect(snatchEbook.result.idempotent).toBe(false);

      await eventually(
        () => repo.listAssetsByBook(book.id),
        (assets) => assets.some((asset) => asset.kind !== "ebook") && assets.some((asset) => asset.kind === "ebook"),
        "audio+ebook assets"
      );

      const assetsRes = await fetchHandler(new Request(`http://localhost/assets?bookId=${book.id}`));
      expect(assetsRes.status).toBe(200);
      const assetsJson = (await assetsRes.json()) as any;
      const audioAsset = assetsJson.assets.find((asset: any) => asset.kind !== "ebook");
      const ebookAsset = assetsJson.assets.find((asset: any) => asset.kind === "ebook");
      expect(audioAsset).toBeTruthy();
      expect(ebookAsset).toBeTruthy();

      const streamRes = await fetchHandler(new Request(`http://localhost/stream/${audioAsset.id}.mp3`));
      expect(streamRes.status).toBe(200);
      const streamBytes = new Uint8Array(await streamRes.arrayBuffer());
      expect(streamBytes.length).toBeGreaterThan(0);

      const ebookRes = await fetchHandler(new Request(`http://localhost/ebook/${ebookAsset.id}`));
      expect(ebookRes.status).toBe(200);
      const ebookBytes = new Uint8Array(await ebookRes.arrayBuffer());
      expect(ebookBytes.length).toBeGreaterThan(0);

      const feedRes = await fetchHandler(new Request("http://localhost/feed.xml"));
      expect(feedRes.status).toBe(200);
      const feedXml = await feedRes.text();
      expect(feedXml.includes(`/stream/${audioAsset.id}.`)).toBe(true);

      const book2 = repo.createBook({ title: "Dune 2", author: "Frank Herbert" });
      repo.createRelease({
        bookId: book2.id,
        provider: "mock",
        title: "Dune 2 recon",
        mediaType: "ebook",
        infoHash: reconcileHash,
        url: `${torznab.baseUrl}/torrent/ebook.torrent`,
        sizeBytes: 200,
        status: "downloaded",
      });

      const reconcileRes = await rpc(fetchHandler, "import.reconcile", {}, 5);
      expect(reconcileRes.result.jobId).toBeGreaterThan(0);

      await eventually(
        () => repo.listAssetsByBook(book2.id),
        (assets) => assets.some((asset) => asset.kind === "ebook"),
        "reconcile import"
      );
    } finally {
      stopWorker = true;
      await sleep(400);
      await Promise.race([worker, sleep(1500)]);
      torznab.stop();
      rtorrent.stop();
      db.close();
    }
  });
});
