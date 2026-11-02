import bencode from "bencode";
import { createHash } from "node:crypto";
import { expect, test } from "bun:test";
import { RtorrentAdapter } from "../../src/media/rtorrent-client";
import { inspectMetainfo } from "../../src/media/metainfo";
import { startMockRtorrent } from "../mocks/rtorrent";

const metainfo = new Uint8Array(
  bencode.encode({
    info: {
      name: "movie.bin",
      length: 10,
      "piece length": 16384,
      pieces: createHash("sha1").update("movie data").digest(),
    },
  }),
);
const hash = inspectMetainfo(metainfo).infoHash;
const location = { rootId: "managed", relativePath: "payload/one" };
const config = {
  name: "movie.bin",
  basePath: "/managed/payload/one/movie.bin",
  directory: "/managed/payload/one",
  sizeBytes: 10,
  completeAfterPolls: 2,
  filePaths: ["movie.bin"],
};
function adapter(url: string) {
  return new RtorrentAdapter({
    clientId: "rtorrent-1",
    ownershipTag: "Podible Movies",
    settings: { transport: "http-xmlrpc", url, username: "", password: "" },
    roots: { managed: "/managed" },
  });
}

test("torrent adapter adds paused, reconciles uncertain success and controls only its own record", async () => {
  const mock = startMockRtorrent({
    byHash: { [hash]: config },
    failAfterLoad: true,
  });
  try {
    const client = adapter(mock.url);
    const ref = await client.ensureAdded({ metainfo, location });
    expect((await client.inspect(ref)).state).toBe("inactive");
    expect(await client.ensureAdded({ metainfo, location })).toEqual(ref);
    expect(mock.loadCalls).toEqual([hash]);
    await client.recheck(ref);
    await client.resume(ref);
    expect((await client.inspect(ref)).state).toBe("seeding");
    expect((await client.files(ref))[0]?.path).toBe(
      "/managed/payload/one/movie.bin",
    );
    await client.removeRecord(ref);
    expect(mock.controlCalls.at(-1)).toBe("d.erase");
    expect(mock.controlCalls.some((call) => /delete|unlink/.test(call))).toBe(
      false,
    );
    await expect(client.inspect({ ...ref, clientId: "other" })).rejects.toThrow(
      "another client",
    );
  } finally {
    mock.stop();
  }
});

test("torrent adapter rejects preexisting foreign ownership, layout changes and unsafe locations", async () => {
  const mock = startMockRtorrent({
    byHash: { [hash]: config },
    preloaded: [hash],
  });
  try {
    await expect(
      adapter(mock.url).ensureAdded({ metainfo, location }),
    ).rejects.toThrow("unmanaged");
    for (const relativePath of ["../escape", "/etc", "a\\b", "a\0b"])
      await expect(
        adapter(mock.url).ensureAdded({
          metainfo,
          location: { rootId: "managed", relativePath },
        }),
      ).rejects.toThrow("location");
    expect(mock.loadCalls).toHaveLength(0);
  } finally {
    mock.stop();
  }
  const owned = startMockRtorrent({
    byHash: { [hash]: { ...config, ownershipTag: "Podible Movies" } },
    preloaded: [hash],
  });
  try {
    await expect(
      adapter(owned.url).ensureAdded({
        metainfo,
        location: { ...location, relativePath: "another" },
      }),
    ).rejects.toThrow("another layout");
  } finally {
    owned.stop();
  }
});
