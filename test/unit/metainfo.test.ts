import { expect, test } from "bun:test";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { inspectMetainfo } from "../../src/media/metainfo";

const piece = createHash("sha1").update("movie data").digest();
function torrent(info: Record<string, unknown>) {
  return new Uint8Array(
    bencode.encode({
      announce: "http://127.0.0.1:1/announce",
      info: { "piece length": 16384, pieces: piece, ...info },
    }),
  );
}
test("metainfo inspection supports v1 single and multi-file layouts", () => {
  expect(
    inspectMetainfo(torrent({ name: "movie.mkv", length: 10 })).files,
  ).toEqual([{ path: "movie.mkv", size: 10 }]);
  const info = inspectMetainfo(
    torrent({
      name: "release",
      files: [
        { path: ["movie.mkv"], length: 8 },
        { path: ["extras", "release.nfo"], length: 2 },
      ],
    }),
  );
  expect(info.files).toEqual([
    { path: "release/movie.mkv", size: 8 },
    { path: "release/extras/release.nfo", size: 2 },
  ]);
  expect(info.infoHash).toMatch(/^[a-f0-9]{40}$/);
});
test("metainfo cannot escape the managed directory or create symlinks", () => {
  for (const name of ["../movie", "/movie", "..", "a\\b", "a\0b"])
    expect(() => inspectMetainfo(torrent({ name, length: 10 }))).toThrow(
      "Unsafe",
    );
  expect(() =>
    inspectMetainfo(
      torrent({
        name: "release",
        files: [{ path: ["..", "movie"], length: 10 }],
      }),
    ),
  ).toThrow("Unsafe");
  expect(() =>
    inspectMetainfo(
      torrent({
        name: "movie",
        length: 10,
        attr: "l",
        "symlink path": ["/etc/passwd"],
      }),
    ),
  ).toThrow("symlinks");
  expect(() =>
    inspectMetainfo(
      torrent({ name: "movie", "name.utf-8": "../escape", length: 10 }),
    ),
  ).toThrow("Unsafe");
});
test("metainfo rejects unsupported metadata, invalid pieces and path collisions", () => {
  expect(() =>
    inspectMetainfo(torrent({ name: "movie", length: 10, "meta version": 2 })),
  ).toThrow("v1");
  expect(() =>
    inspectMetainfo(torrent({ name: "movie", length: 30_000 })),
  ).toThrow("piece count");
  expect(() =>
    inspectMetainfo(
      torrent({
        name: "release",
        files: [
          { path: ["movie"], length: 5 },
          { path: ["MOVIE"], length: 5 },
        ],
      }),
    ),
  ).toThrow("collision");
  expect(() =>
    inspectMetainfo(
      torrent({
        name: "release",
        files: [
          { path: ["movie"], length: 5 },
          { path: ["movie", "file"], length: 5 },
        ],
      }),
    ),
  ).toThrow("collision");
});
