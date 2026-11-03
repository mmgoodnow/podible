import { expect, test } from "bun:test";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  TransmissionRpc,
  TransmissionAdapter,
} from "../../src/media/transmission-client";
import { inspectMetainfo } from "../../src/media/metainfo";
import {
  prepareTransmission,
  readTransmissionIdentity,
} from "../../src/media/managed-transmission";
const bytes = Buffer.from("fixture");
const metainfo = bencode.encode({
  info: {
    name: "film.mkv",
    length: bytes.length,
    "piece length": 16384,
    pieces: createHash("sha1").update(bytes).digest(),
  },
});
test("Transmission uses private handshake, reconciles uncertain adds, and never removes content", async () => {
  const calls: { method: string; arguments: any }[] = [];
  let added = false;
  let owned = true;
  const hash = inspectMetainfo(metainfo).infoHash;
  const fetcher = (async (_url: unknown, init: RequestInit) => {
    if (
      (init.headers as Record<string, string>)["X-Transmission-Session-Id"] !==
      "session"
    )
      return new Response(null, {
        status: 409,
        headers: { "X-Transmission-Session-Id": "session" },
      });
    const input = JSON.parse(init.body as string);
    calls.push(input);
    if (input.method === "torrent-add") {
      added = true;
      throw new Error("Uncertain acknowledgment");
    }
    const torrent = {
      hashString: hash,
      downloadDir: "/managed/case",
      labels: owned ? ["Podible Movies"] : [],
      status: 0,
      leftUntilDone: 7,
      sizeWhenDone: 7,
      rateDownload: 0,
      error: 0,
      errorString: "",
      files: [{ name: "film.mkv", length: 7 }],
    };
    return Response.json({
      result: "success",
      arguments:
        input.method === "torrent-get"
          ? { torrents: added ? [torrent] : [] }
          : {},
    });
  }) as typeof fetch;
  const rpc = new TransmissionRpc({
    url: "http://127.0.0.1",
    username: "internal",
    password: "private",
    fetcher,
  });
  const client = new TransmissionAdapter({
    clientId: "engine",
    roots: { movies: "/managed" },
    ownershipTag: "Podible Movies",
    rpc,
  });
  const ref = await client.ensureAdded({
    metainfo,
    location: { rootId: "movies", relativePath: "case" },
  });
  expect(
    await client.ensureAdded({
      metainfo,
      location: { rootId: "movies", relativePath: "case" },
    }),
  ).toEqual(ref);
  expect(calls.filter((c) => c.method === "torrent-add")).toHaveLength(1);
  expect(calls.find((c) => c.method === "torrent-add")!.arguments.paused).toBe(
    true,
  );
  expect(await client.files(ref)).toEqual([
    { index: 0, path: "/managed/case/film.mkv" },
  ]);
  await client.removeRecord(ref);
  expect(calls.at(-1)!.arguments["delete-local-data"]).toBe(false);
  await expect(client.inspect({ ...ref, clientId: "foreign" })).rejects.toThrow(
    "another client",
  );
  owned = false;
  await expect(client.resume(ref)).rejects.toThrow("unmanaged");
  await expect(
    client.ensureAdded({
      metainfo,
      location: { rootId: "movies", relativePath: "../escape" },
    }),
  ).rejects.toThrow("location");
});
test("Transmission checks result even on HTTP 200 and never leaks provider error bodies", async () => {
  const rpc = new TransmissionRpc({
    url: "http://127.0.0.1",
    username: "a",
    password: "b",
    fetcher: (async () =>
      Response.json({
        result: "passkey=private error",
      })) as unknown as typeof fetch,
  });
  await expect(rpc.call("torrent-add")).rejects.toThrow(
    "Torrent engine rejected torrent-add",
  );
});
test("private Transmission identity and resume directory survive preparation with restrictive permissions", async () => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "private-transmission-"),
  );
  try {
    const first = prepareTransmission(root);
    const second = prepareTransmission(root);
    expect(second).toEqual(first);
    expect(readTransmissionIdentity(root)).toEqual(first);
    expect((await fs.stat(path.join(root, "identity.json"))).mode & 0o777).toBe(
      0o600,
    );
    expect((await fs.stat(root)).mode & 0o777).toBe(0o700);
    const settings = JSON.parse(
      await fs.readFile(path.join(root, "settings.json"), "utf8"),
    );
    expect(settings["rpc-bind-address"]).toBe("127.0.0.1");
    expect(settings["rpc-authentication-required"]).toBe(true);
    expect(settings["watch-dir-enabled"]).toBe(false);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
