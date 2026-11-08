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
test("Transmission classifies tracker errors separately from local faults without exposing tracker credentials", async () => {
  let error = 0;
  const hash = inspectMetainfo(metainfo).infoHash;
  const rpc = new TransmissionRpc({ url: "http://127.0.0.1", username: "a", password: "b",
    fetcher: (async (_url, _init) => Response.json({ result: "success", arguments: { torrents: [{
      hashString: hash, downloadDir: "/managed/case", labels: ["Podible TV"], status: 6,
      leftUntilDone: 0, sizeWhenDone: 7, totalSize: 7, haveValid: 7, haveUnchecked: 0, rateDownload: 0,
      error, errorString: "https://tracker.invalid/secret-passkey/announce failed",
      files: [{ name: "film.mkv", length: 7 }],
    }] } })) as typeof fetch });
  const client = new TransmissionAdapter({ clientId: "engine", roots: { tv: "/managed" }, ownershipTag: "Podible TV", rpc });
  for (const [code, expected] of [[0, null], [1, "tracker"], [2, "tracker"], [3, "storage"], [99, "unknown"]] as const) {
    error = code;
    const snapshot = await client.inspect({ clientId: "engine", nativeId: hash });
    expect(snapshot).toMatchObject({ complete: true, bytesDone: 7, sizeBytes: 7, errorKind: expected });
    expect(snapshot.message === null).toBe(code === 0);
    expect(JSON.stringify(snapshot)).not.toContain("secret-passkey");
  }
});
test("seed-only adds disable every file before start and completeness uses actual verified bytes", async () => {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "seed-only-adapter-")));
  await fs.mkdir(path.join(root, "case"));
  try {
  const calls: { method: string; arguments: Record<string, any> }[] = [];
  let added = false, wanted = true, valid = 0, ignoreUpdates = false, omitPhysical = false;
  let labels: string[] = [];
  const hash = inspectMetainfo(metainfo).infoHash;
  const rpc = new TransmissionRpc({ url: "http://127.0.0.1", username: "a", password: "b",
    fetcher: (async (_url, init) => {
      const input = JSON.parse(init!.body as string); calls.push(input);
      if (input.method === "torrent-add") {
        added = true; labels = input.arguments.labels;
        wanted = !input.arguments["files-unwanted"].includes(0);
      }
      if (input.method === "torrent-set" && !ignoreUpdates) wanted = false;
      return Response.json({ result: "success", arguments: input.method === "torrent-get" ? { torrents: added ? [{
        hashString: hash, downloadDir: path.join(root, "case"), labels, status: 0, leftUntilDone: 0, sizeWhenDone: 0,
        ...(omitPhysical ? {} : { totalSize: 7, haveValid: valid, haveUnchecked: 0 }), rateDownload: 0, error: 0, errorString: "",
        files: [{ name: "film.mkv", length: 7 }], fileStats: [{ wanted }],
      }] : [] } : {} });
    }) as typeof fetch });
  const client = new TransmissionAdapter({ clientId: "engine", roots: { movies: root }, ownershipTag: "Podible Movies", rpc,
    seedOnlyIntentDirectory: path.join(root, "intents") });
  const ref = await client.ensureAdded({ metainfo, location: { rootId: "movies", relativePath: "case" }, seedOnly: true });
  expect(calls.find(call => call.method === "torrent-add")!.arguments).toMatchObject({ paused: true, "files-unwanted": [0] });
  expect(await client.inspect(ref)).toMatchObject({ sizeBytes: 7, bytesDone: 0, complete: false, seedOnly: true });
  valid = 7;
  expect(await client.inspect(ref)).toMatchObject({ bytesDone: 7, complete: true });
  omitPhysical = true;
  expect(await client.inspect(ref)).toMatchObject({ bytesDone: 0, complete: false });
  omitPhysical = false;
  wanted = true;
  await client.resume(ref);
  expect(calls.at(-1)?.method).toBe("torrent-start");
  expect(wanted).toBe(false);
  const starts = calls.filter(call => call.method === "torrent-start").length;
  wanted = true; ignoreUpdates = true;
  await expect(client.resumeSeedOnly(ref)).rejects.toThrow("file selection");
  expect(calls.filter(call => call.method === "torrent-start")).toHaveLength(starts);
  labels = ["Podible Movies"];
  await expect(client.resumeSeedOnly(ref)).rejects.toThrow("ordinary download");
  await expect(client.resume(ref)).rejects.toThrow("ordinary download");
  expect(calls.filter(call => call.method === "torrent-start")).toHaveLength(starts);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
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
test("a completed check with a deferred stop cannot cancel the next verification", async () => {
  const hash = inspectMetainfo(metainfo).infoHash;
  let state = 2;
  let complete = false;
  let stopAt = 0;
  let verifiedAt = 0;
  let stopCalls = 0;
  const rpc = new TransmissionRpc({
    url: "http://127.0.0.1", username: "a", password: "b",
    fetcher: (async (_url, init) => {
      const input = JSON.parse(init!.body as string);
      if (stopAt && Date.now() >= stopAt) {
        stopAt = 0;
        if (state === 2) { state = 0; verifiedAt = 0; }
      }
      if (verifiedAt && Date.now() >= verifiedAt) { state = 0; complete = true; }
      if (input.method === "torrent-stop") {
        stopCalls++;
        // The old verification finishes before Transmission processes stop_soon().
        state = 0; stopAt = Date.now() + 150;
      }
      if (input.method === "torrent-verify") { state = 2; verifiedAt = Date.now() + 250; }
      return Response.json({ result: "success", arguments: input.method === "torrent-get" ? {
        torrents: [{ hashString: hash, downloadDir: "/managed/case", labels: ["Podible Movies"],
          status: state, leftUntilDone: complete ? 0 : 7, sizeWhenDone: 7,
          rateDownload: 0, error: 0, errorString: "", files: [{ name: "film.mkv", length: 7 }] }],
      } : {} });
    }) as typeof fetch,
  });
  const client = new TransmissionAdapter({ clientId: "engine", roots: { movies: "/managed" }, ownershipTag: "Podible Movies", rpc });
  const ref = { clientId: "engine", nativeId: hash };
  await client.recheck(ref);
  await Bun.sleep(300);
  expect(await client.inspect(ref)).toMatchObject({ state: "inactive", complete: true });
  expect(stopCalls).toBe(1);
  await client.quiesce(ref);
  expect(stopCalls).toBe(1);
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
