import path from "node:path";
import {
  readFileSync,
  mkdirSync,
  writeFileSync,
  chmodSync,
  renameSync,
} from "node:fs";
import { randomBytes, randomUUID } from "node:crypto";
import { z } from "zod";
import { configDir } from "../config";
import { TransmissionAdapter, TransmissionRpc } from "./transmission-client";
import { prepareSeedOnlyResumes, seedOnlyIntentDirectory } from "./seed-only-state";
const identitySchema = z.object({
  id: z.string().uuid(),
  username: z.string().min(1),
  password: z.string().min(32),
});
export function readTransmissionIdentity(
  directory = path.join(configDir, "transmission"),
) {
  return identitySchema.parse(
    JSON.parse(readFileSync(path.join(directory, "identity.json"), "utf8")),
  );
}
export function prepareTransmission(
  directory = path.join(configDir, "transmission"),
) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  chmodSync(directory, 0o700);
  const identityPath = path.join(directory, "identity.json");
  try {
    readFileSync(identityPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    writeFileSync(
      identityPath,
      JSON.stringify({
        id: randomUUID(),
        username: "podible",
        password: randomBytes(32).toString("hex"),
      }),
      { mode: 0o600, flag: "wx" },
    );
  }
  chmodSync(identityPath, 0o600);
  const identity = readTransmissionIdentity(directory);
  let previous = {};
  try {
    previous = JSON.parse(
      readFileSync(path.join(directory, "settings.json"), "utf8"),
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const settings = {
    ...previous,
    "rpc-enabled": true,
    "rpc-bind-address": "127.0.0.1",
    "rpc-port": 9091,
    "rpc-authentication-required": true,
    "rpc-username": identity.username,
    "rpc-password": identity.password,
    "rpc-whitelist-enabled": true,
    "rpc-whitelist": "127.0.0.1",
    "rpc-host-whitelist-enabled": true,
    "rpc-host-whitelist": "localhost",
    "rpc-url": "/transmission/",
    "peer-port": 51413,
    "peer-port-random-on-start": false,
    "port-forwarding-enabled": false,
    "start-added-torrents": false,
    "watch-dir-enabled": false,
    "rename-partial-files": false,
    "incomplete-dir-enabled": false,
    "trash-original-torrent-files": false,
    umask: 0o027,
    "script-torrent-added-enabled": false,
    "script-torrent-done-enabled": false,
    "script-torrent-done-seeding-enabled": false,
  };
  const temporary = path.join(directory, "settings.json.tmp");
  writeFileSync(temporary, JSON.stringify(settings), { mode: 0o600 });
  renameSync(temporary, path.join(directory, "settings.json"));
  prepareSeedOnlyResumes(directory, seedOnlyIntentDirectory(directory));
  return identity;
}
export function managedTransmission(
  root: string,
  directory = path.join(configDir, "transmission"),
) {
  const identity = readTransmissionIdentity(directory);
  return new TransmissionAdapter({
    clientId: `transmission:${identity.id}`,
    roots: { movies: path.join(root, "downloads") },
    ownershipTag: "Podible Movies",
    seedOnlyIntentDirectory: seedOnlyIntentDirectory(directory),
    rpc: new TransmissionRpc({
      url: "http://127.0.0.1:9091/transmission/rpc",
      username: identity.username,
      password: identity.password,
    }),
  });
}

export function managedBookTransmission(
  downloadsRoot: string,
  directory = path.join(configDir, "transmission"),
) {
  const identity = readTransmissionIdentity(directory);
  return new TransmissionAdapter({
    clientId: `transmission:${identity.id}`,
    roots: { books: downloadsRoot },
    ownershipTag: "Podible Books",
    seedOnlyIntentDirectory: seedOnlyIntentDirectory(directory),
    rpc: new TransmissionRpc({
      url: "http://127.0.0.1:9091/transmission/rpc",
      username: identity.username,
      password: identity.password,
    }),
  });
}

export function managedTvTransmission(root: string, directory = path.join(configDir, "transmission")) {
  const identity = readTransmissionIdentity(directory);
  return new TransmissionAdapter({
    clientId: `transmission:${identity.id}`,
    roots: { tv: path.join(root, "downloads") },
    ownershipTag: "Podible TV",
    seedOnlyIntentDirectory: seedOnlyIntentDirectory(directory),
    rpc: new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password }),
  });
}
