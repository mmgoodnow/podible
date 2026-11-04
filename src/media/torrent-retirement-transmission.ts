import { z } from "zod";
import { readTransmissionIdentity } from "./managed-transmission";
import { TransmissionRpc } from "./transmission-client";
import { boundedStewardshipFetch } from "./torrent-stewardship";
import { allTrackerRetirementEvidence, retirementDigest } from "./torrent-retirement-evidence";
import { MINIMUM_SEED_SECONDS } from "./torrent-stewardship-types";
import type { RetirementBinding } from "./torrent-retirement-types";

const count = z.number().int().nonnegative().safe();
const rowSchema = z.object({
  id: z.number().int().positive().safe(), hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()).max(100),
  status: z.number().int().min(0).max(6), leftUntilDone: count, secondsSeeding: count,
  totalSize: z.number().int().positive().safe(), sizeWhenDone: count, haveValid: count, haveUnchecked: count,
  error: z.number().int().min(0).max(3), trackerStats: z.unknown(),
});
export type RetirementDaemonSnapshot = {
  clientId: string; identityDigest: string; sessionDigest: string;
  record: z.infer<typeof rowSchema> | null;
};
export interface RetirementDaemon {
  readonly clientId: string;
  read(binding: RetirementBinding): Promise<RetirementDaemonSnapshot>;
  /** Single, conditional, record-only command. No automatic session renegotiation/retry. */
  removeRecord(binding: RetirementBinding, expected: RetirementDaemonSnapshot, assertActive: () => void): Promise<void>;
}
export function validateDaemonBinding(binding: RetirementBinding, snapshot: RetirementDaemonSnapshot) {
  const row = snapshot.record;
  if (snapshot.clientId !== binding.clientId || !row || row.hashString.toLowerCase() !== binding.hash ||
      row.downloadDir !== binding.directory || !row.labels.includes(`Podible ${binding.media === "books" ? "Books" : binding.media === "movies" ? "Movies" : "TV"}`))
    throw new Error("Private daemon record no longer matches the exact binding");
  return row;
}
export function eligibleDaemonRecord(binding: RetirementBinding, snapshot: RetirementDaemonSnapshot, now: number) {
  const row = validateDaemonBinding(binding, snapshot);
  if (![0, 5, 6].includes(row.status) || row.leftUntilDone !== 0 || row.sizeWhenDone !== row.totalSize ||
      row.haveValid !== row.totalSize || row.haveUnchecked !== 0 || row.error === 3 || row.secondsSeeding < MINIMUM_SEED_SECONDS)
    throw new Error("Daemon record is not fully complete with the required actual seed time");
  const trackers = allTrackerRetirementEvidence(row.trackerStats, now);
  if (!trackers) throw new Error("Not every current tracker explicitly reports unregistration");
  return { row, trackers };
}
/** Existing credentials, fixed loopback URL, no file deletion capability. Never used without the required writer fence. */
export function privateRetirementDaemon(options: {
  readIdentity?: typeof readTransmissionIdentity; fetcher?: typeof fetch; now?: () => number;
} = {}): RetirementDaemon {
  const identityReader = options.readIdentity ?? readTransmissionIdentity;
  const identity = identityReader(), clientId = `transmission:${identity.id}`;
  const identityDigest = retirementDigest(identity);
  const fetcher = options.fetcher ?? boundedStewardshipFetch;
  const url = "http://127.0.0.1:9091/transmission/rpc";
  const rpc = new TransmissionRpc({ url, username: identity.username, password: identity.password, fetcher, timeoutMs: 5000 });
  const currentIdentity = () => {
    if (retirementDigest(identityReader()) !== identityDigest) throw new Error("Private daemon identity changed");
  };
  async function session() {
    currentIdentity();
    const value = await rpc.call("session-get", { fields: ["session-id"] });
    return z.string().min(16).max(256).parse(value["session-id"]);
  }
  async function read(binding: RetirementBinding): Promise<RetirementDaemonSnapshot> {
    if (binding.clientId !== clientId) throw new Error("Foreign private daemon binding");
    const before = await session();
    const data = await rpc.call("torrent-get", { ids: [binding.hash], fields: [
      "id", "hashString", "downloadDir", "labels", "status", "leftUntilDone", "secondsSeeding", "totalSize",
      "sizeWhenDone", "haveValid", "haveUnchecked", "error", "trackerStats",
    ] });
    const rows = z.array(rowSchema).max(1).parse(data.torrents);
    const after = await session(); currentIdentity();
    if (before !== after) throw new Error("Private daemon session changed during observation");
    const snapshot = { clientId, identityDigest, sessionDigest: retirementDigest(after), record: rows[0] ?? null };
    if (snapshot.record) validateDaemonBinding(binding, snapshot);
    return snapshot;
  }
  return {
    clientId, read,
    async removeRecord(binding, expected, active) {
      active(); currentIdentity();
      const fresh = await read(binding);
      const current = eligibleDaemonRecord(binding, fresh, (options.now ?? Date.now)());
      const original = eligibleDaemonRecord(binding, expected, (options.now ?? Date.now)());
      const row = current.row;
      if (fresh.identityDigest !== expected.identityDigest || fresh.sessionDigest !== expected.sessionDigest ||
          row.id !== original.row.id || row.secondsSeeding < original.row.secondsSeeding ||
          current.trackers.trackerSetDigest !== original.trackers.trackerSetDigest ||
          current.trackers.trackers.some((tracker, index) => tracker.announcedAt < original.trackers.trackers[index]!.announcedAt))
        throw new Error("Private daemon evidence changed before record-only removal");
      const token = await session();
      if (retirementDigest(token) !== expected.sessionDigest) throw new Error("Private daemon session changed before record-only removal");
      active(); currentIdentity();
      // A 409 is NOT retried. Numeric IDs are scoped to this pinned daemon session.
      const response = await fetcher(url, { method: "POST", redirect: "error", signal: AbortSignal.timeout(5000),
        headers: { "Content-Type": "application/json", "X-Transmission-Session-Id": token,
          Authorization: `Basic ${Buffer.from(`${identity.username}:${identity.password}`).toString("base64")}` },
        body: JSON.stringify({ method: "torrent-remove", arguments: { ids: [row.id], "delete-local-data": false } }),
      });
      if (!response.ok) { void response.body?.cancel().catch(() => {}); throw new Error("Record-only removal acknowledgement is unavailable"); }
      const result = z.object({ result: z.literal("success") }).safeParse(await response.json());
      if (!result.success) throw new Error("Record-only removal acknowledgement is unavailable");
    },
  };
}
