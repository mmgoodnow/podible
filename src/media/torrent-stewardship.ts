import { constants } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { readTransmissionIdentity } from "./managed-transmission";
import { TransmissionRpc } from "./transmission-client";
import { recordRetirementEvidence } from "./torrent-retirement-evidence";
import {
  MINIMUM_SEED_SECONDS, stewardshipCollectParamsSchema, stewardshipCollectResultSchema,
  stewardshipObservationSchema, stewardshipStatusParamsSchema, stewardshipStatusSchema,
  type StewardshipSource, type StewardshipObservation, type StewardshipCollectResult,
} from "./torrent-stewardship-types";

const PREFIX = "torrent-stewardship:v1:";
const ANNOUNCEMENT_SPACING_SECONDS = 15 * 60;
const cursorKey = PREFIX + "cursor", lastKey = PREFIX + "last-collection";
const count = z.number().int().nonnegative().safe();
const clientIdSchema = z.string().regex(/^transmission:[a-f0-9-]{36}$/);
const hashSchema = z.string().regex(/^[a-f0-9]{40}$/);
const digest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const cursorSchema = z.object({ source: z.enum(["books", "screen"]), offset: count });
const historySchema = z.object({ latest: stewardshipObservationSchema, history: z.array(stewardshipObservationSchema).max(8) });
const fields = ["hashString", "downloadDir", "labels", "status", "leftUntilDone", "secondsSeeding", "error", "trackerStats", "totalSize", "sizeWhenDone", "haveValid", "haveUnchecked"];
const trackerSchema = z.object({
  id: count, announce: z.string().max(4096), isBackup: z.boolean(), hasAnnounced: z.boolean(),
  lastAnnounceSucceeded: z.boolean(), lastAnnounceTimedOut: z.boolean(), lastAnnounceTime: count,
  lastAnnounceResult: z.string().max(4096),
});
const torrentSchema = z.object({
  hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()).max(100),
  status: z.number().int().min(0).max(6), leftUntilDone: z.number().nonnegative(),
  secondsSeeding: z.number().nullable().optional(), error: z.number().int().min(0).max(3),
  // Incomplete tracker telemetry is unknown, not proof of a tracker error.
  trackerStats: z.unknown().optional(),
  totalSize: count.optional(), sizeWhenDone: count.optional(), haveValid: count.optional(), haveUnchecked: count.optional(),
});
export type Binding = { key: string; media: "books" | "movies" | "tv"; label: string; hash: string;
  clientId: string; directory: string; root: string; rootToken: string | null; bindingDigest: string };
export type StewardshipConnection = {
  clientId: string;
  /** Deliberately no generic RPC or control capability in the collector's adapter. */
  get(ids: string[], fields: readonly string[]): Promise<Record<string, unknown>>;
};
export type StewardshipAdapters = {
  connection?: StewardshipConnection | null;
  now?: () => number;
  deadlineMs?: number;
  assertActive?: () => void;
  retirement?: import("./torrent-retirement").TorrentRetirementAdapters;
};

/** Bound response bytes before the existing private RPC parser sees JSON. */
export const boundedStewardshipFetch: typeof fetch = Object.assign(async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
  const response = await fetch(input, init);
  if (!response.ok) return response;
  const reader = response.body?.getReader();
  if (!reader) throw new Error("Missing private engine response");
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const part = await reader.read(); if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > 4 * 1024 * 1024) throw new Error("Private engine response exceeded observation budget");
      chunks.push(part.value);
    }
  } catch (error) { void reader.cancel().catch(() => {}); throw error; }
  return new Response(Buffer.concat(chunks), { status: response.status, headers: response.headers });
}, { preconnect: fetch.preconnect });
export function stewardshipConnection(adapters: StewardshipAdapters): StewardshipConnection | null {
  if (adapters.connection !== undefined) return adapters.connection;
  try {
    const identity = readTransmissionIdentity();
    const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username,
      password: identity.password, timeoutMs: 5000, fetcher: boundedStewardshipFetch });
    return { clientId: `transmission:${identity.id}`, get: (ids, selected) => rpc.call("torrent-get", { ids, fields: selected }) };
  } catch { return null; }
}
function safeRelative(value: unknown): value is string {
  return typeof value === "string" && !path.isAbsolute(value) && !/[\\\x00-\x1f]/.test(value) &&
    value.split("/").every((part) => !!part && part !== "." && part !== "..");
}
export function stewardshipBinding(repo: BooksRepo, source: StewardshipSource, row: Record<string, unknown>, privateId: string): Binding | null {
  const settings = repo.readSettings();
  if (!settings || !clientIdSchema.safeParse(privateId).success || row.client_id !== privateId) return null;
  const hash = source === "books" ? row.info_hash : row.native_id;
  if (!hashSchema.safeParse(hash).success) return null;
  let media: Binding["media"], root: string, directory: string, rootToken: string | null = null, label: string;
  if (source === "books") {
    if (row.backend !== "transmission" || row.root_id !== "books" || row.relative_path !== hash ||
        typeof row.root_path !== "string" || !path.isAbsolute(row.root_path) || !z.string().uuid().safeParse(row.root_token).success) return null;
    media = "books"; root = row.root_path; directory = path.join(root, String(hash)); rootToken = String(row.root_token);
    label = `Book ${Number.isSafeInteger(row.book_id) ? row.book_id : "binding"}`;
  } else {
    let location: unknown;
    try { location = JSON.parse(String(row.location_json)); } catch { return null; }
    const parsed = z.object({ rootId: z.enum(["movies", "tv"]), relativePath: z.string().refine(safeRelative) }).strict().safeParse(location);
    if (!parsed.success) return null;
    media = parsed.data.rootId;
    const managedRoot = media === "movies" ? settings.movies.managedRoot : settings.tv.managedRoot;
    if (!managedRoot || !path.isAbsolute(managedRoot)) return null;
    root = path.join(managedRoot, "downloads"); directory = path.join(root, parsed.data.relativePath);
    label = `${media === "movies" ? "Movie" : "TV"} torrent ${row.id}`;
  }
  const key = digest([source, privateId, hash]);
  return { key, media, label, hash: String(hash), clientId: privateId, root, directory, rootToken,
    bindingDigest: digest([source, privateId, hash, root, directory, rootToken,
      source === "books" ? [row.book_id, row.release_id, settings.libraryRoot] : row.id]) };
}
export function stewardshipBindingPage(repo: BooksRepo, source: StewardshipSource, offset: number, limit: number, privateId: string | null) {
  const raw = repo.adminDbTablePage(source === "books" ? "book_torrents" : "media_torrents", limit, offset);
  const bindings = privateId ? raw.rows.flatMap((row) => { const parsed = stewardshipBinding(repo, source, row, privateId); return parsed ? [parsed] : []; }) : [];
  return { bindings, scannedCount: raw.rows.length, excludedCount: raw.rows.length - bindings.length,
    sourceRows: raw.total, nextOffset: offset + raw.rows.length < raw.total ? offset + raw.rows.length : null };
}
async function canonicalDirectory(directory: string) {
  const stat = await fs.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || await fs.realpath(directory) !== path.resolve(directory))
    throw new Error("Unverified private download directory");
}
async function marker(file: string): Promise<Record<string, unknown>> {
  const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > 4096) throw new Error("Unverified ownership marker");
    return z.record(z.string(), z.unknown()).parse(JSON.parse(await handle.readFile("utf8")));
  } finally { await handle.close(); }
}
export async function verifyStewardshipDirectory(repo: BooksRepo, target: Binding) {
  await canonicalDirectory(target.root); await canonicalDirectory(target.directory);
  if (target.media === "books") {
    const library = await fs.realpath(repo.readSettings()!.libraryRoot);
    if (target.root !== path.join(library, ".podible", "downloads")) throw new Error("Book root changed");
    const root = await marker(path.join(target.root, ".podible-content.json"));
    const owned = await marker(path.join(target.directory, ".podible-book-torrent.json"));
    if (root.id !== target.rootToken || owned.rootToken !== target.rootToken ||
        owned.clientId !== target.clientId || owned.infoHash !== target.hash) throw new Error("Book ownership changed");
  }
}
function emptyObservation(target: Binding, now: number, ownership: StewardshipObservation["ownership"], reason: string): StewardshipObservation {
  return { observedAt: new Date(now).toISOString(), bindingDigest: target.bindingDigest, ownership, state: "unknown",
    complete: null, bytesRemaining: null, daemonError: "unknown", trackerState: "unknown", trackerCount: 0, trackerErrorCount: 0,
    trackerUnknownCount: 0, trackerSetDigest: null, announcementTime: null, actualSeedSeconds: null,
    seedCounterRegressed: false, seedProtection: "unknown", remainingSeedSeconds: null,
    consistentUnregisteredAnnouncements: 0, repeatedUnregistered: false, reason };
}
function trackerHealth(value: unknown, now: number) {
  const parsed = z.array(trackerSchema).max(64).safeParse(value);
  const unknown = { trackerState: "unknown" as const, trackerCount: 0, trackerErrorCount: 0, trackerUnknownCount: 0,
    trackerSetDigest: null as string | null, announcementTime: null as number | null };
  if (!parsed.success || !parsed.data.length) return unknown;
  const trackers = parsed.data.filter((tracker) => !tracker.isBackup);
  if (!trackers.length || new Set(trackers.map((t) => t.id)).size !== trackers.length) return unknown;
  const states = trackers.map((tracker) => {
    const age = now / 1000 - tracker.lastAnnounceTime;
    if (!tracker.hasAnnounced || !tracker.lastAnnounceTime || age < 0 || age > 24 * 3600 || tracker.lastAnnounceTimedOut) return "unknown";
    if (tracker.lastAnnounceSucceeded) return "healthy";
    const message = tracker.lastAnnounceResult.trim();
    if (!message) return "unknown";
    if (/^(?:unregistered torrent|torrent (?:is )?not registered|torrent not found|torrent does not exist)(?:[.!]|$)/i.test(message)) return "confirmed-unregistered";
    return /^(?:invalid (?:passkey|authkey)|(?:passkey|authkey) (?:invalid|disabled)|(?:tracker )?permission denied|access denied|(?:your )?account (?:is )?(?:disabled|banned)|torrent banned)(?:[.!:]|$)/i.test(message)
      ? "confirmed-error" : "unknown";
  });
  return { trackerState: states.every((state) => state === "confirmed-unregistered") ? "confirmed-unregistered" as const
    : states.some((state) => state === "confirmed-error" || state === "confirmed-unregistered") ? "confirmed-error" as const
      : states.every((state) => state === "healthy") ? "healthy" as const : "unknown" as const,
    trackerCount: trackers.length, trackerErrorCount: states.filter((state) => state.startsWith("confirmed-")).length,
    trackerUnknownCount: states.filter((state) => state === "unknown").length,
    trackerSetDigest: digest(trackers.map((t) => [t.id, digest(t.announce)]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))),
    announcementTime: Math.min(...trackers.map((t) => t.lastAnnounceTime)),
  };
}
function observation(target: Binding, row: z.infer<typeof torrentSchema>, now: number, previous: StewardshipObservation | null): StewardshipObservation {
  const actualSeedSeconds = count.safeParse(row.secondsSeeding).success ? row.secondsSeeding as number : null;
  const regressed = actualSeedSeconds !== null && previous?.actualSeedSeconds !== null && previous?.actualSeedSeconds !== undefined && actualSeedSeconds < previous.actualSeedSeconds;
  const tracker = trackerHealth(row.trackerStats, now);
  const compatible = previous?.ownership === "verified-private-binding" && previous.bindingDigest === target.bindingDigest &&
    previous.trackerState === "confirmed-unregistered" && previous.trackerSetDigest === tracker.trackerSetDigest && !regressed &&
    tracker.announcementTime !== null && previous.announcementTime !== null && tracker.announcementTime >= previous.announcementTime &&
    now >= Date.parse(previous.observedAt) && now - Date.parse(previous.observedAt) <= 48 * 3600_000;
  let streak = 0;
  if (tracker.trackerState === "confirmed-unregistered") {
    streak = compatible ? previous!.consistentUnregisteredAnnouncements : 1;
    if (compatible && tracker.announcementTime! >= previous!.announcementTime! + ANNOUNCEMENT_SPACING_SECONDS) streak++;
  }
  const state: StewardshipObservation["state"] = row.status === 0 ? row.leftUntilDone === 0 ? "paused-complete" : "paused-incomplete"
    : row.status === 1 || row.status === 2 ? "checking" : row.status === 3 ? "download-queued"
      : row.status === 4 ? "downloading" : row.status === 5 ? "seed-queued" : "seeding";
  return { ...emptyObservation(target, now, "verified-private-binding", row.error === 3 ? "Daemon reports a local storage error; tracker state is separate."
    : tracker.trackerState === "confirmed-unregistered" ? "Current non-backup trackers explicitly reported an unregistered torrent."
      : tracker.trackerState === "confirmed-error" ? "Tracker response confirms an error; raw response text is withheld."
        : state === "paused-incomplete" ? "Owned torrent is paused with incomplete data; no control command was sent."
          : tracker.trackerState === "healthy" ? "Current non-backup tracker announcements succeeded." : "Tracker telemetry is unavailable, stale, timed out, or incomplete; no tracker failure is inferred."),
    ...tracker, state, complete: row.leftUntilDone === 0, bytesRemaining: row.leftUntilDone, actualSeedSeconds,
    daemonError: (["none", "tracker-warning", "tracker-error", "storage-error"] as const)[row.error]!,
    seedCounterRegressed: regressed, seedProtection: actualSeedSeconds === null ? "unknown" : actualSeedSeconds >= MINIMUM_SEED_SECONDS ? "minimum-met" : "protected",
    remainingSeedSeconds: actualSeedSeconds === null ? null : Math.max(0, MINIMUM_SEED_SECONDS - actualSeedSeconds),
    consistentUnregisteredAnnouncements: streak, repeatedUnregistered: streak >= 3,
  };
}
function saved(repo: BooksRepo, target: Binding) {
  const result = historySchema.safeParse(repo.getJsonState(PREFIX + "observation:" + target.key));
  return result.success && result.data.latest.bindingDigest === target.bindingDigest ? result.data : null;
}

/** Scheduling hint only; collection still verifies private-client ownership. Tombstones retain seed obligations. */
export function hasTorrentStewardshipBindings(repo: Pick<BooksRepo, "adminDbTablePage">): boolean {
  return repo.adminDbTablePage("book_torrents", 1, 0).total > 0 || repo.adminDbTablePage("media_torrents", 1, 0).total > 0;
}

export class TorrentStewardship {
  constructor(private repo: BooksRepo, private adapters: StewardshipAdapters = {}) {}
  status(input: z.input<typeof stewardshipStatusParamsSchema> = {}) {
    const params = stewardshipStatusParamsSchema.parse(input), now = (this.adapters.now ?? Date.now)();
    const engine = stewardshipConnection(this.adapters);
    const current = stewardshipBindingPage(this.repo, params.source, params.offset, params.limit, engine?.clientId ?? null);
    const last = stewardshipCollectResultSchema.safeParse(this.repo.getJsonState(lastKey));
    return stewardshipStatusSchema.parse({ mode: "observe-only", destructiveActionsEnabled: false, minimumSeedSeconds: MINIMUM_SEED_SECONDS,
      ...params, sourceRows: current.sourceRows, scannedCount: current.scannedCount, excludedCount: current.excludedCount,
      nextOffset: current.nextOffset, engineConfigured: !!engine,
      unavailableReason: engine ? null : "Private Transmission identity is unavailable; no daemon query was attempted.",
      lastCollection: last.success ? last.data : null, items: current.bindings.map((target) => {
        const latest = saved(this.repo, target)?.latest ?? null;
        return { key: target.key, media: target.media, label: target.label, observation: latest,
          stale: !latest || now < Date.parse(latest.observedAt) || now - Date.parse(latest.observedAt) > 60 * 60_000,
          retirementAllowed: false, retirementBlockers: ["Destructive actions are disabled.", "Shared file bindings and all tracker seed obligations are not proven safe to retire."],
        };
      }) });
  }
  async collect(input: z.input<typeof stewardshipCollectParamsSchema>): Promise<StewardshipCollectResult> {
    const params = stewardshipCollectParamsSchema.parse(input), receiptKey = PREFIX + "collection:" + params.operationId;
    const prior = stewardshipCollectResultSchema.safeParse(this.repo.getJsonState(receiptKey));
    if (prior.success) return prior.data;
    this.adapters.assertActive?.();
    const rawCursor = this.repo.getJsonState(cursorKey);
    const cursor = cursorSchema.safeParse(rawCursor);
    const currentCursor = cursor.success ? cursor.data : { source: "books" as const, offset: 0 };
    const engine = stewardshipConnection(this.adapters), now = (this.adapters.now ?? Date.now)();
    const current = stewardshipBindingPage(this.repo, currentCursor.source, currentCursor.offset, 50, engine?.clientId ?? null);
    let unavailableReason: string | null = engine ? null : "Private Transmission identity is unavailable; no daemon query was attempted.";
    let rows: z.infer<typeof torrentSchema>[] = [];
    if (engine && current.bindings.length) {
      try {
        const deadlineMs = z.number().int().min(1).max(15_000).parse(this.adapters.deadlineMs ?? 15_000);
        const signal = AbortSignal.timeout(deadlineMs);
        const result = await Promise.race([engine.get([...new Set(current.bindings.map((b) => b.hash))], fields),
          new Promise<never>((_resolve, reject) => signal.addEventListener("abort", () => reject(new Error("Observation deadline exceeded")), { once: true }))]);
        rows = z.array(torrentSchema).max(50).parse(result.torrents);
      } catch { unavailableReason = "Private Transmission telemetry is unavailable or unsupported; existing seed data and records were not changed."; }
    }
    this.adapters.assertActive?.();
    const observations = new Map<string, StewardshipObservation>();
    for (const target of current.bindings) {
      let observed = emptyObservation(target, now, "engine-unavailable", unavailableReason ?? "Private engine is unavailable.");
      if (!unavailableReason) {
        const matches = rows.filter((row) => row.hashString.toLowerCase() === target.hash);
        if (!matches.length) observed = emptyObservation(target, now, "missing-record", "The exact owned hash was absent from the daemon response; no replacement was added.");
        else {
          const row = matches[0]!;
          try {
            if (matches.length !== 1 || row.downloadDir !== target.directory || !row.labels.includes(`Podible ${target.media === "books" ? "Books" : target.media === "movies" ? "Movies" : "TV"}`)) throw new Error("Binding mismatch");
            await verifyStewardshipDirectory(this.repo, target);
            observed = observation(target, row, now, saved(this.repo, target)?.latest ?? null);
          } catch { observed = emptyObservation(target, now, "binding-unverified", "Daemon label, exact download layout or filesystem ownership no longer matches the private binding; health is not attributed to this record."); }
        }
      }
      observations.set(target.key, observed);
    }
    return this.repo.transaction(() => {
      this.adapters.assertActive?.();
      const replay = stewardshipCollectResultSchema.safeParse(this.repo.getJsonState(receiptKey));
      if (replay.success) return replay.data;
      const rechecked = stewardshipBindingPage(this.repo, currentCursor.source, currentCursor.offset, 50, engine?.clientId ?? null);
      const valid = new Map(rechecked.bindings.map((target) => [target.key, target]));
      let observedCount = 0;
      for (const target of current.bindings) {
        if (valid.get(target.key)?.bindingDigest !== target.bindingDigest) continue;
        const candidate = observations.get(target.key)!;
        const history = saved(this.repo, target);
        if (history && Date.parse(history.latest.observedAt) > now) continue;
        this.repo.setJsonState(PREFIX + "observation:" + target.key,
          { latest: candidate, history: [...(history?.history ?? []), candidate].slice(-8) });
        recordRetirementEvidence(this.repo, { bindingKey: target.key, bindingDigest: target.bindingDigest,
          observedAt: candidate.observedAt, ownershipVerified: candidate.ownership === "verified-private-binding",
          complete: candidate.complete === true && (() => {
            const row = rows.find((item) => item.hashString.toLowerCase() === target.hash);
            return !!row?.totalSize && row.haveValid === row.totalSize && row.sizeWhenDone === row.totalSize &&
              row.haveUnchecked === 0 && [0, 5, 6].includes(row.status) && row.error !== 3;
          })(), actualSeedSeconds: candidate.actualSeedSeconds,
          trackerStats: rows.find((row) => row.hashString.toLowerCase() === target.hash)?.trackerStats });
        observedCount++;
      }
      const result = stewardshipCollectResultSchema.parse({ ...params, collectedAt: new Date(now).toISOString(), ...currentCursor,
        observedCount, excludedCount: current.excludedCount, unavailableReason });
      this.repo.setJsonState(receiptKey, result);
      const last = stewardshipCollectResultSchema.safeParse(this.repo.getJsonState(lastKey));
      if (!last.success || Date.parse(last.data.collectedAt) <= now) this.repo.setJsonState(lastKey, result);
      if (engine && !unavailableReason && JSON.stringify(this.repo.getJsonState(cursorKey)) === JSON.stringify(rawCursor))
        this.repo.setJsonState(cursorKey, current.nextOffset !== null ? { source: currentCursor.source, offset: current.nextOffset }
          : { source: currentCursor.source === "books" ? "screen" : "books", offset: 0 });
      return result;
    });
  }
}
