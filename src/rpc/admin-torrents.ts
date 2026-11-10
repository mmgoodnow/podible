import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { AppSettings } from "../app-types";
import { readTransmissionIdentity } from "../media/managed-transmission";
import { TransmissionRpc } from "../media/transmission-client";

export type AdminTransmissionReader = { clientId: string; rpc: Pick<TransmissionRpc, "call"> };
export type AdminTorrentBinding = {
  id: number; client_id: string; native_id: string; location_json: string;
};
export type AdminTorrentSource = { media: "movie" | "tv"; binding: AdminTorrentBinding };
export type AdminTorrentObservation = {
  active: boolean; bytesDone: number; sizeBytes: number; bytesPerSecond: number; observedAt: string; error: string | null;
  seedOnly?: boolean;
  state: "checking" | "download-queued" | "downloading" | "stopped" | "seeding";
};
const count = z.number().int().nonnegative().safe();
const rowSchema = z.object({
  hashString: z.string().regex(/^[a-f0-9]{40}$/i), downloadDir: z.string(), labels: z.array(z.string()),
  status: z.number().int().min(0).max(6), leftUntilDone: count, sizeWhenDone: count, rateDownload: count,
  error: count, errorString: z.string(),
}).refine((row) => row.leftUntilDone <= row.sizeWhenDone);
const locationSchema = z.object({ rootId: z.enum(["movies", "tv"]), relativePath: z.string().min(1) }).strict();
const limit = 100;
export const adminTorrentKey = (source: AdminTorrentSource) => `${source.media}:${source.binding.id}`;

/** A read-only projection: no saved observation can establish current ownership or activity. */
export async function readAdminTorrents(
  sources: AdminTorrentSource[], settings: AppSettings, now: number, injected?: AdminTransmissionReader | null,
): Promise<{ observations: Map<string, AdminTorrentObservation>; unknown: number; truncated: boolean }> {
  const unique = [...new Map(sources.map((source) => [adminTorrentKey(source), source])).values()];
  const observations = new Map<string, AdminTorrentObservation>();
  if (!unique.length) return { observations, unknown: 0, truncated: false };
  // Do not guess which binding is current by newest ID when the read budget is exceeded.
  if (unique.length > limit) return { observations, unknown: unique.length, truncated: true };
  try {
    let reader = injected;
    if (reader === undefined) {
      const identity = readTransmissionIdentity();
      reader = { clientId: `transmission:${identity.id}`, rpc: new TransmissionRpc({
        url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password, timeoutMs: 1500,
      }) };
    }
    if (!reader || !reader.clientId.startsWith("transmission:") || !z.string().uuid().safeParse(reader.clientId.slice(13)).success)
      return { observations, unknown: unique.length, truncated: false };
    const owned: { source: AdminTorrentSource; directory: string; label: string }[] = [];
    for (const source of unique) {
      try {
        const binding = source.binding;
        if (binding.client_id !== reader.clientId || !/^[a-f0-9]{40}$/.test(binding.native_id)) continue;
        const location = locationSchema.parse(JSON.parse(binding.location_json));
        if (location.rootId !== (source.media === "movie" ? "movies" : "tv") || path.isAbsolute(location.relativePath) ||
            /[\\\x00-\x1f\x7f]/.test(location.relativePath) || location.relativePath.split("/").some((part) => !part || part === "." || part === "..")) continue;
        const root = source.media === "movie" ? settings.movies.managedRoot : settings.tv.managedRoot;
        if (!root || !path.isAbsolute(root) || path.resolve(root) !== root || await fs.realpath(root) !== root) continue;
        const downloads = path.join(root, "downloads"), directory = path.join(downloads, location.relativePath);
        if (await fs.realpath(downloads) !== downloads || await fs.realpath(directory) !== directory) continue;
        owned.push({ source, directory, label: source.media === "movie" ? "Podible Movies" : "Podible TV" });
      } catch { /* Missing/noncanonical mounts and invalid bindings stay unverified. */ }
    }
    if (!owned.length) return { observations, unknown: unique.length, truncated: false };
    const ids = [...new Set(owned.map(({ source }) => source.binding.native_id))];
    const result = await reader.rpc.call("torrent-get", { ids, fields: [
      "hashString", "downloadDir", "labels", "status", "leftUntilDone", "sizeWhenDone", "rateDownload", "error", "errorString",
    ] });
    const rows = z.array(rowSchema).max(limit).parse(result.torrents);
    for (const { source, directory, label } of owned) {
      const matches = rows.filter((row) => row.hashString.toLowerCase() === source.binding.native_id);
      if (matches.length !== 1) continue;
      const row = matches[0]!;
      if (row.downloadDir !== directory || !row.labels.includes(label) || await fs.realpath(directory).catch(() => null) !== directory) continue;
      observations.set(adminTorrentKey(source), {
        active: [1, 2, 3, 4].includes(row.status) && (row.leftUntilDone > 0 || row.status === 1 || row.status === 2),
        bytesDone: row.sizeWhenDone - row.leftUntilDone, sizeBytes: row.sizeWhenDone, bytesPerSecond: row.rateDownload,
        observedAt: new Date(now).toISOString(), error: row.error ? row.errorString || "Torrent engine reported a tracker or storage error" : null,
        ...(row.labels.includes("Podible Seed Only") ? { seedOnly: true } : {}),
        state: row.status === 1 || row.status === 2 ? "checking" : row.status === 3 ? "download-queued" : row.status === 4 ? "downloading" : row.status === 5 || row.status === 6 ? "seeding" : "stopped",
      });
    }
  } catch { /* Transport failures never promote recorded activity/errors into current facts. */ }
  return { observations, unknown: unique.length - observations.size, truncated: false };
}
