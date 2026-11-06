import { z } from "zod";

const count = z.number().int().nonnegative().safe();
const value = count.nullable();
export const TRANSFER_METRICS_INTERVAL_MS = 60_000;
export const transferMetricsStatusParamsSchema = z.object({ hours: z.union([z.literal(24), z.literal(168), z.literal(720), z.literal(2160)]).default(24) }).strict();
export const transferMetricsStatusSchema = z.object({
  trackingSince: z.string().nullable(), observedAt: z.string().nullable(), stale: z.boolean(), unavailableReason: z.string().nullable(),
  summary: z.object({ seedBytes: value, seedingTorrents: value, uploadedBytes: value, downloadedBytes: value,
    uploadBytesPerSecond: value, downloadBytesPerSecond: value, crossSeedUploadedBytes: value, seedSeconds: value,
    contentUnits: count, averageCopies: z.number().nullable() }).nullable(),
  trackers: z.array(z.object({ host: z.string(), name: z.string(), seedBytes: value, seedingTorrents: value,
    uploadedBytes: value, downloadedBytes: value, seedSeconds: value, ratio: z.number().nullable(),
    uploadBytesPerSecond: value.default(null), downloadBytesPerSecond: value.default(null) })),
  history: z.array(z.object({ at: z.string(), seedBytes: value, seedingTorrents: value, uploadedBytes: value,
    downloadedBytes: value, uploadBytesPerSecond: value, downloadBytesPerSecond: value,
    crossSeedUploadedBytes: value, seedSeconds: value, averageCopies: z.number().nullable(), reusedBytes: count,
    trackers: z.array(z.object({ host: z.string(), name: z.string(), seedBytes: value, seedingTorrents: value,
      uploadedBytes: value, downloadedBytes: value, seedSeconds: value, ratio: z.number().nullable(),
      uploadBytesPerSecond: value.default(null), downloadBytesPerSecond: value.default(null) })) })),
  reuse: z.object({ attempts: count, successful: count, pending: count, unsupported: count, matchRate: z.number().nullable(),
    verifiedReusedBytes: count, sidecarDownloadBytes: value, contentRepairBytes: value, tvMultiFileReusedBytes: count }),
  downloads: z.array(z.object({ id: count, title: z.string(), state: z.string(), bytesDone: value, sizeBytes: value,
    downloadBytesPerSecond: value, href: z.string().nullable() })),
  mappings: z.array(z.object({ host: z.string(), name: z.string(), source: z.enum(["inferred", "unknown"]) })),
  warnings: z.array(z.string()),
});
export type TransferMetricsStatus = z.infer<typeof transferMetricsStatusSchema>;
