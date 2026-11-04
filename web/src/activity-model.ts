import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";

type Ops = inferRouterOutputs<AppRouter>["adminWeb"]["ops"];
export type ActivityDownload = Ops["downloads"][number];

export function attentionItems(incidents: Ops["incidents"]) {
  return incidents.filter(incident => incident.recovery === "manual-retry" ||
    (incident.recovery === "unknown" && !["provider-cooldown", "provider-unavailable"].includes(incident.code)));
}

export function activityItems(data: Pick<Ops, "activeAcquisitions" | "downloads">) {
  const items = new Map<number, { work: Ops["activeAcquisitions"][number]; downloads: ActivityDownload[] }>();
  for (const work of data.activeAcquisitions) items.set(work.job.id, { work, downloads: [] });
  for (const download of data.downloads) {
    const item = items.get(download.job.id) ?? { work: download, downloads: [] };
    item.downloads.push(download);
    items.set(download.job.id, item);
  }
  return [...items.values()];
}

export function downloadProgress(downloads: ActivityDownload[]) {
  if (!downloads.length || downloads.some(d => d.telemetry === "unknown" || d.bytesDone === null ||
      d.sizeBytes === null || d.sizeBytes <= 0 || d.bytesDone > d.sizeBytes)) return null;
  const done = downloads.reduce((sum, d) => sum + d.bytesDone!, 0);
  const size = downloads.reduce((sum, d) => sum + d.sizeBytes!, 0);
  const speed = downloads.every(d => d.bytesPerSecond !== null) ? downloads.reduce((sum, d) => sum + d.bytesPerSecond!, 0) : null;
  if (![done, size, ...(speed === null ? [] : [speed])].every(Number.isSafeInteger)) return null;
  return { done, size, speed, live: downloads.every(d => d.telemetry === "live") };
}

export function problemAction(code: string) {
  if (["provider-configuration", "tracker-client-not-allowed"].includes(code))
    return { href: "/settings", label: "Review connection settings" };
  if (["storage-capacity", "storage-identity"].includes(code))
    return { href: "#activity-storage", label: "Review storage" };
  return null;
}
