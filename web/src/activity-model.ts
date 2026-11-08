import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";

type Ops = inferRouterOutputs<AppRouter>["adminWeb"]["ops"];
export type ActivityDownload = Ops["downloads"][number];
export type ActivityWork = Ops["activeAcquisitions"][number];

/** Worker sleeps describe polling/retry mechanics, not whether the torrent client is doing work. */
export function activityPhase(work: ActivityWork, downloads: ActivityDownload[]) {
  const live = downloads.filter(d => d.telemetry === "live");
  const transferring = live.some(d => d.state === "downloading" || (d.state == null && (d.bytesPerSecond ?? 0) > 0));
  const checking = live.some(d => d.state === "checking");
  const publication = work.stage === "publishing" || work.stage === "publication-queued" || work.stage === "available";
  const labels: Record<string, string> = { searching: "Finding a release", selecting: "Choosing a release", replacing: "Trying another release",
    verifying: "Checking episodes", publishing: "Publication pending", "publication-queued": "Waiting to publish", available: "Confirming availability", retained: "Finishing up",
    downloading: "Downloading", "pack-pending": "Waiting for pack to finish", blocked: "Waiting for a suitable release" };
  const label = publication ? labels[work.stage!]! : transferring ? "Downloading" : checking ? "Checking download"
    : live.some(d => d.state === "download-queued") ? "Download queued" : work.job.recovery === "automatic-retry" ? "Retry scheduled"
    : work.stage === "downloading" ? "Waiting for download status" : labels[work.stage ?? ""] ?? (work.job.recovery === "running" ? "Working" : "Queued");
  const active = transferring || checking || work.job.recovery === "running" || work.stage === "verifying";
  return { label, active, showWake: !transferring && !checking && work.stage !== "verifying",
    transferLabel: publication ? "Other attached downloads" : "Download progress" };
}

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
