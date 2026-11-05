import type { StewardshipStatus } from "../../src/media/torrent-stewardship-types";
import { stewardshipHealth } from "../../src/media/torrent-stewardship-types";

export type StewardshipItem = StewardshipStatus["items"][number];
export type StewardshipIssue = { title: string; cause: string; action: string; href: string };
const review = (item: StewardshipItem) => item.titles[0]?.href ?? "/ops";

export function stewardshipIssue(item: StewardshipItem): StewardshipIssue | null {
  const observation = item.observation;
  if (!observation) return { title: "Not verified yet", cause: "No current verified observation is available for this binding.", action: "Collect a fresh observation", href: "#stewardship-collection" };
  if (item.stale) return { title: "Observation out of date", cause: "The saved observation cannot establish the current torrent state.", action: "Collect a fresh observation", href: "#stewardship-collection" };
  if (observation.ownership === "engine-unavailable") return { title: "Private client unavailable", cause: "The private torrent client could not be reached or identified.", action: "Review Activity", href: "/ops" };
  if (observation.ownership === "missing-record") return { title: "Torrent record not found", cause: "The exact owned record was absent from the last client response. No replacement was added.", action: "Review title", href: review(item) };
  if (observation.ownership !== "verified-private-binding") return { title: "Ownership could not be verified", cause: "The client, download path or filesystem ownership no longer matches this binding.", action: "Review storage", href: "/ops#activity-storage" };
  if (observation.daemonError === "storage-error") return { title: "Storage problem", cause: "The private client reported a local storage error. Check storage access and free space before retrying.", action: "Review storage", href: "/ops#activity-storage" };
  if (observation.trackerState === "confirmed-unregistered") return { title: "Torrent registration missing", cause: "A recent tracker response explicitly reported this torrent as unregistered. Records and seed data have been retained.", action: "Review title", href: review(item) };
  if (observation.trackerState === "confirmed-error") return { title: "Tracker rejected the request", cause: "A recent tracker response confirmed an access or policy error. Check account requirements and configured credentials.", action: "Review connection settings", href: "/settings" };
  if (observation.state === "paused-incomplete") return { title: "Download paused", cause: "The owned torrent is paused before all data has arrived. Review the existing acquisition before starting recovery.", action: "Review title", href: review(item) };
  if (observation.seedCounterRegressed) return { title: "Seed time needs verification", cause: "The client's seed counter decreased; previous repeated-error evidence was reset.", action: "Collect a fresh observation", href: "#stewardship-collection" };
  if (observation.trackerState === "unknown" || observation.daemonError !== "none" || observation.state === "unknown")
    return { title: "Torrent health unknown", cause: "Current tracker or client evidence is incomplete. A tracker failure has not been confirmed.", action: "Collect a fresh observation", href: "#stewardship-collection" };
  if (observation.actualSeedSeconds === null || observation.seedProtection === "unknown")
    return { title: "Seed time unknown", cause: "Actual seeding time could not be verified, so seed obligations remain protected.", action: "Collect a fresh observation", href: "#stewardship-collection" };
  return null;
}

export function stewardshipView(items: readonly StewardshipItem[]) {
  const problems: StewardshipItem[] = [], unknown: StewardshipItem[] = [];
  let healthyCount = 0;
  for (const item of items) {
    const health = stewardshipHealth(item);
    if (health === "healthy") healthyCount++;
    else if (health === "problem") problems.push(item);
    else unknown.push(item);
  }
  return { problems, unknown, healthyCount };
}
