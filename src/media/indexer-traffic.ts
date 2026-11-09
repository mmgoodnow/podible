import type { BooksRepo } from "../repo";

export type TrafficEvent = "search_id" | "search_text" | "metainfo_fetch" | "metainfo_failure" |
  "metainfo_cache_hit" | "metainfo_coalesced" | "metainfo_prefiltered";
export type TrafficPurpose = "inspection" | "acquisition" | "reuse" | "preview" | "search";
const key = "indexer-traffic:counters";

export function recordIndexerTraffic(repo: BooksRepo, purpose: TrafficPurpose, event: TrafficEvent): void {
  repo.transaction(() => {
    const counts = repo.getJsonState<Record<string, number>>(key) ?? {};
    const label = purpose + ":" + event;
    counts[label] = (counts[label] ?? 0) + 1;
    repo.setJsonState(key, counts);
  });
}

export function indexerTrafficPrometheus(repo: BooksRepo): string {
  const counts = repo.getJsonState<Record<string, number>>(key) ?? {};
  return "# HELP podible_indexer_traffic_total Indexer search and metainfo events; cache hits and coalesced requests are not upstream requests.\n" +
    "# TYPE podible_indexer_traffic_total counter\n" +
    Object.entries(counts).map(([label, value]) => {
      const [purpose, event] = label.split(":");
      return `podible_indexer_traffic_total{purpose="${purpose}",event="${event}"} ${value}\n`;
    }).join("");
}
