import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StewardshipPage } from "../../web/src/stewardship";
import {
  MINIMUM_SEED_SECONDS, stewardshipStatusSchema, type StewardshipPanelApi,
} from "../../src/media/torrent-stewardship-types";

test("stewardship panel renders saved unknown/paused health and 30-day protection without collecting on render", () => {
  let collected = 0, reads = 0;
  const status = stewardshipStatusSchema.parse({
    mode: "observe-only", destructiveActionsEnabled: false, minimumSeedSeconds: MINIMUM_SEED_SECONDS,
    source: "books", offset: 0, sourceRows: 1, scannedCount: 1, excludedCount: 0, nextOffset: null,
    engineConfigured: true, unavailableReason: null, lastCollection: null,
    items: [{ key: "a".repeat(64), media: "books", label: "Book 1", stale: false, retirementAllowed: false,
      retirementBlockers: ["Disabled"], observation: {
        observedAt: "2026-11-04T12:00:00.000Z", bindingDigest: "b".repeat(64), ownership: "verified-private-binding",
        state: "paused-incomplete", complete: false, bytesRemaining: 123, daemonError: "none",
        trackerState: "unknown", trackerCount: 0, trackerErrorCount: 0, trackerUnknownCount: 0,
        trackerSetDigest: null, announcementTime: null, actualSeedSeconds: null, seedCounterRegressed: false,
        seedProtection: "unknown", remainingSeedSeconds: null, consistentUnregisteredAnnouncements: 0,
        repeatedUnregistered: false, reason: "Tracker telemetry is unknown; no failure is inferred.",
      },
    }],
  });
  const api: StewardshipPanelApi = { async status() { reads++; return status; },
    async collect() { collected++; throw new Error("Render must never collect"); } };
  const cache = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  cache.setQueryData(["torrent-stewardship", "all", "attention", 0], status);
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client: cache }, createElement(StewardshipPage, { api })));
  expect(html).toContain("Torrent health"); expect(html).toContain("paused incomplete");
  expect(html).toContain("Unknown seed time; protected"); expect(html).toContain("Download paused");
  expect(html).toContain("Meeting the minimum seed time does not prove every tracker obligation is satisfied");
  expect(html).toContain("Collect next batch"); expect(html).toContain("Refresh saved observations");
  expect(collected).toBe(0); expect(reads).toBe(0);
  cache.clear();
});
