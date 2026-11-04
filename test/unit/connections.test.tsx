/// <reference path="../../web/src/css.d.ts" />
import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { Connections, connectionProviders, type ConnectionFeed } from "../../web/src/connections";

const now = "2026-11-04T12:00:00Z";
function feed(overrides: Partial<ConnectionFeed> = {}): ConnectionFeed {
  return { id: "one", provider: "A source", mediaType: "movie", state: "healthy", lastSuccessAt: now,
    lastPolledAt: now, retryAt: null, nextPollAt: null, error: null, gap: false, ...overrides };
}
function render(feeds: ConnectionFeed[], truncated = false) {
  return renderToStaticMarkup(createElement(Connections, { feeds, observedAt: now, truncated }));
}

test("one provider has one status summary, not one availability segment per media feed", () => {
  const feeds = [feed(), feed({ id: "two", mediaType: "tv" }), feed({ id: "three", mediaType: "audio" })];
  const providers = connectionProviders(feeds);
  expect(providers).toHaveLength(1);
  expect(providers[0]!.feeds).toHaveLength(3);
  expect(providers[0]!.label).toBe("Success recorded");
  const html = render(feeds);
  expect(html).toContain("1 provider");
  expect(html.match(/class="connections-provider"/g)).toHaveLength(1);
  expect(html.match(/Success recorded/g)).toHaveLength(1);
  expect(html).toContain("Feed details (3)");
  expect(html).not.toMatch(/\bonline\b|\buptime\b|\brate.limited\b/i);
});

test("mixed saved observations remain mixed even if another media feed succeeded later", () => {
  const providers = connectionProviders([
    feed({ state: "cooldown", retryAt: "2026-11-04T13:00:00Z", lastPolledAt: "2026-11-04T11:00:00Z" }),
    feed({ id: "two", mediaType: "tv", lastPolledAt: now }),
  ]);
  expect(providers[0]).toMatchObject({ label: "Mixed reports", mixed: true, tone: "cooldown", lastPollAt: now });
  expect(providers[0]!.states).toEqual(["cooldown", "healthy"]);
  const html = render(providers[0]!.feeds);
  expect(html).toContain("Snapshots may differ in age.");
  expect(providers[0]!.reports).toEqual([{ state: "cooldown", count: 1 }, { state: "healthy", count: 1 }]);
  expect(html).toContain("1 cooldown");
  expect(html).toContain("1 success");
  const summary = html.slice(html.indexOf("<summary>"), html.indexOf("</summary>"));
  expect(summary).not.toContain("Snapshots");
  expect(html).not.toContain("Provider down");
});

test("a successful transport observation with a history gap does not become a provider outage", () => {
  const observed = feed({ gap: true });
  const provider = connectionProviders([observed])[0]!;
  expect(provider).toMatchObject({ label: "Success recorded", tone: "healthy", gapCount: 1 });
  const html = render([observed]);
  expect(html).toContain("1 feed gap");
  expect(html).toContain("1 with feed gaps");
  expect(html).not.toContain("with poll errors");
  expect(html).not.toContain("Cooling down");
});

test("problem-first ordering prioritizes poll errors, cooldowns and gaps without mutating inputs", () => {
  const feeds = [feed({ provider: "Success" }), feed({ provider: "Unknown", state: "unknown" }),
    feed({ provider: "Cooldown", state: "cooldown" }), feed({ provider: "Gapped", gap: true }),
    feed({ provider: "Error", state: "unavailable" })];
  const before = structuredClone(feeds);
  expect(connectionProviders(feeds).map(provider => provider.name)).toEqual(["Error", "Gapped", "Cooldown", "Unknown", "Success"]);
  expect(feeds).toEqual(before);
});

test("overview problem counts count providers, not duplicated media feed observations", () => {
  const error = { code: "provider-unavailable", message: "A saved poll failed." };
  const html = render([feed({ state: "unavailable", error }), feed({ id: "two", mediaType: "tv", state: "unavailable", error })]);
  expect(html).toContain("1 with poll errors");
  expect(html).not.toContain("2 with poll errors");
  expect(html.match(/A saved poll failed\./g)).toHaveLength(1);
  expect(html).toContain("Recorded for Movies, TV.");
});

test("feed drilldown preserves independent attempts, successes, retry boundaries and gaps", () => {
  const feeds = [feed({ gap: true, retryAt: "2026-11-04T13:00:00Z" }),
    feed({ id: "two", mediaType: "tv", lastPolledAt: "2026-11-04T11:30:00Z", lastSuccessAt: null, nextPollAt: "2026-11-04T14:00:00Z" })];
  const html = render(feeds);
  for (const value of ["2026-11-04T13:00:00Z", "2026-11-04T11:30:00Z", "2026-11-04T14:00:00Z"])
    expect(html).toContain(`dateTime="${value}"`);
  expect(html).toContain("aria-label=\"Movies feed checkpoints\"");
  expect(html).toContain("aria-label=\"TV feed checkpoints\"");
  expect(html).toContain("History gap recorded. Some feed history could not be verified.");
  expect(html).toContain("No history gap recorded.");
  expect(html.match(/A successful poll does not prove complete feed history\./g)).toHaveLength(1);
  expect(html).toContain("Not recorded");
  expect(html).toContain("Shared feed sync wake");
});

test("unknown and empty inventory never appear healthy; absent or invalid times are not invented", () => {
  const html = render([feed({ state: "unknown", lastPolledAt: "invalid", lastSuccessAt: null })]);
  expect(html).toContain("Unknown");
  expect(html).not.toContain("Invalid Date");
  expect(html).not.toContain("Success recorded");
  expect(render([])).toContain("No saved feed observations. Provider availability is unknown.");
  expect(render([])).toContain("0 providers");
});

test("bounded inventory is visibly incomplete before any provider disclosure", () => {
  const html = render([feed()], true);
  expect(html).toContain("1 provider shown");
  expect(html.indexOf("some provider observations may not be shown")).toBeLessThan(html.indexOf("connections-grid"));
});

test("native provider disclosure keeps summary facts visible and safely escapes source text", () => {
  const html = render([feed({ provider: "<script>unsafe</script>", state: "cooldown" })]);
  const summary = html.slice(html.indexOf("<summary>"), html.indexOf("</summary>"));
  expect(summary).toContain("&lt;script&gt;unsafe&lt;/script&gt;");
  expect(summary).toContain("Cooling down");
  expect(summary).toContain("Feed details (1)");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("<details open");
  expect(html).toContain("does not establish the upstream cause or prove rate limiting");
  expect(html).toContain("href=\"/settings\"");
});
