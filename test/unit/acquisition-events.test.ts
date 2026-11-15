import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { AcquisitionEvents } from "../../src/runtime/acquisition-events";
import { CompletionSignals } from "../../src/runtime/completion-signals";
import { plexPublicationChanged, watchPlexCompletions } from "../../src/runtime/plex-completion";

test("an unacknowledged completion remains deliverable after a dispatch error", async () => {
  const db = new Database(":memory:");
  try {
    const send = new CompletionSignals(db, async () => { throw new Error("unavailable"); });
    send.arm("left", "a", "torrent:a");
    send.arm("right", "b", "torrent:b");
    await expect(send.signal("torrent:a")).rejects.toThrow("unavailable");
    const sent: string[][] = [];
    const recovered = new CompletionSignals(db, async (...args) => { sent.push(args); });
    await recovered.signal("torrent:a");
    await recovered.signal("torrent:a");
    expect(sent).toEqual([["left", "a"]]);
    await recovered.signal("torrent:", true);
    expect(sent).toEqual([["left", "a"], ["right", "b"]]);
  } finally { db.close(); }
});

test("Plex wakes publications after scans and scoped terminal video updates even during media analysis", () => {
  const activity = { type: "library.update.section", Context: { librarySectionID: 16 } };
  expect(plexPublicationChanged({ ActivityNotification: { event: "ended", Activity: activity } })).toEqual({ section: "16" });
  expect(plexPublicationChanged({ ActivityNotification: { event: "updated", Activity: activity } })).toBeNull();
  expect(plexPublicationChanged({ ActivityNotification: { event: "ended", Activity: { type: "butler" } } })).toBeNull();
  expect(plexPublicationChanged({ ActivityNotification: { event: "ended", Activity: { type: "library.update.section" } } })).toEqual({ section: "" });
  const update = { sectionID: "16", itemID: "5", type: 4, state: 5 };
  expect(plexPublicationChanged({ TimelineEntry: update })).toEqual({ section: "16" });
  expect(plexPublicationChanged({ TimelineEntry: { ...update, sectionID: 15, type: 1 } })).toEqual({ section: "15" });
  expect(plexPublicationChanged({ TimelineEntry: { ...update, state: 0, metadataState: "created" } })).toBeNull();
  expect(plexPublicationChanged({ TimelineEntry: { ...update, type: 8 } })).toBeNull();
  expect(plexPublicationChanged({ TimelineEntry: { ...update, type: 1, mediaState: "thumbnailing" } })).toEqual({ section: "16" });
  expect(plexPublicationChanged({ TimelineEntry: { ...update, type: 1, state: 4, mediaState: "thumbnailing" } })).toBeNull();
  expect(plexPublicationChanged({ TimelineEntry: { ...update, type: 1, state: 4, metadataState: "loading", mediaState: "thumbnailing" } }))
    .toEqual({ section: "16", settle: true });
  expect(plexPublicationChanged({ TimelineEntry: { ...update, sectionID: undefined } })).toBeNull();
});

test("a burst of Plex metadata loading events schedules one early wake without waiting for thumbnails or a terminal event", async () => {
  const wake = Promise.withResolvers<void>();
  const calls: string[] = [];
  const event = 'event: timeline\ndata: {"TimelineEntry":{"sectionID":"15","type":1,"state":4,"metadataState":"loading","mediaState":"thumbnailing"}}\n\n';
  const fetcher = (async (_input: URL, init: RequestInit) => new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(event.repeat(5)));
      init.signal!.addEventListener("abort", () => controller.close(), { once: true });
    },
  }), { headers: { "Content-Type": "text/event-stream" } })) as typeof fetch;
  const watcher = watchPlexCompletions(async (source, prefix) => {
    calls.push(source);
    if (!prefix) wake.resolve();
  }, () => {}, fetcher);
  const timer = setTimeout(() => wake.reject(new Error("No early metadata wake")), 1500);
  try {
    watcher.configure({ url: "http://plex", token: "test" });
    await wake.promise;
    expect(calls).toEqual(["plex:", "plex:15"]);
  } finally {
    clearTimeout(timer);
    await watcher.stop();
  }
});

test("a Plex timeline event wakes a pending publication after the scan event has already been consumed", async () => {
  const db = new Database(":memory:");
  const sent: string[][] = [];
  const ready = Promise.withResolvers<void>();
  const inbox = new CompletionSignals(db, async (run, slot) => {
    sent.push([run, slot]);
    if (slot === "exact-recheck") ready.resolve();
  });
  const requestFilters: (string | null)[] = [];
  const fetcher = (async (input: URL) => {
    requestFilters.push(input.searchParams.get("filters"));
    return new Response(new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode([
          'event: activity\ndata: {"ActivityNotification":{"event":"ended","Activity":{"type":"library.update.section","Context":{"librarySectionID":16}}}}\n\n',
          'event: timeline\ndata: {"TimelineEntry":{"sectionID":"15","type":4,"state":5}}\n\n',
          'event: timeline\ndata: {"TimelineEntry":{"sectionID":"16","type":4,"state":4,"mediaState":"analyzing"}}\n\n',
          'event: timeline\ndata: {"TimelineEntry":{"sectionID":"16","type":4,"state":5,"mediaState":"thumbnailing"}}\n\n',
        ].join("")));
        controller.close();
      },
    }), { headers: { "Content-Type": "text/event-stream" } });
  }) as typeof fetch;
  // Re-arm as a publish attempt would: reconnect, scan ended, then exact metadata recheck.
  const wakes: string[] = [];
  const watcher = watchPlexCompletions(async (source, prefix) => {
    wakes.push(source);
    await inbox.signal(source, prefix);
    if (prefix) inbox.arm("tv", "after-scan", "plex:16");
    else if (source === "plex:16" && sent.length === 1) inbox.arm("tv", "exact-recheck", "plex:16");
  }, () => {}, fetcher);
  const timer = setTimeout(() => ready.reject(new Error("No scoped Plex metadata wake")), 1000);
  try {
    watcher.configure({ url: "http://plex", token: "test" });
    await ready.promise;
    expect(requestFilters).toEqual(["activity,timeline"]);
    expect(wakes).toEqual(["plex:", "plex:16", "plex:15", "plex:16"]);
    expect(sent).toEqual([["tv", "after-scan"], ["tv", "exact-recheck"]]);
  } finally {
    clearTimeout(timer);
    await watcher.stop();
    db.close();
  }
});

test("browser gets an invalidation on every connection and after an update during backpressure", async () => {
  const events = new AcquisitionEvents();
  const abort = new AbortController();
  const reader = events.changes(abort.signal);
  expect((await reader.next()).value).toEqual({ type: "change" });
  events.notify();
  events.notify();
  expect((await reader.next()).value).toEqual({ type: "change" });
  const idle = reader.next();
  abort.abort();
  expect((await idle).done).toBe(true);
  const reconnect = events.changes(new AbortController().signal);
  expect((await reconnect.next()).value).toEqual({ type: "change" });
  const disconnected = reconnect.next();
  events.close();
  await expect(disconnected).rejects.toMatchObject({ code: "SERVICE_UNAVAILABLE" });
});
