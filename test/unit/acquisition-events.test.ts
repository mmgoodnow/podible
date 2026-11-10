import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { AcquisitionEvents } from "../../src/http/acquisition-events";
import { CompletionSignals } from "../../src/runtime/completion-signals";
import { plexScanEnded } from "../../src/runtime/plex-completion";

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

test("Plex only wakes publications for scan completions, scoped where possible", () => {
  const activity = { type: "library.update.section", Context: { librarySectionID: 16 } };
  expect(plexScanEnded({ ActivityNotification: { event: "ended", Activity: activity } })).toBe("16");
  expect(plexScanEnded({ ActivityNotification: { event: "updated", Activity: activity } })).toBeNull();
  expect(plexScanEnded({ ActivityNotification: { event: "ended", Activity: { type: "butler" } } })).toBeNull();
  expect(plexScanEnded({ ActivityNotification: { event: "ended", Activity: { type: "library.update.section" } } })).toBe("");
});

test("browser gets an invalidation on every connection and after an update during backpressure", async () => {
  const events = new AcquisitionEvents();
  const abort = new AbortController();
  const reader = events.response(abort.signal).body!.getReader();
  events.notify();
  expect(new TextDecoder().decode((await reader.read()).value)).toContain("event: change");
  expect(new TextDecoder().decode((await reader.read()).value)).toContain("event: change");
  abort.abort();
  expect((await reader.read()).done).toBe(true);
  const reconnect = events.response(new AbortController().signal).body!.getReader();
  expect(new TextDecoder().decode((await reconnect.read()).value)).toContain("event: change");
  events.close();
  expect((await reconnect.read()).done).toBe(true);
});
