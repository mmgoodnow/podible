import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { jobJournalReader } from "../../src/worker/job-journal";

test("provider waits come from exact sleeping steps in the pinned real journal, never arbitrary errors or completed sleeps", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "job-waits-")), filename = path.join(root, "execution.sqlite");
  const storage = new SQLiteStorage(filename);
  try {
    await storage.initialize();
    const runId = randomUUID(), otherId = randomUUID(), now = Date.now();
    for (const id of [runId, otherId]) await storage.createRun({ id, workflow: "fixture", input: {}, status: "sleeping",
      idempotencyKey: id, createdAt: now, updatedAt: now, startedAt: now, finishedAt: null, output: null,
      error: null, wakeAt: now + 60_000, waitingFor: null });
    const steps = [
      { name: "audio:search:wait:1", kind: "sleep", status: "sleeping", runId },
      { name: "audio:search:wait:2", kind: "sleep", status: "sleeping", runId },
      { name: "ebook:search:wait:0", kind: "sleep", status: "completed", runId },
      { name: "ebook:search:wait:1", kind: "step", status: "sleeping", runId },
      { name: "secret-api-key", kind: "sleep", status: "sleeping", runId },
      { name: "ebook:search:wait:3", kind: "sleep", status: "sleeping", runId: otherId },
    ] as const;
    for (const step of steps) await storage.saveStepResult({ id: randomUUID(), ...step, output: null, error: null,
      attempts: 0, createdAt: now, updatedAt: now, startedAt: now, history: [] });
    const reader = jobJournalReader(filename);
    expect(reader.waits!([runId])).toEqual(new Map([[runId, [{ reason: "provider-search", media: "audio" }]]]));
    expect(reader.waits!([otherId])).toEqual(new Map([[otherId, [{ reason: "provider-search", media: "ebook" }]]]));
    expect(reader.waits!([]).size).toBe(0);
    expect(() => reader.waits!(Array(101).fill(runId))).toThrow("budget exceeded");
    expect((await storage.getStepResults(runId)).length).toBe(5);
  } finally { await storage.close(); await rm(root, { recursive: true, force: true }); }
});
