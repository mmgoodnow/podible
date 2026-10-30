import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { runMigrations } from "../../src/db";
import { processChapterAnalysisJob, loadStoredManifestationTranscriptPayload } from "../../src/library/chapter-analysis";
import { chapterArtifactStep } from "../../src/library/chapter-execution";
import type { ChapterCurationContext } from "../../src/library/chapter-curation-tools";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import type { WorkerContext } from "../../src/worker/context";

// File-backed journal models replay of completed steps without coupling these
// domain acceptance tests to the worker/runtime's own lease tests.
function journal(db: Database, afterStep?: (name: string) => void) {
  db.run("CREATE TABLE IF NOT EXISTS chapter_test_steps (name TEXT PRIMARY KEY, value TEXT NOT NULL)");
  let active = true;
  const execution: NonNullable<WorkerContext["execution"]> = {
    key: "chapter-test-run",
    assertActive() { if (!active) throw new Error("stale chapter owner"); },
    async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
      execution.assertActive();
      const saved = db.query("SELECT value FROM chapter_test_steps WHERE name = ?").get(name) as { value: string } | null;
      if (saved) return JSON.parse(saved.value).value as T;
      const value = await handler();
      execution.assertActive();
      db.run("INSERT INTO chapter_test_steps VALUES (?, ?)", [name, JSON.stringify({ value })]);
      afterStep?.(name);
      return value;
    },
    async sleep() {},
  };
  return { execution, cancel() { active = false; } };
}

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), "podible-chapter-durable-"));
  const dbPath = path.join(root, "library.sqlite");
  const db = new Database(dbPath);
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Durable Chapters", author: "Test" });
  const manifestation = repo.addManifestation({ bookId: book.id, kind: "audio" });
  for (let i = 0; i < 2; i++) {
    const audioPath = path.join(root, `audio-${i}.mp3`);
    await writeFile(audioPath, `audio-${i}`);
    repo.addAsset({ bookId: book.id, manifestationId: manifestation.id, sequenceInManifestation: i,
      kind: "single", mime: "audio/mpeg", totalSize: 7, durationMs: 60_000,
      files: [{ path: audioPath, size: 7, start: 0, end: 6, durationMs: 60_000 }] });
  }
  const epubPath = path.join(root, "book.epub");
  await writeFile(epubPath, "test epub");
  const ebookManifestation = repo.addManifestation({ bookId: book.id, kind: "ebook" });
  const ebook = repo.addAsset({ bookId: book.id, manifestationId: ebookManifestation.id,
    kind: "single", mime: "application/epub+zip", totalSize: 9,
    files: [{ path: epubPath, size: 9, start: 0, end: 8, durationMs: 0 }] });
  const job = repo.createJob({ type: "chapter_analysis", bookId: book.id,
    payload: { manifestationId: manifestation.id, ebookAssetId: ebook.id } });
  const ctx = { repo, getSettings: () => defaultSettings({ agents: { apiKey: "fake", timeoutMs: 1000 } }), onLog() {} };
  return { root, db, dbPath, repo, manifestation, job, ctx };
}

describe("durable chapter pipeline", () => {
  test.each(["chunk", "publication"])("resumes after %s checkpoint and reopening without repeating paid chunks", async (interruption) => {
    const f = await fixture();
    let paid = 0;
    let curated = 0;
    let fail = true;
    const deps = {
      loadEpubEntries: async () => [],
      extractChunkClip: async () => path.join(f.root, "clip.mp3"),
      transcribeChunk: async () => { paid++; return { words: [{ raw: "hello", token: "hello", startMs: 1000, endMs: 2000 }], segments: [] }; },
      runAgenticCuration: async () => { curated++; return { result: null, finalOutput: null, newItems: [], rawResponses: [] }; },
    };
    try {
      const first = journal(f.db, (name) => {
        if ((interruption === "chunk" ? name.startsWith("chapter:asset:") : name === "chapter:publish-transcript") && fail) {
          fail = false;
          throw new Error("process interrupted");
        }
      });
      await expect(processChapterAnalysisJob({ ...f.ctx, execution: first.execution }, f.job, deps)).rejects.toThrow("process interrupted");
      expect(paid).toBe(interruption === "chunk" ? 1 : 2);
      f.db.close();
      const reopened = new Database(f.dbPath);
      try {
        const repo = new BooksRepo(reopened);
        await processChapterAnalysisJob({ ...f.ctx, repo, execution: journal(reopened).execution }, f.job, deps);
        expect(paid).toBe(2);
        expect(curated).toBe(1);
        const transcript = await loadStoredManifestationTranscriptPayload(repo, f.manifestation.id);
        expect(transcript?.words.map((w) => w.startMs)).toEqual([1000, 61_000]);
        const steps = reopened.query("SELECT value FROM chapter_test_steps").all() as { value: string }[];
        expect(steps.every((s) => !s.value.includes('"words":'))).toBe(true);
      } finally { reopened.close(); }
    } finally { try { f.db.close(); } catch {} await rm(f.root, { recursive: true, force: true }); }
  });

  test.each(["transcription", "curation"])("stale owner during %s cannot publish or mark failure", async (stage) => {
    const f = await fixture();
    const owner = journal(f.db);
    try {
      await expect(processChapterAnalysisJob({ ...f.ctx, execution: owner.execution }, f.job, {
        loadEpubEntries: async () => [],
        extractChunkClip: async () => path.join(f.root, "clip.mp3"),
        transcribeChunk: async () => {
          if (stage === "transcription") owner.cancel();
          return { words: [{ raw: "hello", token: "hello", startMs: 1000, endMs: 2000 }], segments: [] };
        },
        runAgenticCuration: async () => {
          owner.cancel();
          return { result: null, finalOutput: null, newItems: [], rawResponses: [] };
        },
      })).rejects.toThrow("stale chapter owner");
      expect(f.repo.getChapterAnalysis(f.manifestation.id)?.status).not.toBe("succeeded");
      expect(f.repo.getChapterAnalysis(f.manifestation.id)?.status).not.toBe("failed");
      if (stage === "transcription") expect(f.repo.getManifestationTranscript(f.manifestation.id)?.status).not.toBe("succeeded");
    } finally { f.db.close(); await rm(f.root, { recursive: true, force: true }); }
  });

  test("curation resumes completed node decisions after ownership loss", async () => {
    const f = await fixture();
    const calls: number[] = [];
    const first = journal(f.db, (name) => {
      if (name === "chapter:curation:node:1") first.cancel();
    });
    const deps = {
      loadEpubEntries: async () => [],
      extractChunkClip: async () => path.join(f.root, "clip.mp3"),
      transcribeChunk: async () => ({ words: [{ raw: "hello", token: "hello", startMs: 1000, endMs: 2000 }], segments: [] }),
      runAgenticCuration: async (ctx: ChapterCurationContext) => {
        for (const node of [1, 2]) {
          await chapterArtifactStep(ctx, `chapter:curation:node:${node}`, async () => {
            calls.push(node);
            return { accepted: true, node };
          });
        }
        return { result: null, finalOutput: null, newItems: [], rawResponses: [] };
      },
    };
    try {
      await expect(processChapterAnalysisJob({ ...f.ctx, execution: first.execution }, f.job, deps)).rejects.toThrow("stale chapter owner");
      expect(calls).toEqual([1]);
      await processChapterAnalysisJob({ ...f.ctx, execution: journal(f.db).execution }, f.job, deps);
      expect(calls).toEqual([1, 2]);
      expect(f.repo.getChapterAnalysis(f.manifestation.id)?.status).toBe("succeeded");
    } finally { f.db.close(); await rm(f.root, { recursive: true, force: true }); }
  });

  test.each(["cancelled", "failed"])("%s replacement retains the last successful transcript", async (outcome) => {
    const f = await fixture();
    const owner = journal(f.db);
    try {
      const transcriptPath = path.join(f.root, "successful-transcript.json");
      const content = JSON.stringify({ version: 1, text: "previous success", words: [{ text: "previous", token: "previous", startMs: 100, endMs: 200 }] });
      await writeFile(transcriptPath, content);
      f.repo.upsertManifestationTranscript({ manifestationId: f.manifestation.id, status: "succeeded", source: "whisper_transcript",
        algorithmVersion: "existing", fingerprint: "older-audio-fingerprint", transcriptPath, error: null });
      const before = f.repo.getManifestationTranscript(f.manifestation.id);
      await expect(processChapterAnalysisJob({ ...f.ctx, execution: owner.execution }, f.job, {
        loadEpubEntries: async () => [],
        extractChunkClip: async () => path.join(f.root, "clip.mp3"),
        transcribeChunk: async () => {
          if (outcome === "cancelled") owner.cancel();
          throw new Error("replacement failed");
        },
      })).rejects.toThrow(outcome === "cancelled" ? "stale chapter owner" : "replacement failed");
      expect(f.repo.getManifestationTranscript(f.manifestation.id)).toEqual(before);
      expect(await Bun.file(transcriptPath).text()).toBe(content);
    } finally { f.db.close(); await rm(f.root, { recursive: true, force: true }); }
  });

  test("curation artifacts resume completed decisions and keep large evidence out of the journal", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "podible-curation-durable-"));
    const db = new Database(path.join(root, "steps.sqlite"));
    let calls = 0;
    const ctx = { execution: journal(db).execution };
    try {
      const decide = async () => { calls++; return { accepted: true, evidence: "large transcript ".repeat(1000) }; };
      const first = await chapterArtifactStep(ctx, "curation:node:1", decide);
      const replay = await chapterArtifactStep(ctx, "curation:node:1", decide);
      expect(replay).toEqual(first);
      expect(calls).toBe(1);
      const row = db.query("SELECT value FROM chapter_test_steps").get() as { value: string };
      expect(row.value.length).toBeLessThan(1000);
      expect(row.value).not.toContain("large transcript");
    } finally { db.close(); await rm(root, { recursive: true, force: true }); }
  });
});
