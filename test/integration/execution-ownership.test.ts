import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";

test("job handoffs and manifestations have stable retry identities", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Acceptance", author: "Test" });
  const makeJob = () => repo.createJob({ type: "import", bookId: book.id, idempotencyKey: "download:1:import" });
  expect(makeJob().id).toBe(makeJob().id);
  const makeManifestation = () => repo.ensureManifestation("acquire:1:audio:0", { bookId: book.id, kind: "audio" });
  expect(makeManifestation().id).toBe(makeManifestation().id);
  expect(repo.listManifestationsByBook(book.id)).toHaveLength(1);
  db.close();
});

test("superseded execution cannot publish or complete after cancellation", () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Acceptance", author: "Test" });
  const job = repo.createJob({ type: "acquire", bookId: book.id });
  repo.attachJobExecution(job.id, "run");
  expect(repo.ownJobExecution(job.id, "run", "old")).toBe(true);
  const fenced = repo.forExecution(() => {
    if (repo.getJobExecution(job.id)?.owner_token !== "old" || repo.getJob(job.id)?.status === "cancelled") {
      throw new Error("Execution lost ownership");
    }
  });
  expect(repo.ownJobExecution(job.id, "run", "new")).toBe(true);
  expect(() => fenced.addManifestation({ bookId: book.id, kind: "audio" })).toThrow("lost ownership");
  expect(() => fenced.markJobSucceeded(job.id)).toThrow("lost ownership");
  expect(repo.listManifestationsByBook(book.id)).toHaveLength(0);
  repo.markJobCancelled(job.id);
  repo.markJobSucceeded(job.id);
  expect(repo.getJob(job.id)?.status).toBe("cancelled");
  expect(repo.ownJobExecution(job.id, "run", "third")).toBe(false);
  db.close();
});
