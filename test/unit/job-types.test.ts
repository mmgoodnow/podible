import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { JOB_TYPES } from "../../src/job-types";
import { jobTypeSchema } from "../../src/rpc/schemas";
import { jobsRouter } from "../../src/rpc/jobs-router";
import { executeMethod } from "../../src/rpc/framework";

test("every execution job type is accepted and visible through the admin jobs API", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const admin = repo.upsertUser({ provider: "local", providerUserId: "job-types", username: "admin", isAdmin: true });
  const session = repo.createSession(admin.id, "job-types-session");
  const ctx = { repo, session, startTime: Date.now(), request: new Request("http://localhost/rpc") };
  for (const type of JOB_TYPES) {
    expect(jobTypeSchema.parse(type)).toBe(type);
    repo.createJob({ type });
  }
  const all = await executeMethod(jobsRouter.routes.list, ctx, { limit: 100 });
  expect(all.jobs.map((job) => job.type).sort()).toEqual([...JOB_TYPES].sort());
  for (const type of JOB_TYPES) {
    const result = await executeMethod(jobsRouter.routes.list, ctx, { type });
    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]!.type).toBe(type);
  }
});
