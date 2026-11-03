import { appendFileSync } from "node:fs";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createFixtureMovieServices } from "./movie-services";
import { createFixtureSeerrSource, seerrRequest } from "./seerr";

const [root, stopAt = ""] = process.argv.slice(2);
const db = new Database(`${root}/app.sqlite`);
runMigrations(db);
db.exec("CREATE TABLE IF NOT EXISTS fixture_seerr_calls (kind TEXT)");
const repo = new BooksRepo(db);
const fixture = createFixtureMovieServices(root);
fixture.addOffer("process-canary", 1920);
const feed = createFixtureSeerrSource([seerrRequest(100), seerrRequest(101)]);
feed.source.latest = async () => {
  db.query("INSERT INTO fixture_seerr_calls VALUES('latest')").run();
  return seerrRequest(100);
};
const movie = feed.source.movie;
feed.source.movie = async (id) => {
  db.query("INSERT INTO fixture_seerr_calls VALUES('metadata')").run();
  return movie(id);
};
let paused = false;
const runtime = await createJobExecutionRuntime(
  {
    repo,
    getSettings: () => repo.getSettings(),
    movieServices: fixture.services,
    seerrSource: feed.source,
    onLog() {},
  },
  `${root}/execution.sqlite`,
  {
    leaseMs: 250,
    onCheckpoint: async (jobId, name) => {
      if (name !== stopAt) return;
      paused = true;
      appendFileSync(`${root}/events`, `${jobId}:${name}\n`);
      await new Promise(() => {});
    },
  },
);
while (repo.listExecutionJobs().some((job) => job.status !== "cancelled")) {
  if (!paused) await runtime.reconcile();
  await Bun.sleep(20);
}
await runtime.stop();
db.close();
