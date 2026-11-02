import { Database } from "bun:sqlite";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { createPodibleFetchHandler } from "../../src/http";

const db = new Database(":memory:");
runMigrations(db);
const repo = new BooksRepo(db);
repo.ensureSettings();
const user = repo.upsertUser({
  provider: "local",
  providerUserId: "spa-fixture",
  username: "reader",
  isAdmin: false,
});
repo.createSession(
  user.id,
  hashSessionToken("spa-browser-fixture"),
  null,
  "browser",
);
for (const [title, author] of [
  ["The Left Hand of Darkness", "Ursula K. Le Guin"],
  ["Piranesi", "Susanna Clarke"],
  ["The Odyssey", "Homer"],
])
  repo.createBook({ title: title!, author: author! });
const server = Bun.serve({
  hostname: "127.0.0.1",
  port: Number(process.env.PORT ?? 0),
  fetch: createPodibleFetchHandler(repo, Date.now()),
});
console.log(`SPA_FIXTURE_URL=${server.url}`);
process.on("SIGTERM", () => {
  server.stop(true);
  db.close();
  process.exit(0);
});
