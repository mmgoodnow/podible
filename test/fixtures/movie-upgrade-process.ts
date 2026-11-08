import { Database } from "bun:sqlite";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { BooksRepo } from "../../src/repo";
import { createJobExecutionRuntime } from "../../src/worker/execution";
import { createFixtureMovieServices, fixtureFacts } from "./movie-services";

export const upgradeServerViewRoot = "/fixture-plex/movies";
export type UpgradeControl = {
  offers: string[];
  plexVerified: boolean;
  pauseAt: string[];
};
type Fixture = ReturnType<typeof createFixtureMovieServices>;
type FixtureDownload = Fixture["downloads"] extends Map<string, infer T> ? T : never;
export type UpgradeFixtureState = {
  stats: Fixture["stats"];
  downloads: [string, FixtureDownload][];
  addCalls: string[];
  quiesced: string[];
  equalProposals: number;
  verifications: { accepted: boolean; paths: string[] }[];
};

export function checkpointFile(root: string, name: string) {
  return path.join(root, `checkpoint-${name.replaceAll(":", "-")}`);
}

async function main(root: string) {
  const db = new Database(path.join(root, "app.sqlite"));
  db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const fixture = createFixtureMovieServices(root);
  const offers = [
    fixture.addOffer("original", 1280),
    fixture.addOffer("stalled", 1920, true),
    fixture.addOffer("better", 1920),
    fixture.addOffer("equal", 1920),
  ];
  const stateFile = path.join(root, "torrent-state.json");
  const prior: UpgradeFixtureState | null = existsSync(stateFile)
    ? JSON.parse(readFileSync(stateFile, "utf8"))
    : null;
  if (prior) {
    Object.assign(fixture.stats, prior.stats);
    for (const [hash, download] of prior.downloads)
      fixture.downloads.set(hash, download);
  }
  const addCalls = prior?.addCalls ?? [];
  const quiesced = prior?.quiesced ?? [];
  let equalProposals = prior?.equalProposals ?? 0;
  const verifications = prior?.verifications ?? [];
  const control = (): UpgradeControl =>
    JSON.parse(readFileSync(path.join(root, "control.json"), "utf8"));
  // This file models a torrent daemon that survives the application process.
  const save = () => {
    const state: UpgradeFixtureState = {
      stats: fixture.stats,
      downloads: [...fixture.downloads],
      addCalls,
      quiesced,
      equalProposals,
      verifications,
    };
    writeFileSync(`${stateFile}.tmp`, JSON.stringify(state));
    renameSync(`${stateFile}.tmp`, stateFile);
  };
  fixture.services.search = async () => {
    fixture.stats.searches++;
    return {
      offers: offers.filter((offer) => control().offers.includes(offer.key)),
      warnings: [],
    };
  };
  const select = fixture.services.decider.select;
  fixture.services.decider.select = async (input) => {
    // Challenge the real policy guard instead of trusting the agent's filtering.
    if (input.current && input.offers.some((offer) => offer.key === "equal")) {
      equalProposals++;
      return {
        offerKey: "equal",
        facts: fixtureFacts(1920),
        reason: "Fixture agent proposes a distinct equal-quality release",
      };
    }
    return select(input);
  };
  const client = fixture.services.client;
  const ensureAdded = client.ensureAdded.bind(client);
  client.ensureAdded = async (input) => {
    const ref = await ensureAdded(input);
    addCalls.push(ref.nativeId);
    save();
    return ref;
  };
  const inspect = client.inspect.bind(client);
  client.inspect = async (ref) => {
    const result = await inspect(ref);
    save();
    return result;
  };
  const resume = client.resume.bind(client);
  client.resume = async (ref) => {
    await resume(ref);
    save();
  };
  const quiesce = client.quiesce.bind(client);
  client.quiesce = async (ref) => {
    await quiesce(ref);
    quiesced.push(ref.nativeId);
    save();
  };
  const remove = client.removeRecord.bind(client);
  client.removeRecord = async (...args) => {
    try {
      return await remove(...args);
    } finally {
      save();
    }
  };
  fixture.services.library.verify = async (_movie, paths) => {
    if (!paths.length) throw new Error("Expected a movie publication path");
    for (const remote of paths) {
      const relative = path.posix.relative(upgradeServerViewRoot, remote);
      if (relative.startsWith("..") || path.isAbsolute(relative))
        throw new Error("Plex verification path escaped the fixture view");
      // A mock verdict still requires the real publication symlink and bytes.
      await fs.readFile(path.join(root, "views", "plex", "movies", relative));
    }
    const accepted = control().plexVerified;
    verifications.push({ accepted, paths });
    save();
    return accepted;
  };
  fixture.services.timing = {
    pollMs: 1,
    stallMs: 1500,
    plexAttempts: 3,
    plexRetryMs: 1,
  };
  const runtime = await createJobExecutionRuntime(
    {
      repo,
      getSettings: () => repo.getSettings(),
      movieServices: fixture.services,
      onLog() {},
    },
    path.join(root, "execution.sqlite"),
    {
      leaseMs: 300,
      retryDelayMs: 1,
      onCheckpoint: async (jobId, name) => {
        save();
        const job = repo.getJob(jobId);
        if (job?.type !== "movie_acquire" || !control().pauseAt.includes(name))
          return;
        const { acquisitionId } = JSON.parse(job.payload_json!);
        if (repo.movies.acquisition(acquisitionId)?.reason !== "upgrade") return;
        const marker = checkpointFile(root, name);
        writeFileSync(`${marker}.tmp`, JSON.stringify({ jobId, name, pid: process.pid }));
        renameSync(`${marker}.tmp`, marker);
        while (!existsSync(`${marker}.continue`)) await Bun.sleep(10);
      },
    },
  );
  const handler = createPodibleFetchHandler(repo, Date.now(), null, {
    movieServices: fixture.services,
  });
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
  writeFileSync(
    path.join(root, "ready.json.tmp"),
    JSON.stringify({ port: server.port, pid: process.pid }),
  );
  renameSync(path.join(root, "ready.json.tmp"), path.join(root, "ready.json"));
  try {
    while (true) {
      await runtime.reconcile();
      await Bun.sleep(10);
    }
  } finally {
    server.stop(true);
    await runtime.stop();
    db.close();
  }
}

if (import.meta.main) await main(process.argv[2]!);
