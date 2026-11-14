import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

test("the read-only Plex timing report requires a completed download and its exact publication", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "plex-latency-report-"));
  const filename = path.join(dir, "podible.sqlite");
  const stamp = (seconds: number) => new Date(Date.UTC(2026, 0, 1) + seconds * 1000).toISOString();
  try {
    {
      using db = new Database(filename);
      db.exec(`CREATE TABLE acquisition_latency_receipts(media, acquisition_id);
        CREATE TABLE media_torrents(id, client_id, native_id, observation_json);
        CREATE TABLE app_state(key, value_json);`);
      for (const media of ["movie", "tv"]) db.exec(`
        CREATE TABLE ${media}_acquisitions(id, target_id, reason, created_at);
        CREATE TABLE ${media}_versions(id, acquisition_id, facts_json);
        CREATE TABLE ${media}_events(id, target_id, acquisition_id, event_key, stage, created_at);
        CREATE TABLE ${media}_acquisition_downloads(acquisition_id, torrent_id);`);
      const fixture = (media: "movie" | "tv", id: number, completion: number, publication: number, receipt = true) => {
        const base = id * 10;
        db.run(`INSERT INTO ${media}_acquisitions VALUES (?,1,'initial',?)`, [id, stamp(2)]);
        db.run("INSERT INTO media_torrents VALUES (?,'client',?,?)",
          [id, String(id), JSON.stringify({ complete: true, completedAt: stamp(completion) })]);
        db.run(`INSERT INTO ${media}_versions VALUES (?,?,?)`, [id, id, JSON.stringify({
          attemptKey: "tv:attempt:0", source: { ref: { clientId: "client", nativeId: String(id) } },
        })]);
        db.run("INSERT INTO app_state VALUES (?,?)", ["release-health:movie-version-source:" + id, JSON.stringify({ id })]);
        db.run(`INSERT INTO ${media}_acquisition_downloads VALUES (?,?)`, [id, id]);
        if (receipt) db.run("INSERT INTO acquisition_latency_receipts VALUES (?,?)", [media, id]);
        const event = (order: number, key: string, stage: string, at: number) =>
          db.run(`INSERT INTO ${media}_events VALUES (?,1,?,?,?,?)`, [base + order, id, key, stage, stamp(at)]);
        event(1, `tv-acquisition:${id}:0:publishing`, "publishing", 8);
        if (media === "tv" || id === 4) event(2, `tv-acquisition:${id}:1:publishing`, "publishing", 11);
        event(3, `${media === "tv" ? "tv-" : ""}published:1:${id}`, "available", publication);
      };
      fixture("movie", 1, 4, 18);
      fixture("tv", 2, 4, 34);
      fixture("movie", 3, 1, 19); // Reused bytes.
      fixture("movie", 4, 4, 20); // Publishing attempt is ambiguous.
      fixture("tv", 5, 4, 21, false); // No recorded acquisition sample.
    }
    const result = Bun.spawnSync([process.execPath, "run", path.resolve(import.meta.dir, "../../scripts/report-plex-latency.ts"),
      "--db", filename, "--since", stamp(18), "--limit", "1"]);
    expect(result.stderr.toString()).toBe("");
    expect(result.exitCode).toBe(0);
    const { report } = JSON.parse(result.stdout.toString());
    expect(report).toMatchObject([
      { media: "movie", candidatesExamined: 3, samples: [{ acquisitionId: 1, preparationMs: 4000, publicationMs: 10000, totalMs: 14000, underTarget: true }] },
      { media: "tv", candidatesExamined: 1, samples: [{ acquisitionId: 2, preparationMs: 4000, publicationMs: 26000, totalMs: 30000, underTarget: false }] },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
