import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

test("the read-only Plex timing report requires a completed download and its exact publication", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "plex-latency-report-"));
  const filename = path.join(dir, "podible.sqlite");
  const execution = path.join(dir, "execution.sqlite");
  const stamp = (seconds: number) => new Date(Date.UTC(2026, 0, 1) + seconds * 1000).toISOString();
  try {
    {
      using db = new Database(filename);
      db.exec(`CREATE TABLE acquisition_latency_receipts(media, acquisition_id);
        CREATE TABLE media_torrents(id, client_id, native_id, observation_json);
        CREATE TABLE job_executions(job_id, run_id);
        CREATE TABLE app_state(key, value_json);`);
      db.run("INSERT INTO job_executions VALUES (2, 'tv-run')");
      for (const media of ["movie", "tv"]) db.exec(`
        CREATE TABLE ${media}_acquisitions(id, target_id, job_id, reason, created_at);
        CREATE TABLE ${media}_versions(id, acquisition_id, facts_json);
        CREATE TABLE ${media}_events(id, target_id, acquisition_id, event_key, stage, created_at);
        CREATE TABLE ${media}_acquisition_downloads(acquisition_id, torrent_id);`);
      const fixture = (media: "movie" | "tv", id: number, completion: number, publication: number, receipt = true) => {
        const base = id * 10;
        db.run(`INSERT INTO ${media}_acquisitions VALUES (?,1,?,'initial',?)`, [id, media === "tv" ? id : null, stamp(2)]);
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
    {
      using db = new Database(execution);
      db.exec("CREATE TABLE workflow_steps(run_id, name, kind, status, started_at, updated_at, created_at, output)");
      const phase = (name: string, from: number, to: number) => ({ name, startedAt: Date.parse(stamp(from)), finishedAt: Date.parse(stamp(to)) });
      for (const [step, kind, start, end] of [
        ["completion:0:inspect", "step", 3, 5],
        ["seal-download", "step", 4, 5],
        ["inspect", "step", 5, 8],
        ["plex-publication:0", "step", 8, 14],
        ["plex-publication:0:event:0", "wait", 14, 24],
        ["plex-publication:1", "step", 24, 35],
        ["sensitive-step-name", "step", 34, 36],
      ] as const) {
        const output = step === "plex-publication:1" ? JSON.stringify({ phases: [
          phase("publication:scan", 23, 25), phase("publication:verify", 25, 35),
          phase("sensitive-phase-name", 25, 29), phase("publication:promote", 29, 28),
        ] }) : step === "seal-download" ? JSON.stringify({ phases: [phase("canonical-seal:daemon", 4, 5)] }) : null;
        db.run("INSERT INTO workflow_steps VALUES ('tv-run',?,?,'completed',?,?,?,?)",
          ["tv:attempt:0:" + step, kind, Date.parse(stamp(start)), Date.parse(stamp(end)), Date.parse(stamp(start)), output]);
      }
    }
    const result = Bun.spawnSync([process.execPath, "run", path.resolve(import.meta.dir, "../../scripts/report-plex-latency.ts"),
      "--db", filename, "--execution-db", execution, "--since", stamp(18), "--limit", "1"]);
    expect(result.stderr.toString()).toBe("");
    expect(result.exitCode).toBe(0);
    const { report } = JSON.parse(result.stdout.toString());
    expect(report).toMatchObject([
      { media: "movie", candidatesExamined: 3, samples: [{ acquisitionId: 1, preparationMs: 4000, publicationMs: 10000, totalMs: 14000, underTarget: true }] },
      { media: "tv", candidatesExamined: 1, samples: [{ acquisitionId: 2, preparationMs: 4000, publicationMs: 26000, totalMs: 30000,
        underTarget: false, timeline: { truncated: false, segments: [
          { phase: "read_torrent_completion", fromMs: 0, toMs: 1000 },
          { phase: "seal_files", fromMs: 0, toMs: 1000, subphases: [
            { phase: "canonical-seal:daemon", fromMs: 0, toMs: 1000 },
          ] },
          { phase: "review_import", fromMs: 1000, toMs: 4000 },
          { phase: "publication_attempt", fromMs: 4000, toMs: 10000 },
          { phase: "wait_for_plex_event", fromMs: 10000, toMs: 20000 },
          { phase: "publication_attempt", fromMs: 20000, toMs: 30000, subphases: [
            { phase: "publication:scan", fromMs: 20000, toMs: 21000 },
            { phase: "publication:verify", fromMs: 21000, toMs: 30000 },
          ] },
        ] } }] },
    ]);
    expect(report[1].samples[0].timeline.segments[3].subphases).toBeUndefined();
    expect(result.stdout.toString()).not.toContain("sensitive-");
    expect(report[1].samples[0].timeline.segments.at(-1).subphases).toHaveLength(2);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
