#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import { parseArgs } from "node:util";
import path from "node:path";
import { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { booksDbPath, configDir } from "../src/config";
import { BooksRepo } from "../src/repo";
import { createSessionToken, hashSessionToken } from "../src/auth";
import { managedTvTransmission, readTransmissionIdentity } from "../src/media/managed-transmission";
import { TransmissionRpc } from "../src/media/transmission-client";
import { onePieceResumeScope, repairTorrentResume } from "../src/media/torrent-resume-repair";

export async function repairResumeCli(args: string[]) {
  const { values } = parseArgs({ args, strict: true, allowPositionals: false,
    options: { apply: { type: "boolean" }, "dry-run": { type: "boolean" }, "require-build": { type: "string" }, help: { type: "boolean" } } });
  if (values.help) {
    console.log("Exact One Piece S02 resume repair; dry-run by default. Apply only after normal CD deploys the shutdown fix.\nbun scripts/repair-torrent-resume.ts [--dry-run | --apply --require-build FULL_SHA]\nNo deletion, file moves, new acquisitions, or new searches. Rechecks existing bytes and retries only job 8924.");
    return;
  }
  if (values.apply && values["dry-run"]) throw new Error("Conflicting repair modes");
  if (values.apply && (!values["require-build"] || process.env.GIT_COMMIT_SHA !== values["require-build"]))
    throw new Error("Apply requires the exact running shutdown-fixed build SHA");
  const db = new Database(booksDbPath, { readonly: !values.apply, create: false });
  db.exec("PRAGMA busy_timeout = 2000");
  const repo = new BooksRepo(db), engineDirectory = path.join(configDir, "transmission");
  const journal = new SQLiteStorage(repo.executionDatabasePath(), { readOnly: true });
  try {
    const identity = readTransmissionIdentity(engineDirectory);
    const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc", username: identity.username, password: identity.password });
    const session = await rpc.call("session-get");
    if (session["download-dir"] !== "/home/bun/Downloads" || session["start-added-torrents"] !== false)
      throw new Error("Private engine defaults changed; inspect before repair");
    const result = await repairTorrentResume({
      scope: onePieceResumeScope, repo, journal, rpc,
      client: managedTvTransmission(repo.getSettings().tv.managedRoot, engineDirectory),
      engineDirectory, defaultDirectory: "/home/bun/Downloads",
      reportPath: path.join(configDir, "repairs", "one-piece-s02-resume.json"),
      build: process.env.GIT_COMMIT_SHA ?? "", requiredBuild: values["require-build"],
      async retryOriginalJob() {
        const request = repo.tv.requestById(onePieceResumeScope.requestId)!;
        const token = createSessionToken(), auth = repo.createSession(request.user_id, hashSessionToken(token), null, "browser");
        const origin = `http://127.0.0.1:${process.env.PORT ?? "80"}`;
        try {
          const response = await fetch(origin + "/rpc", { method: "POST", headers: { Origin: origin,
            Cookie: `podible_session=${token}`, "Content-Type": "application/json" }, signal: AbortSignal.timeout(15_000),
            body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "jobs.retry", params: { jobId: onePieceResumeScope.jobId } }) });
          const result = await response.json() as { result?: { job?: { id: number } }; error?: unknown };
          if (!response.ok || result.error || result.result?.job?.id !== onePieceResumeScope.jobId) throw new Error("Original job retry was not acknowledged");
        } finally { repo.deleteSession(auth.id); }
      },
    }, values.apply ?? false);
    console.log(JSON.stringify(result, null, 2));
  } finally { await journal.close(); db.close(); }
}

if (import.meta.main) {
  try { await repairResumeCli(process.argv.slice(2)); }
  catch { console.error("Resume repair stopped; no raw RPC, metainfo, credentials, or provider URLs are logged. Inspect the redacted repair report and trusted evidence."); process.exitCode = 1; }
}
