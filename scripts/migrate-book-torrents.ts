#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import path from "node:path";
import { parseArgs } from "node:util";
import { z } from "zod";
import { booksDbPath, configDir } from "../src/config";
import { parseSettings } from "../src/settings";
import { RtorrentClient } from "../src/rtorrent";
import { readTransmissionIdentity } from "../src/media/managed-transmission";
import { TransmissionAdapter, TransmissionRpc } from "../src/media/transmission-client";
import {
  migrateBookTorrents,
  migrationRepository,
  openMigrationJournal,
  prepareMigrationTarget,
} from "../src/media/book-torrent-migration";

const usage = `One-off rTorrent -> private Transmission book migration (dry-run by default).

bun scripts/migrate-book-torrents.ts [--db FILE] [--config-dir DIR]
  [--report FILE] [--book-id ID ...] [--hash SHA1 ...]
  [--legacy-tag Podible|LazyLibrarian ...] [--timeout-seconds N]
  [--apply --workers-stopped]

Apply requires an explicit report path and confirmation that Podible's workers
are stopped for the entire run (keep the private Transmission daemon running).
The same report resumes uncertain/partial runs; it never substitutes for a new
recheck. A stale FILE.lock requires operator review after process termination.
Only DB releases with allowed ownership tags and all original raw bytes qualify.
Defaults include both Podible and LazyLibrarian, including completed failed-status
releases. No tracker URL/metainfo fetches, copying, data deletion, or record erasure.
Both clients must see the same absolute local paths on the same filesystem.
`;

export function parseMigrationArgs(args: string[]) {
  const { values } = parseArgs({
    args, strict: true, allowPositionals: false,
    options: {
      apply: { type: "boolean", default: false },
      "dry-run": { type: "boolean", default: false },
      "workers-stopped": { type: "boolean", default: false },
      help: { type: "boolean", short: "h", default: false },
      db: { type: "string" },
      "config-dir": { type: "string" },
      report: { type: "string" },
      "book-id": { type: "string", multiple: true },
      hash: { type: "string", multiple: true },
      "legacy-tag": { type: "string", multiple: true },
      "timeout-seconds": { type: "string", default: "1800" },
    },
  });
  if (values.apply && values["dry-run"]) throw new Error("Conflicting run modes");
  if (values.apply && (!values.report || !values["workers-stopped"]))
    throw new Error("Apply requires --report and --workers-stopped");
  const bookIds = (values["book-id"] ?? []).map((id) => {
    if (!/^[1-9][0-9]*$/.test(id) || !Number.isSafeInteger(Number(id))) throw new Error("Invalid book ID");
    return Number(id);
  });
  const hashes = (values.hash ?? []).map((hash) => z.string().regex(/^[a-f0-9]{40}$/).parse(hash.toLowerCase()));
  const allowedLegacyTags = (values["legacy-tag"] ?? ["Podible", "LazyLibrarian"])
    .map((tag) => z.enum(["Podible", "LazyLibrarian"]).parse(tag));
  const seconds = Number(values["timeout-seconds"]);
  if (!Number.isFinite(seconds) || seconds <= 0 || seconds > 86400) throw new Error("Invalid timeout");
  return {
    help: values.help,
    apply: values.apply,
    db: path.resolve(values.db ?? booksDbPath),
    configDir: path.resolve(values["config-dir"] ?? (values.db ? path.dirname(values.db) : configDir)),
    report: values.report ? path.resolve(values.report) : undefined,
    bookIds, hashes, allowedLegacyTags, timeoutMs: seconds * 1000,
  };
}

export async function runMigrationCli(args: string[]) {
  const options = parseMigrationArgs(args);
  if (options.help) { console.log(usage); return 0; }
  const db = new Database(options.db, { readonly: !options.apply, create: false, strict: true });
  let journalHandle: Awaited<ReturnType<typeof openMigrationJournal>> | undefined;
  try {
    db.exec("PRAGMA foreign_keys = ON");
    db.exec("PRAGMA busy_timeout = 5000");
    // Do not call getSettings/ensureSettings/runMigrations: dry-run is read-only,
    // and even apply must never implicitly upgrade a live database.
    const row = db.query<{ value_json: string }, []>("SELECT value_json FROM settings WHERE id = 1").get();
    if (!row) throw new Error("Settings must already exist");
    const settings = parseSettings(row.value_json);
    const identity = readTransmissionIdentity(path.join(options.configDir, "transmission"));
    const clientId = `transmission:${identity.id}`;
    const rootPath = path.join(settings.libraryRoot, ".podible", "downloads");
    const rpc = new TransmissionRpc({
      url: "http://127.0.0.1:9091/transmission/rpc",
      username: identity.username, password: identity.password,
    });
    const client = new TransmissionAdapter({
      clientId, roots: { books: rootPath }, ownershipTag: "Podible Books", rpc,
    });
    if (options.apply) {
      const session = await rpc.call("session-get");
      // No automatic start/hook may turn a paused import into a download.
      for (const key of ["start-added-torrents", "script-torrent-added-enabled",
        "script-torrent-done-enabled", "script-torrent-done-seeding-enabled",
        "incomplete-dir-enabled", "rename-partial-files"])
        if (session[key] !== false) throw new Error("Unsafe private engine session configuration");
    }
    const legacy = new RtorrentClient(settings.rtorrent);
    journalHandle = options.report ? await openMigrationJournal(options.report, options.apply) : undefined;
    const report = journalHandle?.journal ?? {
      report: { version: 1 as const, runId: crypto.randomUUID(), entries: {} },
      async save() { throw new Error("Apply requires a durable report"); },
    };
    const results = await migrateBookTorrents({
      repository: migrationRepository(db), client, journal: report,
      prepareTarget: () => prepareMigrationTarget(settings.libraryRoot, clientId),
      legacy: {
        async inspect(hash) {
          const [tag, hashing, state, source] = await Promise.all([
            legacy.getOwnershipTag(hash), legacy.getHashingState(hash),
            legacy.getDownloadState(hash), legacy.getImportSource(hash),
          ]);
          return { tag, hashing, state, source };
        },
        async stop(hash) {
          await legacy.control(hash, "stop");
          await legacy.control(hash, "close");
          const deadline = Date.now() + 5000;
          while ((await legacy.getDownloadState(hash)).isActive) {
            if (Date.now() >= deadline) throw new Error("Legacy torrent did not stop");
            await Bun.sleep(100);
          }
        },
      },
      async assertVerified(ref, metadata) {
        const response = await rpc.call("torrent-get", {
          ids: [ref.nativeId],
          fields: ["hashString", "downloadDir", "labels", "status", "error", "totalSize",
            "haveValid", "haveUnchecked", "leftUntilDone", "percentDone", "fileStats", "files"],
        });
        const rows = z.array(z.object({
          hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()),
          status: z.literal(0), error: z.literal(0), totalSize: z.number(), haveValid: z.number(),
          haveUnchecked: z.literal(0), leftUntilDone: z.literal(0), percentDone: z.literal(1),
          fileStats: z.array(z.object({ wanted: z.literal(true), bytesCompleted: z.number() })),
          files: z.array(z.object({ name: z.string(), length: z.number() })),
        })).parse(response.torrents);
        const torrent = rows.find((t) => t.hashString.toLowerCase() === ref.nativeId);
        if (!torrent || !torrent.labels.includes("Podible Books") ||
          torrent.downloadDir !== path.join(rootPath, ref.nativeId) ||
          torrent.totalSize !== metadata.size || torrent.haveValid !== metadata.size ||
          torrent.files.length !== metadata.files.length || torrent.fileStats.length !== metadata.files.length ||
          torrent.files.some((file, i) => file.name !== metadata.files[i]!.path || file.length !== metadata.files[i]!.size ||
            torrent.fileStats[i]!.bytesCompleted !== file.length)) throw new Error("Full verification not confirmed");
      },
    }, options);
    console.log(JSON.stringify({ mode: options.apply ? "apply" : "dry-run", results: results.map((entry) => ({
      bookId: entry.bookId, releaseId: entry.releaseId, infoHash: entry.infoHash,
      phase: entry.phase, reason: entry.reason, failedAt: entry.failedAt,
      oldRecord: entry.oldRecord, pauseConfirmed: entry.pauseConfirmed,
    })) }, null, 2));
    return results.some((entry) => entry.phase === "failed") ? 1 : 0;
  } finally {
    await journalHandle?.close();
    db.close();
  }
}

if (import.meta.main) {
  try { process.exitCode = await runMigrationCli(process.argv.slice(2)); }
  catch {
    // XML-RPC faults, parse errors, URLs and torrent metadata can contain secrets.
    console.error("Migration stopped. Check arguments/configuration and the redacted run report; raw errors are not logged.");
    process.exitCode = 1;
  }
}
