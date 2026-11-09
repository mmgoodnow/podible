import { Database } from "bun:sqlite";
import { realpathSync } from "node:fs";
import path from "node:path";
import { parseArgs } from "node:util";
import { booksDbPath } from "../src/config";
import { parseSettings } from "../src/settings";
import { readTransmissionIdentity } from "../src/media/managed-transmission";
import { TransmissionRpc } from "../src/media/transmission-client";
import { rotateTrackerAnnounces, TrackerRotationError } from "../src/media/tracker-rotation";

const usage = "bun run rotate:tracker-announces --provider NAME [--apply]";
const { values } = parseArgs({ options: { provider: { type: "string" }, apply: { type: "boolean" }, help: { type: "boolean" } } });
if (values.help || !values.provider?.trim()) {
  console.log(usage);
  process.exit(values.help ? 0 : 1);
}

try {
  const db = new Database(booksDbPath, { readonly: !values.apply, create: false });
  try {
    db.exec("PRAGMA busy_timeout=5000");
    const settings = db.query<{ value_json: string }, []>("SELECT value_json FROM settings WHERE id=1").get();
    if (!settings) throw new Error("No saved settings");
    const { libraryRoot, movies, tv } = parseSettings(settings.value_json);
    const booksRoot = (() => { try { return realpathSync(libraryRoot); } catch { return undefined; } })();
    const identity = readTransmissionIdentity();
    const rpc = new TransmissionRpc({ url: "http://127.0.0.1:9091/transmission/rpc",
      username: identity.username, password: identity.password });
    const result = await rotateTrackerAnnounces({ db, rpc, clientId: `transmission:${identity.id}`,
      provider: values.provider, apply: values.apply ?? false,
      roots: {
        books: booksRoot && path.join(booksRoot, ".podible", "downloads"),
        movies: path.isAbsolute(movies.managedRoot) ? path.join(movies.managedRoot, "downloads") : undefined,
        tv: path.isAbsolute(tv.managedRoot) ? path.join(tv.managedRoot, "downloads") : undefined,
      },
    });
    console.log(JSON.stringify({ mode: values.apply ? "applied" : "preview", ...result }, null, 2));
  } finally { db.close(); }
} catch (error) {
  console.error(error instanceof TrackerRotationError ? error.message :
    "Announce rotation stopped. Verify the newest provider download, database, and private Transmission are available, then preview again. URLs are never printed.");
  process.exitCode = 1;
}
