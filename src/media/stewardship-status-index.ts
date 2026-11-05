import type { Database } from "bun:sqlite";
import { parseSettings } from "../settings";
import type { AppSettings } from "../app-types";
import { stewardshipObservationSchema, type StewardshipSource } from "./torrent-stewardship-types";
import { stewardshipBindingFromSettings, stewardshipIndexContext, stewardshipIndexKey, type Binding } from "./stewardship-binding";

export function stewardshipStatusIndex(settings: AppSettings, source: StewardshipSource, row: Record<string, unknown>, binding: Binding, latest: unknown) {
  const parsed = stewardshipObservationSchema.safeParse(latest);
  if (!parsed.success || parsed.data.bindingDigest !== binding.bindingDigest) return null;
  return { key: stewardshipIndexKey(source, binding.clientId, binding.hash), value: {
    version: 1, key: binding.key, bindingDigest: binding.bindingDigest,
    contextJson: stewardshipIndexContext(settings, source, row), latestJson: JSON.stringify(latest),
  } };
}

/** One-time migration only. Batches 100 rows; never called by status/render, never samples a daemon or touches files. */
export function backfillStewardshipStatusIndex(db: Database) {
  const settingsRow = db.query("SELECT value_json FROM settings WHERE id=1").get() as { value_json: string } | null;
  if (!settingsRow) return { scannedCount: 0, indexedCount: 0 };
  let settings: AppSettings;
  try { settings = parseSettings(settingsRow.value_json); }
  catch { return { scannedCount: 0, indexedCount: 0 }; }
  const read = db.query(`SELECT t.*,b.torrent_id,b.book_id,b.release_id,
    CASE WHEN json_valid(t.location_json) THEN json_extract(t.location_json,'$.rootId') END AS root_id,
    CASE WHEN json_valid(t.location_json) THEN json_extract(t.location_json,'$.relativePath') END AS relative_path
    FROM media_torrents t LEFT JOIN book_torrent_bindings b ON b.torrent_id=t.id WHERE t.id>? ORDER BY t.id LIMIT 100`);
  const observation = db.query("SELECT value_json FROM app_state WHERE key=?");
  const write = db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`);
  const bind = db.query("UPDATE media_torrents SET stewardship_binding_json=? WHERE id=?");
  let cursor = 0, scannedCount = 0, indexedCount = 0;
  for (;;) {
    const rows = read.all(cursor) as (Record<string, unknown> & { id: number; native_id: string; client_id: string; torrent_id: number | null })[];
    if (!rows.length) break;
    for (const raw of rows) {
      scannedCount++; cursor = raw.id;
      const source = raw.torrent_id === null ? "screen" : "books", row = { ...raw, info_hash: raw.native_id };
      const binding = stewardshipBindingFromSettings(settings, source, row, raw.client_id);
      if (!binding) continue;
      const saved = observation.get("torrent-stewardship:v1:observation:" + binding.key) as { value_json: string } | null;
      if (!saved) continue;
      let latest: unknown; try { latest = JSON.parse(saved.value_json).latest; } catch { continue; }
      const entry = stewardshipStatusIndex(settings, source, row, binding, latest);
      if (!entry) continue;
      write.run(entry.key, JSON.stringify(entry.value), new Date().toISOString());
      bind.run(JSON.stringify({ key: binding.key, bindingDigest: binding.bindingDigest, contextJson: entry.value.contextJson }), raw.id);
      indexedCount++;
    }
  }
  return { scannedCount, indexedCount };
}
