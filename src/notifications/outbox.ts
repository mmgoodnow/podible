import type { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import type { AppSettings } from "../app-types";
import { immediateTransaction } from "../db-transaction";
import { parseNotificationSettings, recipientAddresses } from "./settings";

const prefix = "notifications:delivery:v1:";
const pending = prefix + "pending:";
const done = prefix + "done:";
const leaseMs = 30_000;
const lifetimeMs = 12 * 60 * 60_000; // Stay well inside Resend's 24-hour key retention.
const maxDeliveryAttempts = 6;
type Settings = AppSettings["notifications"];
type Message = { title: string; message: string };
type EmailNotice = { id: string; userId: number; title: string; episode: string | null };
type PendingDelivery = {
  message: Message;
  binding: string;
  attempts: number;
  expiresAt: number;
  inFlight: boolean;
  possiblyAccepted: boolean;
} & ({ channel: "email"; userId: number; idempotencyKey: string } | { channel: "pushover" });
type Row = { key: string; value_json: string; updated_at: string };
export type DeliveryClaim = { id: string; delivery: PendingDelivery };
type TerminalOutcome = {
  state: "accepted" | "skipped" | "failed" | "unknown";
  reason?: string;
};
export type DeliveryOutcome = TerminalOutcome | { state: "retry"; reason: string; at: number };

const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const iso = (time: number) => new Date(time).toISOString();
const clean = (value: string) => value.replace(/\s+/g, " ").trim();

export function emailTarget(settings: Settings, to: string | undefined) {
  const from = settings.email.from.trim(), apiKey = settings.email.apiKey.trim();
  return to && from && apiKey ? { from, to, apiKey, binding: hash([from, to, apiKey]) } : null;
}

export function pushoverTarget(settings: Settings) {
  const { enabled, lifecycleEnabled, apiToken, userKey } = settings.pushover;
  return enabled && lifecycleEnabled && apiToken.trim() && userKey.trim()
    ? { binding: hash([apiToken.trim(), userKey.trim()]) } : null;
}

export class NotificationOutbox {
  constructor(private db: Database, private beforeWrite: () => void) {}

  settings(): Settings {
    const row = this.db.query<{ value_json: string }, []>("SELECT value_json FROM settings WHERE id=1").get();
    return parseNotificationSettings(row ? JSON.parse(row.value_json)?.notifications : undefined);
  }

  hasUser(id: number): boolean {
    return !!this.db.query("SELECT 1 FROM users WHERE id=?").get(id);
  }

  /** Called inside promotion, alongside inbox inserts and watch consumption. Never performs I/O. */
  enqueue(identity: string, ownerMessage: Message, notices: EmailNotice[], now: string) {
    const eventId = hash(identity), eventKey = prefix + "event:" + eventId;
    const settings = this.settings();
    const mode = settings.delivery === "live" ? "live" : "dry-run";
    const inserted = this.db.query("INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO NOTHING")
      .run(eventKey, JSON.stringify({ mode }), now);
    if (!inserted.changes || mode !== "live") return;

    const addresses = recipientAddresses(settings.email.recipients);
    const initial = { attempts: 0, expiresAt: Date.parse(now) + lifetimeMs, inFlight: false, possiblyAccepted: false };
    const jobs: { id: string; job: PendingDelivery }[] = [];
    for (const notice of notices) {
      const target = emailTarget(settings, addresses?.get(notice.userId));
      if (!target) continue;
      const title = clean(notice.title), episode = notice.episode && clean(notice.episode);
      jobs.push({ id: hash(["email", notice.id]), job: {
        ...initial, channel: "email", userId: notice.userId, binding: target.binding, idempotencyKey: `podible/${randomUUID()}`,
        message: { title: `Available in Plex: ${title}`, message: `${episode ? `${title} - ${episode}` : title} is now available in Plex.\n\nOpen Podible to view your notification.` },
      } });
    }
    const owner = pushoverTarget(settings);
    if (owner) jobs.push({ id: hash(["pushover", eventId]), job: {
      ...initial, channel: "pushover", binding: owner.binding, message: ownerMessage,
    } });
    if (jobs.length) this.db.query(`INSERT INTO app_state(key,value_json,updated_at)
      SELECT ? || json_extract(value,'$.id'),json_extract(value,'$.job'),? FROM json_each(?)`)
      .run(pending, now, JSON.stringify(jobs));
  }

  claim(now: number): DeliveryClaim | null {
    const at = iso(now);
    const next = () => this.db.query<Row, [string, string, string]>(`SELECT key,value_json,updated_at FROM app_state
      WHERE key>=? AND key<? AND updated_at<=? ORDER BY updated_at,key LIMIT 1`).get(pending, pending + "~", at);
    if (!next()) return null; // Avoid taking the SQLite write lock on idle ticks.
    return immediateTransaction(this.db, this.beforeWrite, () => {
      const row = next();
      if (!row) return null;
      const id = row.key.slice(pending.length), delivery = JSON.parse(row.value_json) as PendingDelivery;
      if ((delivery.channel === "pushover" && delivery.inFlight) || delivery.attempts >= maxDeliveryAttempts || now >= delivery.expiresAt) {
        this.finish(id, delivery, { state: delivery.inFlight || delivery.possiblyAccepted ? "unknown" : "failed", reason: "retry_window_closed" }, now);
        return null;
      }
      delivery.attempts++;
      delivery.possiblyAccepted ||= delivery.inFlight;
      delivery.inFlight = true;
      this.db.query("UPDATE app_state SET value_json=?,updated_at=? WHERE key=?")
        .run(JSON.stringify(delivery), iso(now + leaseMs), row.key);
      return { id, delivery };
    });
  }

  settle({ id, delivery }: DeliveryClaim, outcome: DeliveryOutcome, now: number): DeliveryOutcome | null {
    return immediateTransaction(this.db, this.beforeWrite, () => {
      const row = this.db.query<{ value_json: string }, [string]>("SELECT value_json FROM app_state WHERE key=?").get(pending + id);
      const current = row && JSON.parse(row.value_json) as PendingDelivery;
      if (!current || !current.inFlight || current.attempts !== delivery.attempts) return null;
      const possiblyAccepted = current.possiblyAccepted || (outcome.state === "retry" && outcome.reason !== "http_429");
      if (outcome.state === "retry" && delivery.attempts < maxDeliveryAttempts && outcome.at < delivery.expiresAt) {
        this.db.query("UPDATE app_state SET value_json=?,updated_at=? WHERE key=?")
          .run(JSON.stringify({ ...current, inFlight: false, possiblyAccepted }), iso(outcome.at), pending + id);
        return outcome;
      }
      const state = outcome.state === "accepted" ? "accepted" : possiblyAccepted ? "unknown" : outcome.state === "retry" ? "failed" : outcome.state;
      const terminal: TerminalOutcome = outcome.reason ? { state, reason: outcome.reason } : { state };
      this.finish(id, current, terminal, now);
      return terminal;
    });
  }

  private finish(id: string, delivery: PendingDelivery, outcome: TerminalOutcome, now: number) {
    this.db.query("INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)")
      .run(done + id, JSON.stringify({ channel: delivery.channel, attempts: delivery.attempts, ...outcome }), iso(now));
    this.db.query("DELETE FROM app_state WHERE key=?").run(pending + id);
  }
}
