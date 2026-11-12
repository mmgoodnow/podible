import { Database } from "bun:sqlite";
import { expect, mock, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { deliverNextNotification } from "../../src/notifications/delivery";
import { smtpTransport, type SendSmtp } from "../../src/notifications/smtp";
import { BooksRepo } from "../../src/repo";
import { defaultSettings, parseSettings } from "../../src/settings";

function fixture() {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const user = repo.upsertUser({ provider: "local", providerUserId: "reader", username: "reader" });
  const outbox = repo.notifications.delivery;
  const live = defaultSettings({ notifications: {
    delivery: "live",
    email: { host: "smtp.example.test", from: "podible@example.test", recipients: `${user.id}=reader@example.test` },
    pushover: { enabled: true, lifecycleEnabled: true, apiToken: "fake", userKey: "owner" },
  } });
  let time = Date.parse("2026-01-01T00:00:00Z");
  const enqueue = (id: number, email = true) => repo.transaction(() => {
    outbox.enqueue(`movie:1:${id}`, { title: "Owner", message: "Ready" }, email ? [
      { id: `notice-${id}`, userId: user.id, title: "Movie", episode: null },
    ] : [], new Date(time).toISOString());
  });
  const states = (kind: "event" | "pending" | "done") => db.query<{ value_json: string }, [string]>(
    "SELECT value_json FROM app_state WHERE key GLOB ? ORDER BY key",
  ).all(`notifications:delivery:v1:${kind}:*`).map(row => JSON.parse(row.value_json));
  return { db, repo, user, outbox, live, enqueue, states, now: () => time, advance: (ms: number) => { time += ms; } };
}

test("SMTP requires encryption and legacy settings preserve addresses without a sender connection", () => {
  const settings = parseSettings(JSON.stringify({ notifications: {
    email: { apiKey: "obsolete", from: "podible@example.test", recipients: "1=reader@example.test" },
  } })).notifications;
  expect(settings.delivery).toBe("dry-run");
  expect(settings.email).toMatchObject({ host: "", port: 587, security: "starttls", recipients: "1=reader@example.test" });
  expect(settings.email).not.toHaveProperty("apiKey");
  const connection = { host: "smtp.example.test", port: 587, username: "sender", password: "fixture" };
  expect(smtpTransport({ ...connection, security: "starttls" }).options).toMatchObject({
    host: connection.host, port: 587, secure: false, requireTLS: true, auth: { user: "sender", pass: "fixture" },
  });
  expect(smtpTransport({ ...connection, port: 465, security: "tls" }).options).toMatchObject({
    port: 465, secure: true,
  });
});

test("dry-run events and legacy recovery push settings never dispatch lifecycle messages", async () => {
  const f = fixture();
  try {
    const fetcher = mock(async () => { throw new Error("unexpected provider call"); });
    f.repo.updateSettings({ ...f.live, notifications: { ...f.live.notifications, delivery: "dry-run" } });
    f.enqueue(1);
    expect(f.states("event")).toEqual([{ mode: "dry-run" }]);
    expect(f.states("pending")).toEqual([]);
    f.repo.updateSettings(f.live);
    f.enqueue(1); // Replaying the dry-run event after enabling live must not send it.
    expect(await deliverNextNotification(f.outbox, { fetcher, now: f.now })).toBeNull();

    const legacy = { notifications: { delivery: "live", pushover: { enabled: true, apiToken: "fake", userKey: "owner" } } };
    f.repo.updateSettings(parseSettings(JSON.stringify(legacy)));
    expect(f.repo.getSettings().notifications.pushover.lifecycleEnabled).toBe(false);
    f.enqueue(2);
    expect(await deliverNextNotification(f.outbox, { fetcher, now: f.now })).toBeNull();
    expect(fetcher).not.toHaveBeenCalled();
  } finally { f.db.close(); }
});

test("email retries explicit temporary SMTP rejection with identical content, then stays terminal", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const requests: Parameters<SendSmtp>[1][] = [];
    const smtp: SendSmtp = async (target, message) => {
      expect(target.host).toBe("smtp.example.test");
      expect(target.to).toBe("reader@example.test");
      requests.push(message);
      if (requests.length === 1) throw Object.assign(new Error("Temporary rejection"), { responseCode: 451, command: "DATA" });
      return true;
    };
    const tick = () => deliverNextNotification(f.outbox, { smtp, now: f.now });
    expect(await tick()).toMatchObject({ state: "retry", at: f.now() + 30_000 });
    f.advance(29_999);
    expect(await tick()).toBeNull();
    f.advance(1);
    expect(await tick()).toEqual({ state: "accepted" });
    f.enqueue(1);
    expect(await tick()).toBeNull();
    expect(f.states("done")).toEqual([{ channel: "email", state: "accepted", attempts: 2 }]);
    expect(requests).toHaveLength(2);
    expect(requests[1]).toEqual(requests[0]);
  } finally { f.db.close(); }
});

test("neither email nor push resends after an interrupted delivery", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const first = f.outbox.claim(f.now())!;
    expect(first.delivery.channel).toBe("email");
    expect(f.outbox.claim(f.now())).toBeNull();
    f.advance(30_000);
    expect(f.outbox.claim(f.now())).toBeNull();
    expect(f.outbox.settle(first, { state: "accepted" }, f.now())).toBeNull();
    expect(f.states("done")).toContainEqual({ channel: "email", state: "unknown", attempts: 1, reason: "retry_window_closed" });

    f.live.notifications.pushover.lifecycleEnabled = true;
    f.repo.updateSettings(f.live);
    f.enqueue(2, false);
    const push = f.outbox.claim(f.now())!;
    expect(push.delivery.channel).toBe("pushover");
    f.advance(30_000);
    expect(f.outbox.claim(f.now())).toBeNull();
    expect(f.states("done")).toContainEqual({ channel: "pushover", state: "unknown", attempts: 1, reason: "retry_window_closed" });
  } finally { f.db.close(); }
});

test("email never retries a lost DATA reply", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const smtp = mock(async () => { throw Object.assign(new Error("SMTP connection closed"), { code: "ECONNECTION", command: "DATA" }); });
    const tick = () => deliverNextNotification(f.outbox, { smtp, now: f.now });
    expect(await tick()).toEqual({ state: "unknown", reason: "unconfirmed_email" });
    f.advance(60 * 60_000);
    expect(await tick()).toBeNull();
    expect(smtp).toHaveBeenCalledTimes(1);
    expect(f.states("done")).toEqual([{ channel: "email", state: "unknown", reason: "unconfirmed_email", attempts: 1 }]);
  } finally { f.db.close(); }
});

test("Pushover retries explicit rate limits but not an unconfirmed send", async () => {
  const f = fixture();
  try {
    f.repo.updateSettings(f.live);
    f.enqueue(1, false);
    let calls = 0;
    const fetcher = async () => ++calls === 1
      ? new Response(null, { status: 429, headers: { "Retry-After": "60" } })
      : new Response(null, { status: 503 });
    const tick = () => deliverNextNotification(f.outbox, { fetcher, now: f.now });
    expect(await tick()).toMatchObject({ state: "retry", at: f.now() + 60_000 });
    f.advance(30_000);
    expect(await tick()).toBeNull();
    f.advance(30_000);
    expect(await tick()).toEqual({ state: "unknown", reason: "unconfirmed_push" });
    f.advance(60 * 60_000);
    expect(await tick()).toBeNull();
    expect(calls).toBe(2);
  } finally { f.db.close(); }
});

test("delivery checks current configuration, never retargets queued notices, and does not send expired email", async () => {
  const f = fixture();
  try {
    f.live.notifications.pushover.lifecycleEnabled = false;
    f.repo.updateSettings(f.live);
    f.enqueue(1);
    const smtp = mock(async () => { throw new Error("unexpected provider call"); });
    const tick = () => deliverNextNotification(f.outbox, { smtp, now: f.now });
    f.live.notifications.email.recipients = `${f.user.id}=new@example.test`;
    f.repo.updateSettings(f.live);
    expect(await tick()).toEqual({ state: "skipped", reason: "destination_changed" });
    f.enqueue(2);
    f.repo.updateSettings({ ...f.live, notifications: { ...f.live.notifications, delivery: "dry-run" } });
    expect(await tick()).toEqual({ state: "skipped", reason: "dry_run" });
    f.repo.updateSettings(f.live);
    f.enqueue(3);
    f.advance(12 * 60 * 60_000);
    expect(await tick()).toBeNull();
    expect(f.states("pending")).toEqual([]);
    expect(smtp).not.toHaveBeenCalled();
  } finally { f.db.close(); }
});
